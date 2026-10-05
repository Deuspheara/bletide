//! Qualify the pinned libdbus/Tokio initialization boundary with an isolated peer.
//! No real bus, BlueZ daemon, central manager or BLE device is used.
//! Tests both the client boundary and Bletide's actual inline bootstrap helper.
#[path = "async_setup.rs"]
mod production_setup;
#[path = "dbus_wake.rs"]
mod production_wake;
#[cfg(test)]
mod tests {
    use dbus::{
        Message,
        channel::Channel,
        nonblock::{Proxy, SyncConnection},
    };
    use std::time::Duration;
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{UnixListener, UnixStream},
    };
    struct OwnedIo<T>(Option<tokio::task::JoinHandle<T>>);
    impl<T> Drop for OwnedIo<T> {
        fn drop(&mut self) {
            if let Some(task) = &self.0 {
                task.abort();
            }
        }
    }
    impl<T: std::fmt::Debug> OwnedIo<T> {
        async fn close(mut self) {
            let task = self.0.take().unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
    }
    static NEXT_DIRECTORY: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let id = NEXT_DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Keep below sockaddr_un's path limit even after 100 cycles.
            let path = std::env::temp_dir().join(format!("d-{}-{id}", std::process::id()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    const DEADLINE: Duration = Duration::from_secs(5);

    async fn line(peer: &mut BufReader<UnixStream>) -> Vec<u8> {
        let mut bytes = Vec::new();
        let count = peer.read_until(b'\n', &mut bytes).await.unwrap();
        assert!(
            count > 0 && bytes.len() <= 1024,
            "invalid controlled authentication frame"
        );
        bytes
    }
    async fn hello(peer: &mut BufReader<UnixStream>) -> Message {
        let auth = line(peer).await;
        assert!(
            auth.starts_with(b"\0AUTH EXTERNAL "),
            "unexpected auth: {auth:?}"
        );
        peer.get_mut()
            .write_all(b"OK 12345678901234567890123456789012\r\n")
            .await
            .unwrap();
        let next = line(peer).await;
        if next.starts_with(b"NEGOTIATE_UNIX_FD") {
            peer.get_mut()
                .write_all(b"ERROR unix fd unavailable in controlled peer\r\n")
                .await
                .unwrap();
            assert_eq!(line(peer).await, b"BEGIN\r\n");
        } else {
            assert_eq!(next, b"BEGIN\r\n");
        }
        let mut bytes = vec![0; 16];
        peer.read_exact(&mut bytes).await.unwrap();
        let needed = Message::demarshal_bytes_needed(&bytes).unwrap();
        assert!((16..=8192).contains(&needed));
        bytes.resize(needed, 0);
        peer.read_exact(&mut bytes[16..]).await.unwrap();
        let message = Message::demarshal(&bytes).unwrap();
        assert_eq!(message.member().unwrap().to_string(), "Hello");
        message
    }

    async fn stage(stage: u8) {
        let directory = Directory::new();
        let endpoint = directory.path().join("bus");
        let listener = UnixListener::bind(&endpoint).unwrap();
        let channel = Channel::open_private(&format!("unix:path={}", endpoint.display())).unwrap();
        assert_eq!(channel.unique_name(), None);
        let (resource, connection) =
            dbus_tokio::connection::from_channel::<SyncConnection>(channel).unwrap();
        let mut task = OwnedIo(Some(tokio::spawn(resource)));
        let (peer, _) = listener.accept().await.unwrap();
        let mut peer = BufReader::new(peer);
        let owned_connection = connection.clone();
        let mut registration = Box::pin(async move {
            let proxy = Proxy::new(
                "org.freedesktop.DBus",
                "/org/freedesktop/DBus",
                Duration::from_secs(30),
                owned_connection,
            );
            let result: Result<(String,), dbus::Error> =
                proxy.method_call("org.freedesktop.DBus", "Hello", ()).await;
            result
        });
        let mut registered_name = None;
        if stage == 0 {
            tokio::select! {
                result = &mut registration => panic!("Hello completed before authentication: {result:?}"),
                auth = line(&mut peer) => assert!(auth.starts_with(b"\0AUTH EXTERNAL ")),
            }
        } else {
            let request = tokio::select! {
                result = &mut registration => panic!("Hello completed before peer reply: {result:?}"),
                request = hello(&mut peer) => request,
                result = task.0.as_mut().unwrap() => panic!("native I/O resource ended during authentication: {result:?}"),
            };
            if stage == 2 {
                let mut reply = request.return_with_args((":1.77",));
                // A manually serialized peer reply needs its own nonzero serial.
                reply.set_serial(1);
                let mut wire = Vec::new();
                reply
                    .marshal(|bytes| {
                        wire.extend_from_slice(bytes);
                        Ok::<(), ()>(())
                    })
                    .unwrap();
                peer.get_mut().write_all(&wire).await.unwrap();
                let name = registration.as_mut().await.unwrap().0;
                assert_eq!(name, ":1.77");
                registered_name = Some(name);
                // Async Hello does not install libdbus's borrowed unique name.
                assert_eq!(connection.as_ref().as_ref().unique_name(), None);
            }
        }
        drop(registration);
        let connection = if let Some(name) = registered_name {
            // Retire every bootstrap resource owner before exclusive mutation.
            task.close().await;
            let mut channel = std::sync::Arc::try_unwrap(connection)
                .unwrap_or_else(|_| panic!("bootstrap retained a connection owner"))
                .into_channel();
            for invalid in [
                "",
                "org.example.Bus",
                ":",
                ":1",
                ":.1",
                ":1..2",
                ":bad name",
                ":1.\0bad",
            ] {
                assert!(
                    channel.set_unique_name(invalid).is_err(),
                    "accepted {invalid:?}"
                );
                assert_eq!(channel.unique_name(), None);
            }
            channel.set_unique_name(&name).unwrap();
            drop(name);
            assert_eq!(channel.unique_name(), Some(":1.77"));
            assert!(channel.set_unique_name(":1.88").is_err());
            assert_eq!(channel.unique_name(), Some(":1.77"));
            let (resource, connection) =
                dbus_tokio::connection::from_channel::<SyncConnection>(channel).unwrap();
            task = OwnedIo(Some(tokio::spawn(resource)));
            assert_eq!(connection.as_ref().as_ref().unique_name(), Some(":1.77"));
            connection
        } else {
            connection
        };
        drop(connection);
        task.close().await;
        let mut remaining = Vec::new();
        peer.read_to_end(&mut remaining).await.unwrap();
        assert!(remaining.len() < 8192, "unbounded data after cancellation");
        drop(peer);
        drop(listener);
        drop(directory);
        assert!(!endpoint.exists());
    }

    #[tokio::test]
    async fn cancellation_during_authentication_closes_owned_transport() {
        tokio::time::timeout(DEADLINE, stage(0)).await.unwrap();
    }
    #[tokio::test]
    async fn cancellation_during_hello_closes_owned_transport() {
        tokio::time::timeout(DEADLINE, stage(1)).await.unwrap();
    }
    #[tokio::test]
    async fn completed_hello_installs_owned_unique_name_and_restarts_io() {
        for _ in 0..100 {
            tokio::time::timeout(DEADLINE, stage(2)).await.unwrap();
        }
    }

    async fn inline_stage(stage: u8) {
        let deadline = stage >= 10;
        let stage = stage % 10;
        let directory = Directory::new();
        let endpoint = directory.path().join("bus");
        let listener = UnixListener::bind(&endpoint).unwrap();
        let channel = Channel::open_private(&format!("unix:path={}", endpoint.display())).unwrap();
        let mut setup = Box::pin(super::production_setup::register_channel(channel));
        let (peer, _) = listener.accept().await.unwrap();
        let mut peer = BufReader::new(peer);
        if stage == 5 {
            // Drop before the first poll: channel ownership is already captured.
        } else if stage == 0 {
            tokio::select! {
                result = &mut setup => panic!("setup completed before authentication: {}", result.is_ok()),
                auth = line(&mut peer) => assert!(auth.starts_with(b"\0AUTH EXTERNAL ")),
            }
        } else {
            let request = tokio::select! {
                result = &mut setup => panic!("setup completed before Hello reply: {}", result.is_ok()),
                request = hello(&mut peer) => request,
            };
            if stage >= 2 {
                let mut reply = if stage == 4 {
                    request.error(
                        &dbus::strings::ErrorName::new("org.freedesktop.DBus.Error.AccessDenied")
                            .unwrap(),
                        &std::ffi::CString::new("original registration permission cause").unwrap(),
                    )
                } else {
                    request.return_with_args((if stage == 3 { ":" } else { ":1.77" },))
                };
                reply.set_serial(1);
                let mut wire = Vec::new();
                reply
                    .marshal(|bytes| {
                        wire.extend_from_slice(bytes);
                        Ok::<(), ()>(())
                    })
                    .unwrap();
                peer.get_mut().write_all(&wire).await.unwrap();
                let result = setup.as_mut().await;
                if stage == 4 {
                    let error = match result {
                        Err(dbus_tokio::connection::IOResourceError::Dbus(error)) => error,
                        _ => panic!("registration did not preserve the native D-Bus error"),
                    };
                    assert_eq!(
                        error.name(),
                        Some("org.freedesktop.DBus.Error.AccessDenied")
                    );
                    assert_eq!(
                        error.message(),
                        Some("original registration permission cause")
                    );
                } else if stage == 3 {
                    assert!(result.is_err(), "invalid Hello name accepted");
                } else {
                    let (resource, connection) = result.unwrap();
                    assert_eq!(connection.as_ref().as_ref().unique_name(), Some(":1.77"));
                    drop(resource);
                    assert_eq!(std::sync::Arc::strong_count(&connection), 1);
                    drop(connection);
                }
            }
        }
        // Cancellation tears down the actual inline resource without a task to join.
        if deadline {
            assert!(tokio::time::timeout(Duration::ZERO, setup).await.is_err());
        } else {
            drop(setup);
        }
        let mut remaining = Vec::new();
        peer.read_to_end(&mut remaining).await.unwrap();
        assert!(remaining.len() < 8192);
        drop(peer);
        drop(listener);
        drop(directory);
        assert!(!endpoint.exists());
    }

    #[tokio::test]
    async fn production_inline_setup_cancellation_releases_auth_and_hello() {
        for _ in 0..100 {
            for stage in [5, 0, 1, 10, 11] {
                tokio::time::timeout(DEADLINE, inline_stage(stage))
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn production_inline_setup_owns_name_and_rejects_invalid_reply() {
        for _ in 0..100 {
            for stage in [2, 3] {
                tokio::time::timeout(DEADLINE, inline_stage(stage))
                    .await
                    .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn production_inline_setup_preserves_remote_failure_and_retries() {
        for _ in 0..100 {
            for stage in [4, 2] {
                tokio::time::timeout(DEADLINE, inline_stage(stage))
                    .await
                    .unwrap();
            }
        }
    }

    struct Endpoint(std::path::PathBuf);
    impl Drop for Endpoint {
        fn drop(&mut self) {
            match std::fs::remove_file(&self.0) {
                Ok(()) => (),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => panic!("owned socket endpoint cleanup failed: {error}"),
            }
        }
    }

    async fn session_stage(path: &std::path::Path, stage: u8) {
        let deadline = stage >= 10;
        let stage = stage % 10;
        let endpoint = Endpoint(path.to_owned());
        let listener = UnixListener::bind(path).unwrap();
        let mut setup = Box::pin(bluez_async::BluetoothSession::new_resource_async());
        if stage == 5 {
            drop(setup);
            assert!(
                tokio::time::timeout(Duration::ZERO, listener.accept())
                    .await
                    .is_err()
            );
        } else {
            let (peer, _) = tokio::select! {
                result = &mut setup => panic!("constructor completed before authentication: {}", result.is_ok()),
                accepted = listener.accept() => accepted.unwrap(),
            };
            let mut peer = BufReader::new(peer);
            if stage == 0 {
                tokio::select! {
                    result = &mut setup => panic!("constructor completed before authentication reply: {}", result.is_ok()),
                    auth = line(&mut peer) => assert!(auth.starts_with(b"\0AUTH EXTERNAL ")),
                }
            } else {
                let request = tokio::select! {
                    result = &mut setup => panic!("constructor completed before Hello reply: {}", result.is_ok()),
                    request = hello(&mut peer) => request,
                };
                if stage >= 2 {
                    let mut reply = if stage == 4 {
                        request.error(
                            &dbus::strings::ErrorName::new(
                                "org.freedesktop.DBus.Error.AccessDenied",
                            )
                            .unwrap(),
                            &std::ffi::CString::new("constructor registration permission cause")
                                .unwrap(),
                        )
                    } else {
                        request.return_with_args((if stage == 3 { ":" } else { ":1.77" },))
                    };
                    reply.set_serial(1);
                    let mut wire = Vec::new();
                    reply
                        .marshal(|bytes| {
                            wire.extend_from_slice(bytes);
                            Ok::<(), ()>(())
                        })
                        .unwrap();
                    peer.get_mut().write_all(&wire).await.unwrap();
                    let result = setup.as_mut().await;
                    if stage == 4 {
                        match result {
                            Err(bluez_async::BluetoothError::DbusSetupError(
                                dbus_tokio::connection::IOResourceError::Dbus(error),
                            )) => {
                                assert_eq!(
                                    error.name(),
                                    Some("org.freedesktop.DBus.Error.AccessDenied")
                                );
                                assert_eq!(
                                    error.message(),
                                    Some("constructor registration permission cause")
                                );
                            }
                            _ => panic!("constructor lost original D-Bus error"),
                        }
                    } else if stage == 3 {
                        assert!(result.is_err(), "constructor accepted invalid unique name");
                    } else {
                        let (resource, session) = result.unwrap();
                        assert!(session.cleanup_failure().is_none());
                        if stage == 7 {
                            // The returned resource must own cleanup even before its first poll.
                            drop(resource);
                            drop(session);
                        } else {
                            let retained = session.clone();
                            let mut owner = OwnedIo(Some(tokio::spawn(resource)));
                            drop(session);
                            assert!(
                                tokio::time::timeout(Duration::ZERO, owner.0.as_mut().unwrap())
                                    .await
                                    .is_err(),
                                "resource stopped with a live session owner"
                            );
                            if stage == 6 {
                                owner.close().await;
                                assert!(retained.cleanup_failure().is_none());
                                drop(retained);
                            } else {
                                drop(retained);
                                owner.0.as_mut().unwrap().await.unwrap().unwrap();
                                owner.0.take();
                            }
                        }
                    }
                }
            }
            if deadline {
                assert!(tokio::time::timeout(Duration::ZERO, setup).await.is_err());
            } else {
                drop(setup);
            }
            let mut remaining = Vec::new();
            peer.read_to_end(&mut remaining).await.unwrap();
            assert!(remaining.len() < 8192);
        }
        drop(listener);
        drop(endpoint);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn production_session_constructor_cancels_retries_and_joins_cleanup() {
        let path =
            std::path::PathBuf::from(std::env::var_os("OPENBLE_DBUS_TEST_ENDPOINT").unwrap());
        assert_eq!(
            std::env::var("DBUS_SYSTEM_BUS_ADDRESS").unwrap(),
            format!("unix:path={}", path.display())
        );
        let fd_directory = if cfg!(target_os = "linux") {
            "/proc/self/fd"
        } else {
            "/dev/fd"
        };
        let descriptor_count = || std::fs::read_dir(fd_directory).unwrap().count();
        let baseline = descriptor_count();
        for cycle in 0..100 {
            for stage in [5, 0, 10, 1, 11, 3, 4, 2, 6, 7] {
                tokio::time::timeout(DEADLINE, session_stage(&path, stage))
                    .await
                    .unwrap();
                assert_eq!(
                    descriptor_count(),
                    baseline,
                    "descriptor retained after cycle {cycle}, stage {stage}"
                );
            }
        }
        eprintln!("session constructor: {baseline} descriptors before and after 1000 cycles");
    }
}
