use crate::cleanup::CleanupSender;
use dbus::Message;
use dbus::channel::{MatchingReceiver, Token};
use dbus::message::MatchRule;
use dbus::nonblock::SyncConnection;
use futures::Stream;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;

type Setup = Pin<Box<dyn Future<Output = Result<(), dbus::Error>> + Send>>;

/// Owns local publication before AddMatch starts, including cancellation while
/// its acknowledgement is pending. Cleanup runs in the owned transport resource.
pub struct MessageStream {
    token: Option<Token>,
    pending: Option<Setup>,
    confirmed: bool,
    rule: String,
    retired: Arc<AtomicBool>,
    events: Receiver<Message>,
    connection: Arc<SyncConnection>,
    cleanup: CleanupSender,
}

impl MessageStream {
    pub(crate) fn new(
        rule: MatchRule<'static>,
        connection: Arc<SyncConnection>,
        cleanup: CleanupSender,
    ) -> Self {
        let failure = cleanup.clone();
        let remote_rule = rule.match_str();
        let retired = Arc::new(AtomicBool::new(false));
        let callback_retired = retired.clone();
        let (mut sender, events) = crate::signal_queue::queue(move || {
            failure.record_failure("D-Bus signal event queue overflow");
        });
        let token = connection.start_receive(
            rule,
            Box::new(move |message, _| {
                if callback_retired.load(Ordering::SeqCst) {
                    return false;
                }
                sender.push(message);
                // Keep overflow cleanup under the owner, but retire a callback
                // already in dispatch when its stream was dropped.
                !callback_retired.load(Ordering::SeqCst)
            }),
        );
        Self {
            token: Some(token),
            pending: None,
            confirmed: false,
            rule: remote_rule,
            retired,
            events,
            connection,
            cleanup,
        }
    }

    pub(crate) async fn enable_remote(mut self) -> Result<Self, crate::BluetoothError> {
        let connection = self.connection.clone();
        let rule = self.rule.clone();
        // This future is created only when enable_remote is first polled. Its
        // first poll sends AddMatch before returning Pending; unpolled setup
        // cannot start registration later through Drop cleanup.
        self.pending = Some(Box::pin(
            async move { connection.add_match_no_cb(&rule).await },
        ));
        let result = match self.pending.as_mut() {
            Some(pending) => pending.await,
            None => return Err(dbus::Error::new_failed("Missing owned AddMatch setup").into()),
        };
        self.pending = None;
        match result {
            Ok(()) => {
                self.confirmed = true;
                Ok(self)
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl Stream for MessageStream {
    type Item = Message;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.events.poll_recv(cx)
    }
}

impl Drop for MessageStream {
    fn drop(&mut self) {
        self.retired.store(true, Ordering::SeqCst);
        self.events.close();
        let connection = self.connection.clone();
        let Some(token) = self.token.take() else {
            return;
        };
        // Close publication even when the callback is temporarily in dispatch.
        // Remote removal is driven by
        // the same owned future as D-Bus I/O, never an unowned Drop task.
        if let Some((_, callback)) = connection.stop_receive(token) {
            drop(callback);
        }
        // The callback can be absent while native dispatch holds it; remote
        // cleanup must not depend on finding it in the local registry.
        let rule = self.rule.clone();
        let pending = self.pending.take();
        if pending.is_none() && !self.confirmed {
            return;
        }
        self.cleanup.push(async move {
            crate::cleanup::remove_after_setup(
                async move {
                    match pending {
                        Some(pending) => pending
                            .await
                            .map_err(|error| format!("D-Bus canceled AddMatch failed: {error}")),
                        None => Ok(()),
                    }
                },
                async move {
                    connection
                        .remove_match_no_cb(&rule)
                        .await
                        .map_err(|error| format!("D-Bus RemoveMatch failed: {error}"))
                },
            )
            .await
        });
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use dbus::channel::Channel;
    use std::os::unix::net::UnixListener;
    use std::sync::atomic::AtomicUsize;

    struct SocketPath(std::path::PathBuf);
    impl Drop for SocketPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn connection() -> (
        Arc<SyncConnection>,
        SocketPath,
        UnixListener,
        std::os::unix::net::UnixStream,
    ) {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = SocketPath(std::path::PathBuf::from(format!(
            "/tmp/openble-match-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        )));
        let listener = UnixListener::bind(&path.0).unwrap();
        // open_private opens transport without bus registration. No daemon,
        // authentication reply, system-bus or Bluetooth service is involved.
        let channel = Channel::open_private(&format!("unix:path={}", path.0.display())).unwrap();
        let (peer, _) = listener.accept().unwrap();
        (
            Arc::new(SyncConnection::from(channel)),
            path,
            listener,
            peer,
        )
    }

    #[tokio::test]
    async fn unpolled_setup_retires_local_callback_without_starting_remote_work() {
        let (connection, _path, _listener, _peer) = connection();
        let (sender, cleanup) = crate::cleanup::queue();
        let stream = MessageStream::new(
            MatchRule::new_signal("org.example.Test", "Changed"),
            connection.clone(),
            sender,
        );
        let token = stream.token.unwrap();
        drop(stream.enable_remote());
        assert!(connection.stop_receive(token).is_none());
        assert_eq!(
            Arc::strong_count(&connection),
            1,
            "unpolled setup queued remote work"
        );
        drop(cleanup);
    }

    #[tokio::test]
    async fn cancellation_owns_pending_add_even_when_local_callback_is_in_dispatch() {
        let (connection, _path, _listener, _peer) = connection();
        let (sender, cleanup) = crate::cleanup::queue();
        let stream = MessageStream::new(
            MatchRule::new_signal("org.example.Test", "Changed"),
            connection.clone(),
            sender,
        );
        let token = stream.token.unwrap();
        let mut setup = Box::pin(stream.enable_remote());
        assert!(futures::poll!(setup.as_mut()).is_pending());
        // Native dispatch removes the callback from the registry while calling
        // it. Reproduce that ownership state before the setup future is dropped.
        let (_, mut callback) = connection.stop_receive(token).unwrap();
        drop(setup);
        let message = Message::new_signal("/test", "org.example.Test", "Changed").unwrap();
        assert!(
            !callback(message, &connection),
            "retired callback republished a value"
        );
        drop(callback);
        assert!(
            Arc::strong_count(&connection) > 1,
            "pending AddMatch was discarded"
        );
        drop(cleanup);
        assert_eq!(
            Arc::strong_count(&connection),
            1,
            "canceled cleanup retained its connection"
        );
    }
}
