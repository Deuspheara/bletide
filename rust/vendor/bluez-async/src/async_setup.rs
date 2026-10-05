//! Authentication and Hello own their reactor inline; cancellation drops both.
use dbus::{
    channel::Channel,
    nonblock::{Proxy, SyncConnection},
};
use dbus_tokio::connection::{IOResource, IOResourceError, from_channel};
use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

pub(crate) async fn register_channel(
    channel: Channel,
) -> Result<(IOResource<SyncConnection>, Arc<SyncConnection>), IOResourceError> {
    let (resource, connection) = from_channel::<SyncConnection>(channel)?;
    let mut resource = Box::pin(resource);
    // Install the reactor waker before Hello sends queued authentication data.
    poll_fn(|cx| match resource.as_mut().poll(cx) {
        Poll::Pending => Poll::Ready(Ok(())),
        Poll::Ready(error) => Poll::Ready(Err(error)),
    })
    .await?;
    let name: (String,) = {
        let proxy = Proxy::new(
            "org.freedesktop.DBus",
            "/org/freedesktop/DBus",
            Duration::from_secs(30),
            connection.clone(),
        );
        let hello = proxy.method_call("org.freedesktop.DBus", "Hello", ());
        tokio::pin!(hello);
        tokio::select! {
            result = &mut hello => result?,
            error = resource.as_mut() => return Err(error),
        }
    };
    // No task was spawned. Dropping the inline resource retires its fd watch
    // before exclusive channel mutation and preserves the connected transport.
    drop(resource);
    let connection = Arc::try_unwrap(connection)
        .map_err(|_| dbus::Error::new_failed("D-Bus bootstrap retained a connection owner"))?;
    let mut channel = connection.into_channel();
    channel.set_unique_name(&name.0)?;
    from_channel::<SyncConnection>(channel).map_err(Into::into)
}
