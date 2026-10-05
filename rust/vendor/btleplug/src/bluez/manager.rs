use super::adapter::Adapter;
use crate::session_task::SessionTask;
use crate::{Result, api};
use async_trait::async_trait;
use bluez_async::BluetoothSession;
use std::{ops::Deref, sync::Arc};

#[derive(Clone, Debug)]
pub(crate) struct Session {
    transport: BluetoothSession,
    task: Arc<SessionTask>,
}
impl Session {
    pub(crate) async fn operation<T>(
        &self,
        operation: impl std::future::Future<Output = Result<T>> + Send,
    ) -> Result<T>
    where
        T: Send,
    {
        self.task.operation(operation).await
    }

    pub(crate) fn notification_results<S>(
        &self,
        values: S,
    ) -> impl futures::Stream<Item = Result<S::Item>> + Send + use<S>
    where
        S: futures::Stream + Send + 'static,
        S::Item: Send,
    {
        self.task.notification_results(values)
    }

    pub(crate) fn monitor<S>(
        &self,
        events: S,
    ) -> impl futures::Stream<Item = std::result::Result<S::Item, String>> + Send + use<S>
    where
        S: futures::Stream + Send + 'static,
        S::Item: Send,
    {
        self.task.monitor(events)
    }
}

impl Deref for Session {
    type Target = BluetoothSession;
    fn deref(&self) -> &Self::Target {
        &self.transport
    }
}

/// Implementation of [api::Manager](crate::api::Manager).
#[derive(Clone, Debug)]
pub struct Manager {
    session: Session,
}

impl Manager {
    pub async fn new() -> Result<Self> {
        let (resource, transport) = BluetoothSession::new_resource_async().await?;
        let task = Arc::new(SessionTask::spawn(async move {
            resource.await.map_err(|error| error.to_string())
        }));
        Ok(Self {
            session: Session { transport, task },
        })
    }

    /// Stop and join this manager's shared D-Bus transport. All adapters and
    /// peripherals from this manager become unusable after shutdown.
    pub async fn shutdown(&self) -> Result<()> {
        let joined = self.session.task.close().await;
        match self.session.transport.cleanup_failure() {
            Some(message) => Err(crate::Error::RuntimeError(message)),
            None => joined,
        }
    }
}

#[async_trait]
impl api::Manager for Manager {
    type Adapter = Adapter;

    async fn adapters(&self) -> Result<Vec<Adapter>> {
        let adapters = self
            .session
            .operation(async { Ok(self.session.get_adapters().await?) })
            .await?;
        Ok(adapters
            .into_iter()
            .map(|adapter| Adapter::new(self.session.clone(), adapter.id))
            .collect())
    }
}
