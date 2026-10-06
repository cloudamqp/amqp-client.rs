//! The `Client`'s current AMQP session and the state shared with its handles.

use std::future::Future;
use std::sync::{Arc, Mutex};

use super::recovery::RecoveryConfig;
use super::topology::Topology;
use crate::client::channel::Channel;
use crate::client::config::Config;
use crate::client::connection::Connection;
use crate::error::Error;

/// One AMQP session: a connection and the channel the client declares and
/// publishes on. Its generation identifies it across recoveries.
pub(crate) struct Session {
    pub(crate) generation: u64,
    pub(crate) connection: Connection,
    pub(crate) channel: Arc<Channel>,
}

/// State owned by the `Client`; handles and consumers hold it weakly, so
/// dropping the `Client` ends them.
pub(crate) struct ClientInner {
    #[allow(dead_code, reason = "used to reconnect once session recovery lands")]
    config: Config,
    recovery: Option<RecoveryConfig>,
    pub(crate) topology: Arc<Mutex<Topology>>,
    session: Mutex<Arc<Session>>,
}

impl ClientInner {
    pub(crate) async fn connect(
        config: Config,
        recovery: Option<RecoveryConfig>,
    ) -> Result<Arc<Self>, Error> {
        let session = open_session(&config, 0).await?;
        Ok(Arc::new(Self {
            config,
            recovery,
            topology: Arc::default(),
            session: Mutex::new(Arc::new(session)),
        }))
    }

    pub(crate) fn recovery(&self) -> Option<&RecoveryConfig> {
        self.recovery.as_ref()
    }

    pub(crate) fn current(&self) -> Arc<Session> {
        self.session.lock().unwrap().clone()
    }

    /// Runs `op` on the current session.
    pub(crate) async fn with_session<R, F, Fut>(&self, op: F) -> Result<R, Error>
    where
        F: Fn(Arc<Session>) -> Fut,
        Fut: Future<Output = Result<R, Error>>,
    {
        op(self.current()).await
    }
}

async fn open_session(config: &Config, generation: u64) -> Result<Session, Error> {
    let connection = Connection::connect_with_config(config.clone()).await?;
    let channel = Arc::new(connection.open_channel().await?);
    Ok(Session {
        generation,
        connection,
        channel,
    })
}
