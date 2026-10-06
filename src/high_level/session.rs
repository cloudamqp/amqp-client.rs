//! The `Client`'s current AMQP session, and operation-driven recovery of it
//! (ADR-0002).

use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::time::Instant;

use tokio::sync::watch;
use tracing::{info, warn};

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
    /// Replaced by a new channel once the broker closes it.
    channel: Mutex<Arc<Channel>>,
    /// The channel in confirm mode for `publish_confirmed`: opened on first
    /// use, replaced once the broker closes it.
    confirm_channel: tokio::sync::Mutex<Option<Arc<Channel>>>,
}

impl Session {
    pub(crate) fn channel(&self) -> Arc<Channel> {
        self.channel.lock().unwrap().clone()
    }

    /// The confirm-mode channel, opened (or reopened after the broker closed
    /// it) on demand.
    pub(crate) async fn confirm_channel(&self) -> Result<Arc<Channel>, Error> {
        let mut slot = self.confirm_channel.lock().await;
        if let Some(channel) = slot.as_ref().filter(|c| !c.is_closed()) {
            return Ok(channel.clone());
        }
        let channel = Arc::new(self.connection.open_channel().await?);
        channel.confirm_select().await?;
        *slot = Some(channel.clone());
        Ok(channel)
    }

    /// The channel, reopened first if the broker closed it (e.g. after a
    /// publish to a missing exchange).
    async fn open_channel(&self) -> Result<Arc<Channel>, Error> {
        let channel = self.channel();
        if !channel.is_closed() {
            return Ok(channel);
        }
        let fresh = Arc::new(self.connection.open_channel().await?);
        let mut current = self.channel.lock().unwrap();
        if current.is_closed() {
            *current = fresh;
        }
        Ok(current.clone())
    }
}

/// Runs `op` on `session`, with its channel open.
async fn run_on<R, F, Fut>(session: Arc<Session>, op: &F) -> Result<R, Error>
where
    F: Fn(Arc<Session>) -> Fut,
    Fut: Future<Output = Result<R, Error>>,
{
    session.open_channel().await?;
    op(session).await
}

/// The session slot. `waiters` and `consumers` are the recovery demand.
struct Slot {
    session: Arc<Session>,
    recovering: bool,
    waiters: usize,
    consumers: usize,
}

/// State owned by the `Client`; handles, consumers and the recovery task hold
/// it weakly, so dropping the `Client` ends them.
pub(crate) struct ClientInner {
    config: Config,
    recovery: Option<RecoveryConfig>,
    pub(crate) topology: Arc<Mutex<Topology>>,
    slot: Mutex<Slot>,
    /// Announces each new session generation.
    generation_tx: watch::Sender<u64>,
}

impl ClientInner {
    pub(crate) async fn connect(
        config: Config,
        recovery: Option<RecoveryConfig>,
    ) -> Result<Arc<Self>, Error> {
        let topology = Arc::default();
        let session = open_session(&config, &topology, 0).await?;
        Ok(Arc::new(Self {
            config,
            recovery,
            topology,
            slot: Mutex::new(Slot {
                session: Arc::new(session),
                recovering: false,
                waiters: 0,
                consumers: 0,
            }),
            generation_tx: watch::channel(0).0,
        }))
    }

    pub(crate) fn recovery(&self) -> Option<&RecoveryConfig> {
        self.recovery.as_ref()
    }

    pub(crate) fn rpc_timeout(&self) -> Duration {
        self.config.rpc_timeout
    }

    pub(crate) fn current(&self) -> Arc<Session> {
        self.slot.lock().unwrap().session.clone()
    }

    /// Runs `op` on a usable session. With recovery on, it first waits for
    /// recovery in progress, and if `op` finds the session lost, triggers
    /// recovery, waits for it and runs `op` once more. All waiting together
    /// takes at most the recovery timeout (time spent in `op` not counted).
    pub(crate) async fn with_session<R, F, Fut>(self: &Arc<Self>, op: F) -> Result<R, Error>
    where
        F: Fn(Arc<Session>) -> Fut,
        Fut: Future<Output = Result<R, Error>>,
    {
        let Some(timeout) = self.recovery.as_ref().map(|r| r.timeout) else {
            return run_on(self.current(), &op).await;
        };
        // The budget covers waiting only, not the time `op` itself takes.
        let mut budget = timeout;
        let session = {
            let slot = self.slot.lock().unwrap();
            (!slot.recovering).then(|| slot.session.clone())
        };
        let session = match session {
            Some(session) => session,
            None => {
                let generation = self.current().generation;
                let started = Instant::now();
                let session = session_after(Arc::downgrade(self), generation, Some(budget)).await?;
                budget = budget.saturating_sub(started.elapsed());
                session
            }
        };
        match run_on(session.clone(), &op).await {
            Err(Error::ConnectionClosed { .. }) => {
                let next =
                    session_after(Arc::downgrade(self), session.generation, Some(budget)).await?;
                run_on(next, &op).await
            }
            result => result,
        }
    }

    /// Counts a live recoverable consumer as recovery demand until the
    /// returned guard is dropped.
    pub(crate) fn register_consumer(self: &Arc<Self>) -> ConsumerRegistration {
        self.slot.lock().unwrap().consumers += 1;
        ConsumerRegistration {
            client: Arc::downgrade(self),
        }
    }
}

/// Waits for a session newer than `generation`, triggering recovery if it is
/// not running. Gives up after `timeout` (if any) or when the client is
/// dropped. Holds the client only weakly while waiting.
pub(crate) async fn session_after(
    client: Weak<ClientInner>,
    generation: u64,
    timeout: Option<Duration>,
) -> Result<Arc<Session>, Error> {
    let mut generations = {
        let inner = client.upgrade().ok_or_else(client_dropped)?;
        inner.slot.lock().unwrap().waiters += 1;
        inner.generation_tx.subscribe()
    };
    let _waiter = Waiter(client.clone());
    let wait = async {
        loop {
            {
                let inner = client.upgrade().ok_or_else(client_dropped)?;
                let mut slot = inner.slot.lock().unwrap();
                if slot.session.generation > generation {
                    return Ok(slot.session.clone());
                }
                if !slot.recovering {
                    slot.recovering = true;
                    tokio::spawn(recover(client.clone()));
                }
            }
            if generations.changed().await.is_err() {
                return Err(client_dropped());
            }
        }
    };
    match timeout {
        Some(t) => tokio::time::timeout(t, wait)
            .await
            .unwrap_or_else(|_| Err(session_lost())),
        None => wait.await,
    }
}

/// The recovery task: connect, declare the recorded topology, install the new
/// generation; on failure wait the recovery timeout and retry. Stands down when
/// demand reaches zero or the client is dropped.
async fn recover(client: Weak<ClientInner>) {
    loop {
        // Hold the client only to read what an attempt needs: a stalled
        // connect must not keep a dropped client (and its consumers) alive.
        let (config, topology, timeout, generation) = {
            let Some(inner) = client.upgrade() else {
                return;
            };
            let mut slot = inner.slot.lock().unwrap();
            if slot.waiters == 0 && slot.consumers == 0 {
                slot.recovering = false;
                return;
            }
            let timeout = inner
                .recovery
                .as_ref()
                .map_or(Duration::ZERO, |r| r.timeout);
            (
                inner.config.clone(),
                inner.topology.clone(),
                timeout,
                slot.session.generation + 1,
            )
        };
        match open_session(&config, &topology, generation).await {
            Ok(session) => {
                // If the client was dropped meanwhile, the session is dropped
                // (and closed) here.
                let Some(inner) = client.upgrade() else {
                    return;
                };
                {
                    let mut slot = inner.slot.lock().unwrap();
                    slot.session = Arc::new(session);
                    slot.recovering = false;
                }
                inner.generation_tx.send_replace(generation);
                info!(generation, "AMQP session recovered");
                return;
            }
            Err(e) => warn!(error = %e, "session recovery failed, retrying"),
        }
        tokio::time::sleep(timeout).await;
    }
}

/// Connects and declares the recorded topology: exchanges, then queues
/// (following renames of server-named queues), then bindings.
async fn open_session(
    config: &Config,
    topology: &Mutex<Topology>,
    generation: u64,
) -> Result<Session, Error> {
    let connection = Connection::connect_with_config(config.clone()).await?;
    let channel = Arc::new(connection.open_channel().await?);
    let (exchanges, queues) = {
        let t = topology.lock().unwrap();
        (t.exchanges().to_vec(), t.queues().to_vec())
    };
    for e in exchanges {
        channel.exchange_declare(&e.name, &e.kind, e.opts).await?;
    }
    for q in queues {
        let ok = channel.queue_declare(&q.requested_name, q.opts).await?;
        topology.lock().unwrap().rename_queue(q.id, &ok.queue);
    }
    let bindings: Vec<_> = {
        let t = topology.lock().unwrap();
        t.bindings()
            .iter()
            .filter_map(|b| Some((t.queue_name(b.queue)?.to_owned(), b.clone())))
            .collect()
    };
    for (queue, b) in bindings {
        channel
            .queue_bind(&queue, &b.exchange, &b.routing_key, b.arguments)
            .await?;
    }
    Ok(Session {
        generation,
        connection,
        channel: Mutex::new(channel),
        confirm_channel: tokio::sync::Mutex::default(),
    })
}

/// Decrements the waiter count on drop, including on timeout or cancel.
struct Waiter(Weak<ClientInner>);

impl Drop for Waiter {
    fn drop(&mut self) {
        if let Some(inner) = self.0.upgrade() {
            inner.slot.lock().unwrap().waiters -= 1;
        }
    }
}

/// Keeps a recoverable consumer counted as recovery demand.
pub(crate) struct ConsumerRegistration {
    client: Weak<ClientInner>,
}

impl Drop for ConsumerRegistration {
    fn drop(&mut self) {
        if let Some(inner) = self.client.upgrade() {
            inner.slot.lock().unwrap().consumers -= 1;
        }
    }
}

fn client_dropped() -> Error {
    Error::ConnectionClosed {
        code: 0,
        reason: "client dropped".into(),
    }
}

fn session_lost() -> Error {
    Error::ConnectionClosed {
        code: 0,
        reason: "connection lost; recovery did not finish in time".into(),
    }
}
