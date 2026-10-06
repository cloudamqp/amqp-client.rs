//! A TCP proxy in front of the broker that tests can cut, to simulate losing
//! the connection.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

const BROKER: &str = "127.0.0.1:5672";

pub struct Proxy {
    port: u16,
    state: Arc<State>,
    accept_task: JoinHandle<()>,
}

struct State {
    down: AtomicBool,
    accepted: AtomicUsize,
    /// While true, nothing is forwarded but sockets stay open.
    frozen: watch::Sender<bool>,
    connections: Mutex<Vec<JoinHandle<()>>>,
}

impl Proxy {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(State {
            down: AtomicBool::new(false),
            accepted: AtomicUsize::new(0),
            frozen: watch::channel(false).0,
            connections: Mutex::default(),
        });
        let accept_state = state.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                accept_state.accepted.fetch_add(1, Ordering::SeqCst);
                if accept_state.down.load(Ordering::SeqCst) {
                    drop(client);
                    continue;
                }
                let conn = tokio::spawn(forward(client, accept_state.frozen.subscribe()));
                accept_state.connections.lock().unwrap().push(conn);
            }
        });
        Self {
            port,
            state,
            accept_task,
        }
    }

    pub fn uri(&self) -> String {
        format!("amqp://guest:guest@127.0.0.1:{}", self.port)
    }

    /// Drops every proxied connection; new ones are still accepted.
    pub fn cut(&self) {
        for conn in self.state.connections.lock().unwrap().drain(..) {
            conn.abort();
        }
    }

    /// While down, new connections are closed at once, as if the broker were
    /// unreachable.
    pub fn set_down(&self, down: bool) {
        self.state.down.store(down, Ordering::SeqCst);
    }

    /// Stops forwarding without closing anything, like a silent network
    /// partition: only a heartbeat timeout can tell.
    pub fn freeze(&self) {
        self.state.frozen.send_replace(true);
    }

    /// Connections accepted so far, including ones closed while down.
    pub fn accepted(&self) -> usize {
        self.state.accepted.load(Ordering::SeqCst)
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept_task.abort();
        self.cut();
    }
}

async fn forward(client: TcpStream, frozen: watch::Receiver<bool>) {
    let Ok(broker) = TcpStream::connect(BROKER).await else {
        return;
    };
    let (client_read, client_write) = client.into_split();
    let (broker_read, broker_write) = broker.into_split();
    tokio::join!(
        pipe(client_read, broker_write, frozen.clone()),
        pipe(broker_read, client_write, frozen),
    );
}

/// Copies `from` to `to` until either side closes; while frozen, holds
/// everything read.
async fn pipe(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut frozen: watch::Receiver<bool>,
) {
    let mut buf = vec![0; 64 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if *frozen.borrow() {
            // Hold the data and the sockets until the proxy is dropped.
            let _ = frozen.wait_for(|f| !f).await;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}
