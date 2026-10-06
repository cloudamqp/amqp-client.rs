//! A TCP proxy in front of the broker that tests can cut, to simulate losing
//! the connection.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const BROKER: &str = "127.0.0.1:5672";

pub struct Proxy {
    port: u16,
    state: Arc<State>,
    accept_task: JoinHandle<()>,
}

#[derive(Default)]
struct State {
    down: AtomicBool,
    accepted: AtomicUsize,
    connections: Mutex<Vec<JoinHandle<()>>>,
}

impl Proxy {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(State::default());
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
                let conn = tokio::spawn(forward(client));
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

async fn forward(mut client: TcpStream) {
    let Ok(mut broker) = TcpStream::connect(BROKER).await else {
        return;
    };
    let _ = tokio::io::copy_bidirectional(&mut client, &mut broker).await;
}
