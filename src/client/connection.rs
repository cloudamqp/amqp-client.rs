use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;
use tracing::{debug, error, trace, warn};

use super::auth;
use super::channel::Channel;
use super::config::Config;
use crate::error::Error;
use crate::protocol::frame::{self, Frame, PROTOCOL_HEADER};
use crate::protocol::method::MethodFrame;
use crate::protocol::method::channel as ch_method;
use crate::protocol::method::connection as conn_method;
use crate::protocol::table::Table;
use crate::protocol::types::{LongStr, ShortStr};

/// Outgoing frame or control message for the writer task.
pub(crate) enum OutgoingFrame {
    Frame(Frame),
    /// A frame carrying a publish-admission permit. The writer drops the
    /// permit only after `write_all` succeeds for this frame's bytes,
    /// providing genuine end-to-end backpressure: a publisher cannot enqueue
    /// more than `publish_admission_bytes` worth of content frames into the
    /// socket / kernel send queue at any time.
    FrameWithPermit(Frame, tokio::sync::OwnedSemaphorePermit),
    Shutdown,
}

/// Allocates AMQP channel IDs with a free-list of recycled IDs.
///
/// Channel IDs in AMQP 0-9-1 are u16, with 0 reserved for connection-level
/// frames. We use a `BTreeSet` for the free-list (so allocation is deterministic
/// and IDs stay dense) plus a high-water counter for fresh allocations.
pub(crate) struct ChannelIdAllocator {
    free: BTreeSet<u16>,
    /// IDs whose `Channel.Close` was sent on drop; reusable once `CloseOk`
    /// arrives.
    closing: BTreeSet<u16>,
    next: u16,
    cap: u16,
}

impl ChannelIdAllocator {
    pub(crate) fn new(cap: u16) -> Self {
        Self {
            free: BTreeSet::new(),
            closing: BTreeSet::new(),
            next: 1,
            cap,
        }
    }

    fn allocate(&mut self) -> Option<u16> {
        if let Some(id) = self.free.pop_first() {
            return Some(id);
        }
        // `next == 0` is the exhausted sentinel (valid IDs start at 1).
        if self.next == 0 || self.next > self.cap {
            return None;
        }
        let id = self.next;
        // checked_add returns None at u16::MAX; mark exhausted via 0.
        self.next = self.next.checked_add(1).unwrap_or(0);
        Some(id)
    }

    pub(crate) fn release(&mut self, id: u16) {
        if id != 0 && id <= self.cap {
            self.free.insert(id);
        }
    }

    pub(crate) fn mark_closing(&mut self, id: u16) {
        self.closing.insert(id);
    }

    pub(crate) fn is_closing(&self, id: u16) -> bool {
        self.closing.contains(&id)
    }

    /// Releases `id` if it was closing. Returns whether it was.
    pub(crate) fn finish_closing(&mut self, id: u16) -> bool {
        let was_closing = self.closing.remove(&id);
        if was_closing {
            self.release(id);
        }
        was_closing
    }
}

/// Internal shared state for the connection.
///
/// This is `Send + Sync` and shared via `Arc` between the connection,
/// its channels, and message handles.
pub(crate) struct ConnectionShared {
    /// Sender for outgoing frames (writer task consumes these).
    pub(crate) frame_tx: mpsc::UnboundedSender<OutgoingFrame>,
    /// Byte-counted semaphore controlling publish admission. Each
    /// `basic_publish` (and `basic_publish_confirmed`) call acquires
    /// `min(body.len(), capacity)` permits before sending content frames;
    /// the writer drops the permits after `write_all` succeeds. Acks,
    /// heartbeats, and RPC method frames bypass this entirely.
    pub(crate) publish_admission: Arc<tokio::sync::Semaphore>,
    /// Capacity of `publish_admission` in bytes — used to cap
    /// `acquire_many_owned` for over-large bodies.
    pub(crate) publish_admission_capacity: usize,
    /// Channel-ID allocator, shared with every channel and the reader task.
    pub(crate) id_alloc: Mutex<ChannelIdAllocator>,
    /// Notified when the peer is considered dead (heartbeat timeout); stops
    /// the reader task.
    pub(crate) dead: tokio::sync::Notify,
}

impl ConnectionShared {
    /// Sends `Channel.Close` for `channel_id` without waiting. The reader task
    /// frees the id when the broker's `CloseOk` arrives; if the connection is
    /// already gone, the id is freed now.
    pub(crate) fn close_without_waiting(&self, channel_id: u16) {
        let close = Frame::Method(
            channel_id,
            MethodFrame::ChannelClose(ch_method::Close {
                reply_code: 200,
                reply_text: ShortStr::from_static("Normal shutdown"),
                class_id: 0,
                method_id: 0,
            }),
        );
        let mut ids = self.id_alloc.lock().unwrap();
        if self.frame_tx.send(OutgoingFrame::Frame(close)).is_ok() {
            ids.mark_closing(channel_id);
        } else {
            ids.release(channel_id);
        }
    }
}

/// A stream that is either plain TCP or TLS-wrapped TCP.
#[allow(
    clippy::large_enum_variant,
    reason = "one per connection; boxing would add indirection on every read/write"
)]
enum MaybeTls {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// An AMQP connection over TCP, using tokio's async runtime.
pub struct Connection {
    pub(crate) shared: Arc<ConnectionShared>,
    /// Channel registry: channel_id -> sender for incoming frames.
    /// Shared with the reader task so it can route frames to channels.
    channel_senders: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Frame>>>>,
    /// Negotiated max frame size, used when opening channels.
    frame_max: u32,
    /// Per-RPC timeout used by Channel methods, copied from `Config` at
    /// connect time so each spawned Channel can stamp its own copy.
    rpc_timeout: Duration,
    /// Handle to the reader task.
    reader_task: JoinHandle<()>,
    /// Handle to the writer task.
    writer_task: JoinHandle<()>,
    /// Handle to the heartbeat task (if enabled).
    heartbeat_task: Option<JoinHandle<()>>,
    /// Receiver for connection-level frames (channel 0).
    conn_frames_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Frame>>,
}

impl Connection {
    /// Connect to an AMQP broker using a URI.
    ///
    /// ```ignore
    /// let conn = Connection::connect("amqp://guest:guest@localhost").await?;
    /// ```
    pub async fn connect(uri: &str) -> Result<Self, Error> {
        let config = Config::from_uri(uri)?;
        Self::connect_with_config(config).await
    }

    /// Connect using an explicit configuration.
    pub async fn connect_with_config(config: Config) -> Result<Self, Error> {
        let timeout = config.connect_timeout;
        match tokio::time::timeout(timeout, Self::do_connect(config)).await {
            Ok(result) => result,
            Err(_) => Err(Error::Timeout),
        }
    }

    async fn do_connect(config: Config) -> Result<Self, Error> {
        let addr = format!("{}:{}", config.host, config.port);
        debug!(addr = %addr, "connecting to AMQP broker");

        let tcp_stream = TcpStream::connect(&addr).await?;
        tcp_stream.set_nodelay(true)?;

        // Optionally wrap in TLS
        let mut stream = if config.tls {
            install_default_crypto_provider();
            let tls_config = rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore {
                    roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
                })
                .with_no_client_auth();
            let connector = TlsConnector::from(Arc::new(tls_config));
            let server_name = rustls::pki_types::ServerName::try_from(config.host.as_str())
                .map_err(|e| Error::InvalidUri(format!("invalid TLS server name: {e}")))?
                .to_owned();
            let tls_stream = connector.connect(server_name, tcp_stream).await?;
            MaybeTls::Tls(tls_stream)
        } else {
            MaybeTls::Plain(tcp_stream)
        };

        // --- Handshake (sequential on the full stream) ---

        // Send protocol header
        stream.write_all(PROTOCOL_HEADER).await?;

        // Read Connection.Start. Pre-tune cap at the spec-mandated minimum.
        let start_frame = read_frame(&mut stream, MIN_FRAME_MAX).await?;
        let _start = match start_frame {
            Frame::Method(0, MethodFrame::ConnectionStart(s)) => s,
            _ => {
                return Err(Error::Protocol(
                    crate::error::ProtocolError::UnexpectedMethod {
                        expected: "Connection.Start",
                        got: format!("{start_frame:?}"),
                    },
                ));
            }
        };
        debug!("received Connection.Start");

        // Send Connection.StartOk
        let mut client_properties = Table::new();
        client_properties.insert(
            ShortStr::try_from("product").map_err(Error::Protocol)?,
            crate::protocol::table::FieldValue::LongStr(LongStr::from("amqp-client.rs")),
        );
        client_properties.insert(
            ShortStr::try_from("version").map_err(Error::Protocol)?,
            crate::protocol::table::FieldValue::LongStr(LongStr::from(env!("CARGO_PKG_VERSION"))),
        );

        // Capabilities: tells RabbitMQ which extensions we understand. The key
        // one for correctness is `consumer_cancel_notify=true`: without it,
        // RabbitMQ closes the entire channel when a quorum-queue leader fails
        // over (and the consumer goes with it) instead of sending a clean
        // Basic.Cancel. With it, callers get Consumer::cancellation() ==
        // ServerCancel and can decide their own retry policy.
        let mut capabilities = Table::new();
        capabilities.insert(
            ShortStr::try_from("consumer_cancel_notify").map_err(Error::Protocol)?,
            crate::protocol::table::FieldValue::Boolean(true),
        );
        capabilities.insert(
            ShortStr::try_from("connection.blocked").map_err(Error::Protocol)?,
            crate::protocol::table::FieldValue::Boolean(true),
        );
        client_properties.insert(
            ShortStr::try_from("capabilities").map_err(Error::Protocol)?,
            crate::protocol::table::FieldValue::Table(capabilities),
        );

        let start_ok = MethodFrame::ConnectionStartOk(conn_method::StartOk {
            client_properties,
            mechanism: ShortStr::try_from("PLAIN").map_err(Error::Protocol)?,
            response: auth::plain_response(&config.username, &config.password),
            locale: ShortStr::try_from("en_US").map_err(Error::Protocol)?,
        });
        write_frame(&mut stream, &Frame::Method(0, start_ok)).await?;

        // Read Connection.Tune. Still pre-tune; the negotiated frame_max is
        // not in effect yet (this very frame carries the broker's proposal).
        let tune_frame = read_frame(&mut stream, MIN_FRAME_MAX).await?;
        let tune = match tune_frame {
            Frame::Method(0, MethodFrame::ConnectionTune(t)) => t,
            Frame::Method(0, MethodFrame::ConnectionClose(close)) => {
                return Err(Error::ConnectionClosed {
                    code: close.reply_code,
                    reason: close.reply_text.to_string(),
                });
            }
            _ => {
                return Err(Error::Protocol(
                    crate::error::ProtocolError::UnexpectedMethod {
                        expected: "Connection.Tune",
                        got: format!("{tune_frame:?}"),
                    },
                ));
            }
        };
        debug!(
            channel_max = tune.channel_max,
            frame_max = tune.frame_max,
            heartbeat = tune.heartbeat,
            "received Connection.Tune"
        );

        // Negotiate parameters
        let channel_max = negotiate(config.channel_max, tune.channel_max);
        let frame_max = negotiate_u32(config.frame_max, tune.frame_max);
        let heartbeat = negotiate(config.heartbeat, tune.heartbeat);

        // Send Connection.TuneOk
        let tune_ok = MethodFrame::ConnectionTuneOk(conn_method::TuneOk {
            channel_max,
            frame_max,
            heartbeat,
        });
        write_frame(&mut stream, &Frame::Method(0, tune_ok)).await?;

        // Send Connection.Open
        let open = MethodFrame::ConnectionOpen(conn_method::Open {
            virtual_host: ShortStr::try_from(config.vhost.as_str()).map_err(Error::Protocol)?,
        });
        write_frame(&mut stream, &Frame::Method(0, open)).await?;

        // Read Connection.OpenOk. Tune is settled, so honor the negotiated cap.
        let post_tune_cap = if frame_max == 0 { u32::MAX } else { frame_max };
        let open_ok_frame = read_frame(&mut stream, post_tune_cap).await?;
        match open_ok_frame {
            Frame::Method(0, MethodFrame::ConnectionOpenOk) => {}
            Frame::Method(0, MethodFrame::ConnectionClose(close)) => {
                return Err(Error::ConnectionClosed {
                    code: close.reply_code,
                    reason: close.reply_text.to_string(),
                });
            }
            _ => {
                return Err(Error::Protocol(
                    crate::error::ProtocolError::UnexpectedMethod {
                        expected: "Connection.OpenOk",
                        got: format!("{open_ok_frame:?}"),
                    },
                ));
            }
        }

        debug!("AMQP connection established");

        // --- Post-handshake: split stream and spawn tasks ---

        let (read_half, write_half) = tokio::io::split(stream);

        // Outgoing frame channel (unbounded so control frames never block)
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded_channel();

        // Publish-admission semaphore. Capacity-bytes throttling is applied
        // only on the publish path; control frames (acks, heartbeats, RPCs)
        // bypass this and always enqueue immediately.
        let publish_admission_capacity = config.publish_admission_bytes;
        let publish_admission = Arc::new(tokio::sync::Semaphore::new(
            publish_admission_capacity.min(tokio::sync::Semaphore::MAX_PERMITS),
        ));

        // Channel-ID allocator. AMQP caps at u16::MAX with 0 reserved for the
        // connection; if the broker negotiated a smaller channel_max, honor it.
        let id_cap = if channel_max == 0 {
            u16::MAX
        } else {
            channel_max
        };
        let id_alloc = Mutex::new(ChannelIdAllocator::new(id_cap));

        let shared = Arc::new(ConnectionShared {
            frame_tx: outgoing_tx,
            publish_admission,
            publish_admission_capacity,
            id_alloc,
            dead: tokio::sync::Notify::new(),
        });

        // Channel registry shared between Connection and reader task
        let channel_senders: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Frame>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Connection-level frames (channel 0) receiver
        let (conn_tx, conn_frames_rx) = mpsc::unbounded_channel();

        // Monotonic connection start time; last_recv stores elapsed nanos since conn_start
        let conn_start = Instant::now();
        let last_recv = Arc::new(AtomicU64::new(0));

        // Spawn writer task
        let writer_task = tokio::spawn(writer_loop(write_half, outgoing_rx));

        // Spawn reader task
        let reader_senders = channel_senders.clone();
        let reader_shared = shared.clone();
        let reader_last_recv = last_recv.clone();
        let reader_task = tokio::spawn(async move {
            reader_loop(
                read_half,
                reader_senders.clone(),
                conn_tx,
                reader_shared,
                reader_last_recv,
                conn_start,
                post_tune_cap,
            )
            .await;
            // The connection is gone: dropping every channel's sender ends
            // its dispatch task, which ends its consumers with
            // `ConnectionClosed`.
            reader_senders.lock().unwrap().clear();
        });

        // Spawn heartbeat task if enabled
        let heartbeat_task = if heartbeat > 0 {
            let hb_shared = shared.clone();
            let hb_last_recv = last_recv;
            Some(tokio::spawn(heartbeat_loop(
                heartbeat,
                hb_shared,
                hb_last_recv,
                conn_start,
            )))
        } else {
            None
        };

        Ok(Connection {
            shared,
            channel_senders,
            frame_max,
            rpc_timeout: config.rpc_timeout,
            reader_task,
            writer_task,
            heartbeat_task,
            conn_frames_rx: tokio::sync::Mutex::new(conn_frames_rx),
        })
    }

    /// Open a new AMQP channel on this connection.
    pub async fn open_channel(&self) -> Result<Channel, Error> {
        let channel_id = self.allocate_channel_id()?;

        // Create unbounded channel for incoming frames to this AMQP channel
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();

        // Register in the channel registry
        self.channel_senders
            .lock()
            .unwrap()
            .insert(channel_id, incoming_tx);

        // Send Channel.Open
        let frame = Frame::Method(channel_id, MethodFrame::ChannelOpen);
        self.shared
            .frame_tx
            .send(OutgoingFrame::Frame(frame))
            .map_err(|_| Error::ConnectionClosed {
                code: 0,
                reason: "connection write channel closed".into(),
            })?;

        // Wait for Channel.OpenOk
        let mut incoming_rx = incoming_rx;
        let response = match tokio::time::timeout(self.rpc_timeout, incoming_rx.recv()).await {
            Ok(Some(frame)) => frame,
            Ok(None) => {
                return Err(Error::ConnectionClosed {
                    code: 0,
                    reason: "connection closed while opening channel".into(),
                });
            }
            Err(_) => {
                self.channel_senders.lock().unwrap().remove(&channel_id);
                // The broker may still open it; close it before reusing the id.
                self.shared.close_without_waiting(channel_id);
                return Err(Error::Timeout);
            }
        };

        match response {
            Frame::Method(_, MethodFrame::ChannelOpenOk) => {}
            Frame::Method(_, MethodFrame::ChannelClose(close)) => {
                return Err(Error::ChannelClosed {
                    channel_id,
                    code: close.reply_code,
                    reason: close.reply_text.to_string(),
                });
            }
            _ => {
                return Err(Error::Protocol(
                    crate::error::ProtocolError::UnexpectedMethod {
                        expected: "Channel.OpenOk",
                        got: format!("{response:?}"),
                    },
                ));
            }
        }

        debug!(channel_id, "channel opened");

        Ok(Channel::new(
            channel_id,
            self.shared.clone(),
            self.frame_max,
            self.rpc_timeout,
            self.channel_senders.clone(),
            incoming_rx,
        ))
    }

    /// Gracefully close the connection.
    pub async fn close(self) -> Result<(), Error> {
        let close = Frame::Method(
            0,
            MethodFrame::ConnectionClose(conn_method::Close {
                reply_code: 200,
                reply_text: ShortStr::try_from("Normal shutdown").map_err(Error::Protocol)?,
                class_id: 0,
                method_id: 0,
            }),
        );
        self.shared
            .frame_tx
            .send(OutgoingFrame::Frame(close))
            .map_err(|_| Error::ConnectionClosed {
                code: 0,
                reason: "connection already closed".into(),
            })?;

        // Wait for Connection.CloseOk from the server
        let mut conn_frames_rx = self.conn_frames_rx.lock().await;
        while let Some(frame) = conn_frames_rx.recv().await {
            match frame {
                Frame::Method(0, MethodFrame::ConnectionCloseOk) => {
                    debug!("received Connection.CloseOk");
                    break;
                }
                _ => {
                    trace!(frame = ?frame, "ignoring frame while waiting for CloseOk");
                }
            }
        }

        // Signal writer to shut down
        let _ = self.shared.frame_tx.send(OutgoingFrame::Shutdown);

        // Signal writer to stop; abort tasks so Drop doesn't double-abort.
        self.writer_task.abort();
        self.reader_task.abort();
        if let Some(ref hb) = self.heartbeat_task {
            hb.abort();
        }

        debug!("connection closed");
        Ok(())
    }

    fn allocate_channel_id(&self) -> Result<u16, Error> {
        self.shared
            .id_alloc
            .lock()
            .unwrap()
            .allocate()
            .ok_or(Error::ChannelLimitReached)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Best-effort graceful close: ask the broker to terminate cleanly.
        // If close().await was already called, frame_tx is closed and these
        // sends silently fail. Otherwise the writer task drains its queue,
        // emits Connection.Close on the wire, then exits on Shutdown. We do
        // NOT abort the writer here so it has a chance to flush.
        if let Ok(reason) = ShortStr::try_from("Normal shutdown") {
            let close = Frame::Method(
                0,
                MethodFrame::ConnectionClose(conn_method::Close {
                    reply_code: 200,
                    reply_text: reason,
                    class_id: 0,
                    method_id: 0,
                }),
            );
            let _ = self.shared.frame_tx.send(OutgoingFrame::Frame(close));
            let _ = self.shared.frame_tx.send(OutgoingFrame::Shutdown);
        }
        self.reader_task.abort();
        // Aborting the reader skips its cleanup: end every channel here, so
        // their consumers end with `ConnectionClosed`.
        self.channel_senders.lock().unwrap().clear();
        if let Some(ref hb) = self.heartbeat_task {
            hb.abort();
        }
    }
}

/// Spec-mandated minimum `frame_max`. Pre-tune we have no negotiated value
/// so we cap reads at this size to defend against a hostile or buggy peer
/// claiming arbitrary frame sizes (could otherwise allocate ~4 GB).
const MIN_FRAME_MAX: u32 = 4096;

/// Read a single frame from the TCP stream (used during handshake).
///
/// Caps the accepted frame size at `max_size` bytes to prevent allocation
/// attacks from a peer that lies about the size in the frame header.
async fn read_frame<R: AsyncReadExt + Unpin>(
    stream: &mut R,
    max_size: u32,
) -> Result<Frame, Error> {
    // Read 7-byte header: type(1) + channel(2) + size(4)
    let mut header_buf = [0u8; 7];
    stream.read_exact(&mut header_buf).await?;

    let size_u32 = u32::from_be_bytes([header_buf[3], header_buf[4], header_buf[5], header_buf[6]]);
    if size_u32 > max_size {
        return Err(Error::FrameTooLarge {
            size: size_u32,
            max: max_size,
        });
    }
    let size = size_u32 as usize;

    // Read payload + frame end
    let mut payload_buf = vec![0u8; size + 1];
    stream.read_exact(&mut payload_buf).await?;

    let frame_end = payload_buf[size];
    if frame_end != frame::FRAME_END {
        return Err(Error::Protocol(
            crate::error::ProtocolError::InvalidFrameEnd(frame_end),
        ));
    }

    // Assemble full frame bytes and decode
    let mut full = BytesMut::with_capacity(7 + size + 1);
    full.extend_from_slice(&header_buf);
    full.extend_from_slice(&payload_buf);

    let mut bytes = full.freeze();
    let frame = Frame::decode(&mut bytes)?;
    Ok(frame)
}

/// Write a single frame to the TCP stream (used during handshake).
async fn write_frame<W: AsyncWriteExt + Unpin>(stream: &mut W, frame: &Frame) -> Result<(), Error> {
    let mut buf = BytesMut::new();
    frame.encode(&mut buf);
    stream.write_all(&buf).await?;
    Ok(())
}

/// Try to decode one frame from the read buffer.
/// Returns `None` if the buffer doesn't contain a complete frame yet.
/// Caps accepted frame sizes at `max_size` to defend against malformed peer.
fn try_decode_frame(buf: &mut BytesMut, max_size: u32) -> Result<Option<Frame>, Error> {
    if buf.len() < 7 {
        return Ok(None);
    }

    let size_u32 = u32::from_be_bytes([buf[3], buf[4], buf[5], buf[6]]);
    if size_u32 > max_size {
        return Err(Error::FrameTooLarge {
            size: size_u32,
            max: max_size,
        });
    }
    let size = size_u32 as usize;
    let frame_len = 7 + size + 1;

    if buf.len() < frame_len {
        return Ok(None);
    }

    let mut frame_bytes = buf.split_to(frame_len).freeze();
    let frame = Frame::decode(&mut frame_bytes)?;
    Ok(Some(frame))
}

/// Soft cap on a single `write_all` payload from the writer. Larger batches
/// are flushed in chunks. Sized to comfortably exceed typical TCP MSS while
/// keeping the local write buffer small enough that bursty traffic doesn't
/// pin a lot of memory.
const WRITER_BATCH_BYTES: usize = 64 * 1024;

/// Maximum frames drained per `recv_many` call. Bounds the inner encode loop
/// so a flood of tiny frames can't starve the runtime worker.
const WRITER_BATCH_FRAMES: usize = 256;

/// Writer task: drains outgoing frames in batches and coalesces them into a
/// single `write_all` per batch.
///
/// Most publishes produce three frames (method + header + body); without
/// batching that's three syscalls. Draining the mpsc with `recv_many` and
/// encoding into a single contiguous buffer collapses that to one syscall
/// and works much better with TLS records on the wire.
async fn writer_loop(
    mut write_half: tokio::io::WriteHalf<MaybeTls>,
    mut outgoing_rx: mpsc::UnboundedReceiver<OutgoingFrame>,
) {
    let mut write_buf = BytesMut::with_capacity(WRITER_BATCH_BYTES);
    let mut drained: Vec<OutgoingFrame> = Vec::with_capacity(WRITER_BATCH_FRAMES);

    loop {
        // Block waiting for at least one frame.
        let n = outgoing_rx
            .recv_many(&mut drained, WRITER_BATCH_FRAMES)
            .await;
        if n == 0 {
            debug!("outgoing channel closed");
            return;
        }

        write_buf.clear();
        // Permits collected for frames in this batch. Released only after
        // `write_all` succeeds for the buffer they were attached to.
        let mut held_permits: Vec<tokio::sync::OwnedSemaphorePermit> = Vec::new();
        let mut shutdown = false;
        for item in drained.drain(..) {
            match item {
                OutgoingFrame::Frame(frame) => frame.encode(&mut write_buf),
                OutgoingFrame::FrameWithPermit(frame, permit) => {
                    frame.encode(&mut write_buf);
                    held_permits.push(permit);
                }
                OutgoingFrame::Shutdown => {
                    shutdown = true;
                    break;
                }
            }
            // Soft byte cap: flush what we have and start a fresh batch so
            // a single huge body doesn't pin the buffer.
            if write_buf.len() >= WRITER_BATCH_BYTES {
                if let Err(e) = write_half.write_all(&write_buf).await {
                    error!(error = %e, "write error");
                    return;
                }
                write_buf.clear();
                // Release permits for frames whose bytes have hit the socket.
                held_permits.clear();
            }
        }
        if !write_buf.is_empty() {
            if let Err(e) = write_half.write_all(&write_buf).await {
                error!(error = %e, "write error");
                return;
            }
            held_permits.clear();
        }
        if shutdown {
            debug!("writer shutdown requested");
            return;
        }
        // Be polite: yield so other tasks on the same worker make progress
        // even if the mpsc is permanently saturated.
        tokio::task::yield_now().await;
    }
}

/// Delivers a frame to its channel. A cached sender may be stale: its channel
/// was dropped and the id reused by a new channel, so on a failed send the
/// registry is consulted again.
fn route_to_channel(
    channel_id: u16,
    frame: Frame,
    cached_senders: &mut HashMap<u16, mpsc::UnboundedSender<Frame>>,
    channel_senders: &Mutex<HashMap<u16, mpsc::UnboundedSender<Frame>>>,
) {
    let mut frame = frame;
    if let Some(cached) = cached_senders.get(&channel_id) {
        match cached.send(frame) {
            Ok(()) => return,
            Err(mpsc::error::SendError(f)) => frame = f,
        }
        let stale = cached_senders.remove(&channel_id).unwrap();
        forget_sender(channel_senders, channel_id, &stale);
    }
    let Some(sender) = channel_senders.lock().unwrap().get(&channel_id).cloned() else {
        warn!(channel_id, "frame for unknown channel");
        return;
    };
    if sender.send(frame).is_err() {
        warn!(channel_id, "channel receiver dropped");
        forget_sender(channel_senders, channel_id, &sender);
    } else {
        cached_senders.insert(channel_id, sender);
    }
}

/// Removes `sender` from the registry, unless the id now belongs to another channel.
fn forget_sender(
    channel_senders: &Mutex<HashMap<u16, mpsc::UnboundedSender<Frame>>>,
    channel_id: u16,
    sender: &mpsc::UnboundedSender<Frame>,
) {
    let mut senders = channel_senders.lock().unwrap();
    if senders
        .get(&channel_id)
        .is_some_and(|s| s.same_channel(sender))
    {
        senders.remove(&channel_id);
    }
}

/// Reader task: reads frames from the stream and routes them to the appropriate channel.
async fn reader_loop(
    mut read_half: tokio::io::ReadHalf<MaybeTls>,
    channel_senders: Arc<Mutex<HashMap<u16, mpsc::UnboundedSender<Frame>>>>,
    conn_tx: mpsc::UnboundedSender<Frame>,
    shared: Arc<ConnectionShared>,
    last_recv: Arc<AtomicU64>,
    conn_start: Instant,
    frame_max: u32,
) {
    let mut buf = BytesMut::with_capacity(256 * 1024);
    let mut cached_senders: HashMap<u16, mpsc::UnboundedSender<Frame>> = HashMap::new();

    loop {
        // Try to parse all complete frames already in the buffer
        loop {
            match try_decode_frame(&mut buf, frame_max) {
                Ok(Some(frame)) => {
                    // Update last-recv timestamp (elapsed nanos since conn_start)
                    let now = conn_start.elapsed().as_nanos() as u64;
                    last_recv.store(now, Ordering::Relaxed);

                    trace!(frame = ?frame, "received frame");

                    match frame {
                        Frame::Heartbeat => {
                            trace!("received heartbeat");
                        }
                        Frame::Method(0, MethodFrame::ConnectionClose(close)) => {
                            warn!(
                                code = close.reply_code,
                                reason = %close.reply_text,
                                "server closing connection"
                            );
                            let close_ok = Frame::Method(0, MethodFrame::ConnectionCloseOk);
                            let _ = shared.frame_tx.send(OutgoingFrame::Frame(close_ok));
                            let _ = shared.frame_tx.send(OutgoingFrame::Shutdown);
                            return;
                        }
                        Frame::Method(0, MethodFrame::ConnectionCloseOk) => {
                            debug!("received Connection.CloseOk");
                            let _ = conn_tx.send(Frame::Method(0, MethodFrame::ConnectionCloseOk));
                            return;
                        }
                        Frame::Method(0, MethodFrame::ConnectionBlocked(ref blocked)) => {
                            warn!(reason = %blocked.reason, "connection blocked by broker");
                        }
                        Frame::Method(0, MethodFrame::ConnectionUnblocked) => {
                            debug!("connection unblocked");
                        }
                        Frame::Method(0, _) => {
                            let _ = conn_tx.send(frame);
                        }
                        Frame::Method(
                            channel_id,
                            MethodFrame::ChannelCloseOk | MethodFrame::ChannelClose(_),
                        ) if shared.id_alloc.lock().unwrap().is_closing(channel_id) => {
                            // A channel dropped while open: its Close was sent
                            // on drop. On a simultaneous broker Close, answer it.
                            if matches!(frame, Frame::Method(_, MethodFrame::ChannelClose(_))) {
                                let close_ok =
                                    Frame::Method(channel_id, MethodFrame::ChannelCloseOk);
                                let _ = shared.frame_tx.send(OutgoingFrame::Frame(close_ok));
                            }
                            cached_senders.remove(&channel_id);
                            shared.id_alloc.lock().unwrap().finish_closing(channel_id);
                        }
                        Frame::Method(channel_id, _)
                        | Frame::Header(channel_id, _)
                        | Frame::Body(channel_id, _) => {
                            route_to_channel(
                                channel_id,
                                frame,
                                &mut cached_senders,
                                &channel_senders,
                            );
                        }
                    }
                }
                Ok(None) => break, // Need more data
                Err(e) => {
                    error!(error = %e, "frame decode error");
                    return;
                }
            }
        }

        // Read more data from the socket
        if buf.capacity() - buf.len() < 64 * 1024 {
            buf.reserve(256 * 1024);
        }
        let read = tokio::select! {
            read = read_half.read_buf(&mut buf) => read,
            () = shared.dead.notified() => {
                debug!("reader stopped: peer considered dead");
                return;
            }
        };
        match read {
            Ok(0) => {
                debug!("connection closed by peer");
                return;
            }
            Ok(_) => {}
            Err(e) => {
                error!(error = %e, "read error");
                return;
            }
        }
    }
}

/// Heartbeat task: sends heartbeats and checks for dead peers.
async fn heartbeat_loop(
    heartbeat_secs: u16,
    shared: Arc<ConnectionShared>,
    last_recv: Arc<AtomicU64>,
    conn_start: Instant,
) {
    let interval = Duration::from_secs(heartbeat_secs as u64);
    let send_interval = interval / 2;
    let timeout = interval * 2;

    loop {
        tokio::time::sleep(send_interval).await;

        if shared
            .frame_tx
            .send(OutgoingFrame::Frame(Frame::Heartbeat))
            .is_err()
        {
            debug!("heartbeat: outgoing channel closed");
            break;
        }

        let last_nanos = last_recv.load(Ordering::Relaxed);
        let now_nanos = conn_start.elapsed().as_nanos() as u64;
        let elapsed = Duration::from_nanos(now_nanos.saturating_sub(last_nanos));

        if elapsed > timeout {
            error!(
                elapsed_secs = elapsed.as_secs(),
                timeout_secs = timeout.as_secs(),
                "heartbeat timeout: no data received from peer"
            );
            // End the connection: stop the reader (which ends every channel)
            // and the writer.
            shared.dead.notify_one();
            let _ = shared.frame_tx.send(OutgoingFrame::Shutdown);
            break;
        }
    }
}

/// Negotiate a u16 parameter (0 means "no preference").
fn negotiate(client: u16, server: u16) -> u16 {
    match (client, server) {
        (0, s) => s,
        (c, 0) => c,
        (c, s) => c.min(s),
    }
}

/// Negotiate a u32 parameter (0 means "no preference").
fn negotiate_u32(client: u32, server: u32) -> u32 {
    match (client, server) {
        (0, s) => s,
        (c, 0) => c,
        (c, s) => c.min(s),
    }
}

/// Install the rustls `ring` crypto provider as the process-wide default,
/// once. rustls 0.23 panics if `ClientConfig::builder()` is called before a
/// provider is installed *and* the build has more than one provider feature
/// enabled (which can happen via dependency unification with `tokio-rustls`).
/// Calling `install_default()` ourselves is robust regardless of which
/// providers are linked in. If another provider is already installed by the
/// host application, the install is a no-op.
fn install_default_crypto_provider() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiate_both_zero() {
        assert_eq!(negotiate(0, 0), 0);
    }

    #[test]
    fn negotiate_client_zero_uses_server() {
        assert_eq!(negotiate(0, 2047), 2047);
    }

    #[test]
    fn negotiate_server_zero_uses_client() {
        assert_eq!(negotiate(100, 0), 100);
    }

    #[test]
    fn negotiate_picks_minimum() {
        assert_eq!(negotiate(100, 200), 100);
        assert_eq!(negotiate(200, 100), 100);
    }

    #[test]
    fn negotiate_equal_values() {
        assert_eq!(negotiate(50, 50), 50);
    }

    #[test]
    fn negotiate_u32_both_zero() {
        assert_eq!(negotiate_u32(0, 0), 0);
    }

    #[test]
    fn negotiate_u32_client_zero_uses_server() {
        assert_eq!(negotiate_u32(0, 131072), 131072);
    }

    #[test]
    fn negotiate_u32_server_zero_uses_client() {
        assert_eq!(negotiate_u32(65536, 0), 65536);
    }

    #[test]
    fn negotiate_u32_picks_minimum() {
        assert_eq!(negotiate_u32(65536, 131072), 65536);
        assert_eq!(negotiate_u32(131072, 65536), 65536);
    }

    #[test]
    fn allocator_increments_from_one() {
        let mut a = ChannelIdAllocator::new(u16::MAX);
        assert_eq!(a.allocate(), Some(1));
        assert_eq!(a.allocate(), Some(2));
        assert_eq!(a.allocate(), Some(3));
    }

    #[test]
    fn allocator_respects_cap() {
        let mut a = ChannelIdAllocator::new(2);
        assert_eq!(a.allocate(), Some(1));
        assert_eq!(a.allocate(), Some(2));
        assert_eq!(a.allocate(), None);
    }

    #[test]
    fn allocator_recycles_released_ids() {
        let mut a = ChannelIdAllocator::new(u16::MAX);
        let _ = a.allocate(); // 1
        let id2 = a.allocate().unwrap(); // 2
        let _ = a.allocate(); // 3
        a.release(id2);
        // Free-list pops smallest first; the recycled 2 comes back before 4.
        assert_eq!(a.allocate(), Some(2));
        assert_eq!(a.allocate(), Some(4));
    }

    #[test]
    fn allocator_release_ignores_zero_and_oob() {
        let mut a = ChannelIdAllocator::new(10);
        a.release(0); // reserved — should not enter free-list
        a.release(11); // beyond cap — ignore
        assert!(a.free.is_empty());
    }

    #[test]
    fn allocator_does_not_overflow_past_cap() {
        let mut a = ChannelIdAllocator::new(u16::MAX);
        a.next = u16::MAX;
        // Should succeed once at u16::MAX, then exhaust without panicking.
        assert_eq!(a.allocate(), Some(u16::MAX));
        assert_eq!(a.allocate(), None);
        // Release of MAX is honored (not zero, within cap).
        a.release(u16::MAX);
        assert_eq!(a.allocate(), Some(u16::MAX));
    }

    #[test]
    fn allocator_release_then_allocate_is_dense() {
        let mut a = ChannelIdAllocator::new(u16::MAX);
        for _ in 0..5 {
            a.allocate();
        } // 1..=5 used
        a.release(3);
        a.release(1);
        // Smallest free first: 1, then 3, then fresh 6.
        assert_eq!(a.allocate(), Some(1));
        assert_eq!(a.allocate(), Some(3));
        assert_eq!(a.allocate(), Some(6));
    }

    #[test]
    fn allocator_holds_closing_ids_until_close_ok() {
        let mut a = ChannelIdAllocator::new(u16::MAX);
        assert_eq!(a.allocate(), Some(1));
        a.mark_closing(1);
        assert_eq!(a.allocate(), Some(2));
        assert!(a.finish_closing(1));
        assert_eq!(a.allocate(), Some(1));
    }

    #[test]
    fn allocator_finish_closing_ignores_ids_not_closing() {
        let mut a = ChannelIdAllocator::new(u16::MAX);
        assert_eq!(a.allocate(), Some(1));
        assert!(!a.finish_closing(1));
        assert_eq!(a.allocate(), Some(2));
    }

    /// Encode an oversized fake frame header and verify the decoder rejects it.
    /// Catches a hostile peer claiming an arbitrary frame size pre-tune.
    #[test]
    fn try_decode_frame_rejects_oversize() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[1u8]); // frame_type = METHOD
        buf.extend_from_slice(&[0u8, 0]); // channel_id = 0
        // Claim size = u32::MAX. We never read past the header, so we don't
        // need to supply payload; the cap should reject before allocating.
        buf.extend_from_slice(&u32::MAX.to_be_bytes());

        let result = try_decode_frame(&mut buf, MIN_FRAME_MAX);
        assert!(matches!(
            result,
            Err(Error::FrameTooLarge { size, max })
                if size == u32::MAX && max == MIN_FRAME_MAX
        ));
    }

    #[test]
    fn try_decode_frame_accepts_within_cap() {
        // A valid heartbeat frame (size=0) is below any sensible cap.
        let mut buf = BytesMut::new();
        Frame::Heartbeat.encode(&mut buf);
        let result = try_decode_frame(&mut buf, MIN_FRAME_MAX).unwrap();
        assert_eq!(result, Some(Frame::Heartbeat));
    }
}
