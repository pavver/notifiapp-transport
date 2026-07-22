use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio::time::Duration;
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Message};
use uuid::Uuid;

use crate::{
    error::TransportError,
    frame::{Frame, FrameAccumulator, FrameKind, decode_frame, encode_frame},
    scheduler::{MessagePriority, WfqScheduler},
    state::ConnectionState,
};

#[cfg(feature = "crypto")]
use crate::crypto::NoiseSession;

// ---------------------------------------------------------------------------
// WsServerConfig
// ---------------------------------------------------------------------------

/// Configuration for accepting incoming WS sessions.
pub struct WsServerConfig {
    /// Protocol name this server expects from connecting clients.
    pub protocol_name: String,
    /// Server protocol version. Clients not matching receive PROTOCOL_REJECTED.
    pub protocol_version: String,
    /// Server Noise private key (32 bytes, X25519).
    /// Required when feature `crypto` is enabled.
    #[cfg(feature = "crypto")]
    pub noise_private_key: Vec<u8>,
    /// Maximum accepted payload size in bytes.
    pub max_payload_bytes: usize,
    /// How often the server sends heartbeat Pings.
    pub heartbeat_interval: Duration,
    /// How long to wait for a Pong before closing the connection.
    pub heartbeat_timeout: Duration,
}

impl WsServerConfig {
    pub fn new(
        protocol_name: impl Into<String>,
        protocol_version: impl Into<String>,
        #[cfg(feature = "crypto")] noise_private_key: Vec<u8>,
    ) -> Self {
        Self {
            protocol_name: protocol_name.into(),
            protocol_version: protocol_version.into(),
            #[cfg(feature = "crypto")]
            noise_private_key,
            max_payload_bytes: 4 * 1024 * 1024,
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(10),
        }
    }
}

// ---------------------------------------------------------------------------
// ServerSessionHandle
// ---------------------------------------------------------------------------

/// Cheap, cloneable handle for pushing frames to a connected client.
#[derive(Clone)]
pub struct ServerSessionHandle {
    pub id: Uuid,
    cmd_tx: mpsc::UnboundedSender<SessionCmd>,
    state_rx: watch::Receiver<ConnectionState>,
}

enum SessionCmd {
    Send {
        frame: Frame,
        priority: MessagePriority,
    },
    Close,
}

impl ServerSessionHandle {
    /// Send a response frame matched by the client's request `id`.
    pub fn respond(&self, id: u32, data: Vec<u8>) -> Result<(), TransportError> {
        self.cmd_tx
            .send(SessionCmd::Send {
                frame: Frame {
                    id,
                    kind: FrameKind::Message,
                    data,
                },
                priority: MessagePriority::Normal,
            })
            .map_err(|_| TransportError::ChannelError)
    }

    /// Push a server-initiated event frame (`id = 0`).
    pub fn push_event(
        &self,
        data: Vec<u8>,
        priority: MessagePriority,
    ) -> Result<(), TransportError> {
        self.cmd_tx
            .send(SessionCmd::Send {
                frame: Frame {
                    id: 0,
                    kind: FrameKind::Event,
                    data,
                },
                priority,
            })
            .map_err(|_| TransportError::ChannelError)
    }

    /// Gracefully close the session.
    pub fn close(&self) {
        self.cmd_tx.send(SessionCmd::Close).ok();
    }

    pub fn state(&self) -> ConnectionState {
        self.state_rx.borrow().clone()
    }

    pub fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.state_rx.clone()
    }

    /// Returns `true` if the underlying WS connection is still alive.
    pub fn is_alive(&self) -> bool {
        matches!(self.state(), ConnectionState::Online)
    }
}

// ---------------------------------------------------------------------------
// accept_ws_session
// ---------------------------------------------------------------------------

/// Perform the transport handshake on an incoming raw WS stream and return a
/// session handle + inbox receiver + background task future.
///
/// The function is **generic over the underlying I/O stream** — it works with
/// any `AsyncRead + AsyncWrite + Unpin + Send + 'static` stream, including:
/// - `tokio::net::TcpStream` (plain TCP)
/// - `tokio_rustls::server::TlsStream<TcpStream>` (TLS)
/// - Any other stream wrapped in `tokio_tungstenite::WebSocketStream`
///
/// For integration with **Axum**, upgrade the socket to a tungstenite stream
/// first using [`tokio_tungstenite::WebSocketStream::from_raw_socket`], or
/// use the provided [`accept_from_raw`] helper.
///
/// ## Axum example
///
/// ```ignore
/// use tokio_tungstenite::WebSocketStream;
///
/// async fn ws_handler(ws: WebSocketUpgrade) -> impl IntoResponse {
///     ws.on_upgrade(|axum_ws| async {
///         let stream = axum_ws_to_tungstenite(axum_ws);  // see helper below
///         match accept_ws_session(stream, &config).await {
///             Ok((handle, mut inbox, task)) => {
///                 tokio::spawn(task);
///                 while let Some(frame) = inbox.recv().await {
///                     handle.respond(frame.id, my_response_bytes).ok();
///                 }
///             }
///             Err(e) => tracing::warn!("Session failed: {}", e),
///         }
///     })
/// }
/// ```
pub async fn accept_ws_session<S>(
    mut ws: WebSocketStream<S>,
    config: &WsServerConfig,
) -> Result<
    (
        ServerSessionHandle,
        mpsc::UnboundedReceiver<Frame>,
        impl std::future::Future<Output = ()> + Send,
    ),
    TransportError,
>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // --- Protocol version handshake ---
    let hello_msg = match ws.next().await {
        Some(Ok(Message::Text(t))) => t.to_string(),
        _ => return Err(TransportError::ProtocolHandshakeFailed),
    };

    let expected_prefix = format!("{} ", config.protocol_name);
    if !hello_msg.starts_with(&expected_prefix) {
        let _ = ws
            .send(Message::Text(
                format!("PROTOCOL_REJECTED {}", config.protocol_version).into(),
            ))
            .await;
        return Err(TransportError::ProtocolHandshakeFailed);
    }

    let client_version = hello_msg
        .trim_start_matches(expected_prefix.as_str())
        .trim();
    if client_version != config.protocol_version {
        let _ = ws
            .send(Message::Text(
                format!("PROTOCOL_REJECTED {}", config.protocol_version).into(),
            ))
            .await;
        return Err(TransportError::VersionMismatch {
            client: client_version.to_string(),
            server: config.protocol_version.clone(),
        });
    }

    ws.send(Message::Text("PROTOCOL_ACCEPTED".into()))
        .await
        .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

    // --- Noise XX handshake (server responder) ---
    #[cfg(feature = "crypto")]
    let noise = {
        let mut noise = NoiseSession::server(&config.noise_private_key)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;
        let mut h_buf = vec![0u8; 65535];

        // <- E (initiator → responder)
        let data = match ws.next().await {
            Some(Ok(Message::Binary(d))) => d,
            _ => return Err(TransportError::NoiseHandshakeFailed("step 1: no E".into())),
        };
        noise
            .read_message(&data, &mut h_buf)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;

        // -> E, EE, S, ES
        let n = noise
            .write_message(&[], &mut h_buf)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;
        ws.send(Message::Binary(h_buf[..n].to_vec().into()))
            .await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        // <- S, SE
        let data = match ws.next().await {
            Some(Ok(Message::Binary(d))) => d,
            _ => {
                return Err(TransportError::NoiseHandshakeFailed(
                    "step 3: no S,SE".into(),
                ));
            }
        };
        noise
            .read_message(&data, &mut h_buf)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;

        if !noise.is_finished() {
            return Err(TransportError::NoiseHandshakeFailed(
                "handshake not complete".into(),
            ));
        }
        noise
    };

    // --- Build handles ---
    let session_id = Uuid::new_v4();
    let (state_tx, state_rx) = watch::channel(ConnectionState::Online);
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SessionCmd>();
    let (inbox_tx, inbox_rx) = mpsc::unbounded_channel::<Frame>();

    let handle = ServerSessionHandle {
        id: session_id,
        cmd_tx,
        state_rx,
    };

    let max_bytes = config.max_payload_bytes;
    let heartbeat_interval = config.heartbeat_interval;
    let heartbeat_timeout = config.heartbeat_timeout;

    let task = async move {
        run_server_session(
            ws,
            #[cfg(feature = "crypto")]
            noise,
            cmd_rx,
            inbox_tx,
            state_tx,
            max_bytes,
            heartbeat_interval,
            heartbeat_timeout,
        )
        .await;
    };

    Ok((handle, inbox_rx, task))
}

// ---------------------------------------------------------------------------
// Session background task
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_server_session<S>(
    mut ws: WebSocketStream<S>,
    #[cfg(feature = "crypto")] mut noise: NoiseSession,
    mut cmd_rx: mpsc::UnboundedReceiver<SessionCmd>,
    inbox_tx: mpsc::UnboundedSender<Frame>,
    state_tx: watch::Sender<ConnectionState>,
    max_bytes: usize,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut accumulator = FrameAccumulator::new();
    let mut scheduler = WfqScheduler::<Frame>::new();
    let mut heartbeat_deadline = tokio::time::Instant::now() + heartbeat_interval;
    let mut waiting_for_pong = false;

    loop {
        let hb = tokio::time::sleep_until(heartbeat_deadline);
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        heartbeat_deadline = tokio::time::Instant::now() + heartbeat_interval;
                        waiting_for_pong = false;

                        #[cfg(feature = "crypto")]
                        let decrypted = {
                            let mut buf = vec![0u8; data.len()];
                            match noise.read_message(&data, &mut buf) {
                                Ok(n) => { buf.truncate(n); buf }
                                Err(e) => {
                                    tracing::warn!("Server session noise decrypt error: {}", e);
                                    break;
                                }
                            }
                        };
                        #[cfg(not(feature = "crypto"))]
                        let decrypted = data.to_vec();

                        match accumulator.feed(&decrypted, max_bytes) {
                            Ok(Some(frame_bytes)) => {
                                match decode_frame(&frame_bytes) {
                                    Ok(frame) => {
                                        match frame.kind {
                                            FrameKind::Ping => {
                                                scheduler.enqueue(
                                                    Frame { id: 0, kind: FrameKind::Pong, data: vec![] },
                                                    MessagePriority::RealTime,
                                                );
                                            }
                                            FrameKind::Pong => { /* timer already reset */ }
                                            _ => {
                                                if inbox_tx.send(frame).is_err() {
                                                    break; // reader dropped
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("Server session frame decode error: {}", e);
                                        break;
                                    }
                                }
                            }
                            Ok(None) => {} // accumulating more chunks
                            Err(e) => {
                                tracing::warn!("Server session accumulator: {}", e);
                                break;
                            }
                        }
                    }
                    _ => break,
                }
            }

            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    SessionCmd::Send { frame, priority } => {
                        scheduler.enqueue(frame, priority);
                        let mut error = false;
                        #[allow(clippy::while_let_on_iterator)]
                        while let Some(frame) = scheduler.next() {
                            let encoded = match encode_frame(&frame) {
                                Ok(b) => b,
                                Err(_) => { error = true; break; }
                            };
                            #[cfg(feature = "crypto")]
                            let chunks = match noise.encrypt_chunked(&encoded) {
                                Ok(c) => c,
                                Err(_) => { error = true; break; }
                            };
                            #[cfg(not(feature = "crypto"))]
                            let chunks = vec![encoded];

                            for chunk in chunks {
                                if ws.send(Message::Binary(chunk.into())).await.is_err() {
                                    error = true;
                                    break;
                                }
                            }
                            if error { break; }
                        }
                        if error { break; }
                    }
                    SessionCmd::Close => break,
                }
            }

            _ = hb => {
                if waiting_for_pong {
                    tracing::debug!("Server session heartbeat timeout");
                    break;
                }
                let ping = Frame { id: 0, kind: FrameKind::Ping, data: vec![] };
                if let Ok(encoded) = encode_frame(&ping) {
                    #[cfg(feature = "crypto")]
                    if let Ok(chunks) = noise.encrypt_chunked(&encoded) {
                        for chunk in chunks {
                            if ws.send(Message::Binary(chunk.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    #[cfg(not(feature = "crypto"))]
                    let _ = ws.send(Message::Binary(encoded.into())).await;
                }
                waiting_for_pong = true;
                heartbeat_deadline = tokio::time::Instant::now() + heartbeat_timeout;
            }
        }
    }

    state_tx.send(ConnectionState::Disconnected).ok();
    let _ = ws.close(None).await;
}
