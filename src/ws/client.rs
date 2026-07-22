use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::{
    auth::{AuthHandler, AuthOutcome, NoAuth},
    endpoint::{EndpointHandle, EndpointPriority, EndpointRegistry},
    error::TransportError,
    frame::{Frame, FrameAccumulator, FrameKind, decode_frame, encode_frame},
    scheduler::{MessagePriority, WfqScheduler},
    state::ConnectionState,
};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, sleep, timeout};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[cfg(feature = "crypto")]
use crate::crypto::NoiseSession;

// ---------------------------------------------------------------------------
// Timeouts and limits
// ---------------------------------------------------------------------------

/// Maximum time to wait for a server response to any single request.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum time to wait for the server's auth response.
const DEFAULT_AUTH_TIMEOUT: Duration = Duration::from_secs(15);
/// Ping sent after this interval of incoming silence.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// Connection considered dead if no Pong arrives within this window after Ping.
const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default maximum payload size (4 MiB).
const DEFAULT_MAX_PAYLOAD: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for [`WsClient`].
pub struct WsClientConfig {
    /// Human-readable protocol name sent during the version handshake.
    /// Example: `"NOTIFI_APP"`, `"CAMERA_SERVICE"`.
    pub protocol_name: String,
    /// Protocol version string (e.g., `"1.0.0"`).
    pub protocol_version: String,
    /// How long between heartbeat Pings.
    pub heartbeat_interval: Duration,
    /// How long to wait for a Pong before declaring the connection dead.
    pub heartbeat_timeout: Duration,
    /// Timeout for individual application-level request/response pairs.
    pub request_timeout: Duration,
    /// Timeout for the initial auth exchange after Noise handshake.
    pub auth_timeout: Duration,
    /// Maximum accepted incoming payload size.
    pub max_payload_bytes: usize,
    /// Optional pinned server Noise public key (trust-on-first-use if `None`).
    #[cfg(feature = "crypto")]
    pub noise_server_key: Option<Vec<u8>>,
}

impl WsClientConfig {
    pub fn new(protocol_name: impl Into<String>, protocol_version: impl Into<String>) -> Self {
        Self {
            protocol_name: protocol_name.into(),
            protocol_version: protocol_version.into(),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            auth_timeout: DEFAULT_AUTH_TIMEOUT,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD,
            #[cfg(feature = "crypto")]
            noise_server_key: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal command channel
// ---------------------------------------------------------------------------

enum ClientCmd {
    Send {
        frame: Frame,
        priority: MessagePriority,
    },
    SwitchEndpoint,
    EndpointsChanged,
    WakeUp,
}

// ---------------------------------------------------------------------------
// WsClient
// ---------------------------------------------------------------------------

/// Type alias to keep the event handler field declaration readable.
type EventHandlerStorage = Arc<parking_lot::RwLock<Option<Arc<dyn Fn(Vec<u8>) + Send + Sync>>>>;

/// Async WebSocket client with:
/// - Automatic endpoint failover and reconnection with exponential backoff
/// - Noise XX encryption
/// - WFQ priority scheduling for outgoing messages
/// - Request/response correlation via per-request ID
/// - Server push events dispatched to registered callbacks
pub struct WsClient {
    next_id: AtomicU32,
    state_tx: watch::Sender<ConnectionState>,
    state_rx: watch::Receiver<ConnectionState>,
    cmd_tx: mpsc::UnboundedSender<ClientCmd>,
    /// Awaiting responses: request_id → oneshot sender.
    pending: DashMap<u32, oneshot::Sender<Result<Vec<u8>, TransportError>>>,
    /// Server-push event callback. All events are dispatched here.
    event_handler: EventHandlerStorage,
    endpoints: EndpointRegistry,
    config: Arc<WsClientConfig>,
    auth: Arc<dyn AuthHandler>,
}

impl WsClient {
    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /// Create a new client and start the background connection loop.
    ///
    /// The client will not connect until at least one endpoint is added via
    /// [`add_endpoint`] and (if an `AuthHandler` was supplied) auth data is
    /// provided.
    pub fn new(config: WsClientConfig, auth: Option<Arc<dyn AuthHandler>>) -> Arc<Self> {
        let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let client = Arc::new(Self {
            next_id: AtomicU32::new(2), // 0 = events, 1 = auth
            state_tx,
            state_rx,
            cmd_tx,
            pending: DashMap::new(),
            event_handler: Arc::new(parking_lot::RwLock::new(None)),
            endpoints: EndpointRegistry::new(),
            config: Arc::new(config),
            auth: auth.unwrap_or_else(|| Arc::new(NoAuth)),
        });

        let bg = Arc::clone(&client);
        tokio::spawn(async move { bg.connection_loop(cmd_rx).await });

        client
    }

    // -----------------------------------------------------------------------
    // Endpoint management
    // -----------------------------------------------------------------------

    pub fn add_endpoint(
        &self,
        url: &str,
        priority: EndpointPriority,
    ) -> Result<EndpointHandle, TransportError> {
        let handle = self.endpoints.add(url, priority)?;
        self.cmd_tx.send(ClientCmd::EndpointsChanged).ok();
        Ok(handle)
    }

    pub fn remove_endpoint(&self, handle: &EndpointHandle) {
        self.endpoints.remove(handle);
        self.cmd_tx.send(ClientCmd::EndpointsChanged).ok();
    }

    pub fn switch_to_endpoint(&self, handle: &EndpointHandle) {
        self.endpoints.switch_to(handle);
        self.cmd_tx.send(ClientCmd::SwitchEndpoint).ok();
    }

    pub fn clear_forced_endpoint(&self) {
        self.endpoints.clear_forced();
        self.cmd_tx.send(ClientCmd::EndpointsChanged).ok();
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    pub fn state(&self) -> ConnectionState {
        self.state_rx.borrow().clone()
    }

    pub fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.state_rx.clone()
    }

    // -----------------------------------------------------------------------
    // Event callback
    // -----------------------------------------------------------------------

    /// Register a handler for server-push events.
    ///
    /// Events are `Frame { kind: Event, id: 0, data }` frames received while
    /// `Online`. The `data` bytes are forwarded verbatim to this callback;
    /// the protocol layer is responsible for deserialising them.
    ///
    /// Only one handler can be registered at a time. Subsequent calls replace
    /// the previous handler.
    pub fn on_event(&self, handler: impl Fn(Vec<u8>) + Send + Sync + 'static) {
        *self.event_handler.write() = Some(Arc::new(handler));
    }

    // -----------------------------------------------------------------------
    // Sending
    // -----------------------------------------------------------------------

    /// Send a request and await the server's response.
    ///
    /// Serialise your request with your chosen codec, pass the bytes here.
    /// The returned bytes are the server's raw response payload — deserialise
    /// according to your protocol.
    pub async fn request(
        &self,
        data: Vec<u8>,
        priority: MessagePriority,
    ) -> Result<Vec<u8>, TransportError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);

        let frame = Frame {
            id,
            kind: FrameKind::Message,
            data,
        };
        self.cmd_tx
            .send(ClientCmd::Send { frame, priority })
            .map_err(|_| TransportError::ChannelError)?;

        match timeout(self.config.request_timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.pending.remove(&id);
                Err(TransportError::ChannelError)
            }
            Err(_) => {
                self.pending.remove(&id);
                Err(TransportError::RequestTimeout)
            }
        }
    }

    /// Fire-and-forget: send a frame with no response expected.
    pub fn send_event(
        &self,
        data: Vec<u8>,
        priority: MessagePriority,
    ) -> Result<(), TransportError> {
        let frame = Frame {
            id: 0,
            kind: FrameKind::Event,
            data,
        };
        self.cmd_tx
            .send(ClientCmd::Send { frame, priority })
            .map_err(|_| TransportError::ChannelError)
    }

    /// Wake the connection loop (e.g., after new auth data is provided).
    pub fn wake(&self) {
        self.cmd_tx.send(ClientCmd::WakeUp).ok();
    }

    // -----------------------------------------------------------------------
    // Connection loop
    // -----------------------------------------------------------------------

    async fn connection_loop(self: Arc<Self>, mut cmd_rx: mpsc::UnboundedReceiver<ClientCmd>) {
        let mut backoff_ms: u64 = 500;

        loop {
            // Wait until we have at least one endpoint.
            loop {
                if !self.endpoints.is_empty() {
                    break;
                }
                tokio::select! {
                    _ = sleep(Duration::from_millis(500)) => {}
                    Some(_) = cmd_rx.recv() => {}
                }
            }

            let urls = self.endpoints.ordered();
            let mut connected = false;

            'endpoints: for (ep_id, url) in &urls {
                self.state_tx.send(ConnectionState::Connecting).ok();

                // --- Open WS connection ---
                let mut ws = match connect_async(url.as_str()).await {
                    Ok((ws, _)) => ws,
                    Err(e) => {
                        self.state_tx
                            .send(ConnectionState::Error(e.to_string()))
                            .ok();
                        continue 'endpoints;
                    }
                };

                // --- Protocol version handshake ---
                self.state_tx.send(ConnectionState::Handshaking).ok();
                let hello = format!(
                    "{} {}",
                    self.config.protocol_name, self.config.protocol_version
                );
                if ws.send(Message::Text(hello.into())).await.is_err() {
                    continue 'endpoints;
                }
                match ws.next().await {
                    Some(Ok(Message::Text(reply))) if reply.contains("PROTOCOL_ACCEPTED") => {}
                    Some(Ok(Message::Text(reply))) if reply.contains("PROTOCOL_REJECTED") => {
                        let server_ver = reply
                            .strip_prefix("PROTOCOL_REJECTED ")
                            .unwrap_or("unknown")
                            .trim()
                            .to_string();
                        self.state_tx
                            .send(ConnectionState::VersionMismatch {
                                client: self.config.protocol_version.clone(),
                                server: server_ver,
                            })
                            .ok();
                        // All endpoints on this host will have the same version — wait for
                        // an EndpointsChanged command before retrying.
                        loop {
                            tokio::select! {
                                _ = sleep(Duration::from_millis(500)) => {}
                                Some(cmd) = cmd_rx.recv() => {
                                    if matches!(cmd, ClientCmd::EndpointsChanged | ClientCmd::SwitchEndpoint) {
                                        break;
                                    }
                                }
                            }
                            if !matches!(self.state(), ConnectionState::VersionMismatch { .. }) {
                                break;
                            }
                        }
                        break 'endpoints;
                    }
                    _ => continue 'endpoints,
                }

                // --- Noise XX handshake ---
                #[cfg(feature = "crypto")]
                let mut noise = {
                    self.state_tx.send(ConnectionState::Authenticating).ok();
                    let key = self.config.noise_server_key.as_deref();
                    let mut noise = match NoiseSession::client(key) {
                        Ok(n) => n,
                        Err(e) => {
                            tracing::error!("Noise init failed: {}", e);
                            continue 'endpoints;
                        }
                    };
                    let mut h_buf = vec![0u8; 65535];

                    // -> E
                    let n = match noise.write_message(&[], &mut h_buf) {
                        Ok(n) => n,
                        Err(_) => continue 'endpoints,
                    };
                    if ws
                        .send(Message::Binary(h_buf[..n].to_vec().into()))
                        .await
                        .is_err()
                    {
                        continue 'endpoints;
                    }
                    // <- E, EE, S, ES
                    match ws.next().await {
                        Some(Ok(Message::Binary(data))) => {
                            if noise.read_message(&data, &mut h_buf).is_err() {
                                continue 'endpoints;
                            }
                        }
                        _ => continue 'endpoints,
                    }
                    // -> S, SE
                    let n = match noise.write_message(&[], &mut h_buf) {
                        Ok(n) => n,
                        Err(_) => continue 'endpoints,
                    };
                    if ws
                        .send(Message::Binary(h_buf[..n].to_vec().into()))
                        .await
                        .is_err()
                    {
                        continue 'endpoints;
                    }
                    if !noise.is_finished() {
                        continue 'endpoints;
                    }
                    noise
                };

                // --- Application-level auth ---
                if let Some(auth_bytes) = self.auth.auth_payload().await {
                    self.state_tx.send(ConnectionState::WaitingForAuth).ok();
                    let auth_frame = Frame {
                        id: 1,
                        kind: FrameKind::Message,
                        data: auth_bytes,
                    };
                    let encoded = match encode_frame(&auth_frame) {
                        Ok(b) => b,
                        Err(_) => continue 'endpoints,
                    };

                    #[cfg(feature = "crypto")]
                    let send_result = {
                        match noise.encrypt_chunked(&encoded) {
                            Ok(chunks) => {
                                let mut ok = true;
                                for chunk in chunks {
                                    if ws.send(Message::Binary(chunk.into())).await.is_err() {
                                        ok = false;
                                        break;
                                    }
                                }
                                ok
                            }
                            Err(_) => false,
                        }
                    };
                    #[cfg(not(feature = "crypto"))]
                    let send_result = ws.send(Message::Binary(encoded.into())).await.is_ok();

                    if !send_result {
                        continue 'endpoints;
                    }

                    // Await auth response with timeout.
                    let auth_result = timeout(
                        self.config.auth_timeout,
                        Self::recv_one_frame(
                            &mut ws,
                            #[cfg(feature = "crypto")]
                            &mut noise,
                            self.config.max_payload_bytes,
                        ),
                    )
                    .await;

                    let response_data = match auth_result {
                        Ok(Ok(frame)) if frame.id == 1 => frame.data,
                        _ => continue 'endpoints,
                    };

                    match self.auth.process_auth_response(&response_data).await {
                        AuthOutcome::Success => {}
                        AuthOutcome::Unauthorized => {
                            self.state_tx.send(ConnectionState::Unauthorized).ok();
                            break 'endpoints;
                        }
                        AuthOutcome::Failed => continue 'endpoints,
                        AuthOutcome::RetryWithNewPayload(_) => continue 'endpoints,
                    }
                }

                // --- Online ---
                *self.endpoints.last_connected.write() = Some(*ep_id);
                backoff_ms = 500;
                connected = true;
                self.state_tx.send(ConnectionState::Online).ok();

                let mut accumulator = FrameAccumulator::new();
                let mut scheduler = WfqScheduler::<Frame>::new();
                let mut heartbeat_deadline =
                    tokio::time::Instant::now() + self.config.heartbeat_interval;
                let mut waiting_for_pong = false;

                // --- Message loop ---
                loop {
                    let hb = tokio::time::sleep_until(heartbeat_deadline);
                    tokio::select! {
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Binary(data))) => {
                                    heartbeat_deadline =
                                        tokio::time::Instant::now() + self.config.heartbeat_interval;
                                    waiting_for_pong = false;

                                    #[cfg(feature = "crypto")]
                                    let decrypted = {
                                        let mut buf = vec![0u8; data.len()];
                                        match noise.read_message(&data, &mut buf) {
                                            Ok(n) => { buf.truncate(n); buf }
                                            Err(_) => break,
                                        }
                                    };
                                    #[cfg(not(feature = "crypto"))]
                                    let decrypted = data.to_vec();

                                    match accumulator.feed(&decrypted, self.config.max_payload_bytes) {
                                        Ok(Some(frame_bytes)) => {
                                            match decode_frame(&frame_bytes) {
                                                Ok(frame) => self.dispatch_frame(frame),
                                                Err(e) => {
                                                    tracing::warn!("Frame decode error: {}", e);
                                                    break;
                                                }
                                            }
                                        }
                                        Ok(None) => {} // need more chunks
                                        Err(e) => {
                                            tracing::warn!("Frame accumulator error: {}", e);
                                            break;
                                        }
                                    }
                                }
                                _ => break,
                            }
                        }

                        Some(cmd) = cmd_rx.recv() => {
                            match cmd {
                                ClientCmd::Send { frame, priority } => {
                                    scheduler.enqueue(frame, priority);
                                    // Drain scheduler, interleaving with recv-side checks.
                                    let mut send_error = false;
                                    #[allow(clippy::while_let_on_iterator)]
                                    while let Some(frame) = scheduler.next() {
                                        let encoded = match encode_frame(&frame) {
                                            Ok(b) => b,
                                            Err(_) => { send_error = true; break; }
                                        };

                                        #[cfg(feature = "crypto")]
                                        let chunks = match noise.encrypt_chunked(&encoded) {
                                            Ok(c) => c,
                                            Err(_) => { send_error = true; break; }
                                        };
                                        #[cfg(not(feature = "crypto"))]
                                        let chunks = vec![encoded];

                                        for chunk in chunks {
                                            if ws.send(Message::Binary(chunk.into())).await.is_err() {
                                                send_error = true;
                                                break;
                                            }
                                        }
                                        if send_error { break; }

                                        // Drain any commands that arrived while sending.
                                        let mut reconnect = false;
                                        while let Ok(queued) = cmd_rx.try_recv() {
                                            match queued {
                                                ClientCmd::Send { frame: f, priority: p } => {
                                                    scheduler.enqueue(f, p);
                                                }
                                                ClientCmd::SwitchEndpoint
                                                | ClientCmd::EndpointsChanged => {
                                                    let _ = ws.close(None).await;
                                                    reconnect = true;
                                                }
                                                ClientCmd::WakeUp => {}
                                            }
                                        }
                                        if reconnect || send_error { break; }
                                    }
                                    if send_error { break; }
                                }
                                ClientCmd::SwitchEndpoint | ClientCmd::EndpointsChanged => {
                                    let _ = ws.close(None).await;
                                    break;
                                }
                                ClientCmd::WakeUp => {}
                            }
                        }

                        _ = hb => {
                            if waiting_for_pong {
                                // Heartbeat timeout — close dead connection.
                                let _ = ws.close(None).await;
                                break;
                            }
                            // Send Ping frame.
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
                            heartbeat_deadline =
                                tokio::time::Instant::now() + self.config.heartbeat_timeout;
                        }
                    }
                }

                self.state_tx.send(ConnectionState::Disconnected).ok();
                // Fail all pending requests so callers don't hang.
                self.pending.retain(|_, _| false);
                break 'endpoints;
            }

            if !connected {
                self.state_tx.send(ConnectionState::Disconnected).ok();
                self.pending.retain(|_, _| false);
                tokio::select! {
                    _ = sleep(Duration::from_millis(backoff_ms)) => {}
                    Some(_) = cmd_rx.recv() => {}
                }
                backoff_ms = (backoff_ms * 2).min(60_000);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn dispatch_frame(&self, frame: Frame) {
        match frame.kind {
            FrameKind::Message if frame.id > 0 => {
                if let Some((_, tx)) = self.pending.remove(&frame.id) {
                    let _ = tx.send(Ok(frame.data));
                }
            }
            FrameKind::Event | FrameKind::Message => {
                // id = 0: server push event.
                if let Some(handler) = self.event_handler.read().clone() {
                    handler(frame.data);
                }
            }
            FrameKind::Pong => {
                // Handled implicitly by resetting the heartbeat timer on any recv.
            }
            FrameKind::Ping => {
                // Auto-respond with Pong (fire-and-forget, best-effort).
                let pong = Frame {
                    id: 0,
                    kind: FrameKind::Pong,
                    data: vec![],
                };
                self.cmd_tx
                    .send(ClientCmd::Send {
                        frame: pong,
                        priority: MessagePriority::RealTime,
                    })
                    .ok();
            }
        }
    }

    /// Read exactly one complete frame from the WS stream.
    /// Used during auth handshake before the main message loop starts.
    async fn recv_one_frame<S>(
        ws: &mut S,
        #[cfg(feature = "crypto")] noise: &mut NoiseSession,
        max_bytes: usize,
    ) -> Result<Frame, TransportError>
    where
        S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        let mut acc = FrameAccumulator::new();
        while let Some(Ok(Message::Binary(data))) = ws.next().await {
            #[cfg(feature = "crypto")]
            let decrypted = {
                let mut buf = vec![0u8; data.len()];
                let n = noise
                    .read_message(&data, &mut buf)
                    .map_err(|e| TransportError::NoiseDecryptFailed(e.to_string()))?;
                buf.truncate(n);
                buf
            };
            #[cfg(not(feature = "crypto"))]
            let decrypted = data.to_vec();

            if let Some(frame_bytes) = acc.feed(&decrypted, max_bytes)? {
                return decode_frame(&frame_bytes);
            }
        }
        Err(TransportError::ConnectionClosed)
    }
}
