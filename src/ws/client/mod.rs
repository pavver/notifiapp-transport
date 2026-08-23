pub mod backoff;
pub mod cmd;
pub mod config;
pub mod connection_loop;
pub mod stream;
pub mod transport;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::runtime::{spawn, timeout};
use crate::{
    auth::{AuthHandler, NoAuth},
    error::TransportError,
    frame::{Frame, FrameKind},
    scheduler::MessagePriority,
    state::ConnectionState,
};
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot, watch};

use cmd::ClientCmd;
pub use config::WsClientConfig;

/// Type alias to keep the event handler field declaration readable.
type EventHandlerStorage = Arc<parking_lot::RwLock<Option<Arc<dyn Fn(Vec<u8>) + Send + Sync>>>>;

/// Async WebSocket client with:
/// - Automatic endpoint failover and reconnection with exponential backoff
/// - Noise XX encryption
/// - WFQ priority scheduling for outgoing messages
/// - Request/response correlation via per-request ID
/// - Server push events dispatched to registered callbacks
pub struct WsClient {
    pub(crate) next_id: AtomicU32,
    pub(crate) state_tx: watch::Sender<ConnectionState>,
    pub(crate) state_rx: watch::Receiver<ConnectionState>,
    pub(crate) cmd_tx: mpsc::Sender<ClientCmd>,
    /// Awaiting responses: request_id → oneshot sender.
    pub(crate) pending: DashMap<u32, oneshot::Sender<Result<Vec<u8>, TransportError>>>,
    /// Server-push event callback. All events are dispatched here.
    pub(crate) event_handler: EventHandlerStorage,
    pub(crate) url: parking_lot::RwLock<Option<String>>,
    pub(crate) auto_reconnect: std::sync::atomic::AtomicBool,
    pub(crate) config: Arc<WsClientConfig>,
    pub(crate) auth: Arc<dyn AuthHandler>,
    pub(crate) cancel: tokio_util::sync::CancellationToken,
}

impl WsClient {
    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /// Create a new client and start the background connection loop.
    ///
    /// The client will not connect until an endpoint is set via
    /// [`set_endpoint`] and (if an `AuthHandler` was supplied) auth data is
    /// provided.
    pub fn new(config: WsClientConfig, auth: Option<Arc<dyn AuthHandler>>) -> Arc<Self> {
        let (state_tx, state_rx) = watch::channel(ConnectionState::Disconnected);
        let (cmd_tx, cmd_rx) = mpsc::channel(config.buffer_size);
        let cancel = tokio_util::sync::CancellationToken::new();

        let client = Arc::new(Self {
            next_id: AtomicU32::new(2), // 0 = events, 1 = auth
            state_tx,
            state_rx,
            cmd_tx,
            pending: DashMap::new(),
            event_handler: Arc::new(parking_lot::RwLock::new(None)),
            url: parking_lot::RwLock::new(None),
            auto_reconnect: std::sync::atomic::AtomicBool::new(true),
            config: Arc::new(config),
            auth: auth.unwrap_or_else(|| Arc::new(NoAuth)),
            cancel,
        });

        let bg = Arc::clone(&client);
        spawn(async move { bg.connection_loop(cmd_rx).await });

        client
    }

    // -----------------------------------------------------------------------
    // Endpoint management & Reconnect Controls
    // -----------------------------------------------------------------------

    /// Set the WS endpoint URL (e.g. `"ws://192.168.1.10:8080"`).
    pub fn set_endpoint(&self, url_str: &str) -> Result<(), TransportError> {
        url::Url::parse(url_str).map_err(|_| TransportError::InvalidUrl(url_str.to_string()))?;
        *self.url.write() = Some(url_str.to_string());
        self.cmd_tx.try_send(ClientCmd::EndpointChanged).ok();
        Ok(())
    }

    /// Clear current endpoint, stopping active connection.
    pub fn clear_endpoint(&self) {
        *self.url.write() = None;
        self.cmd_tx.try_send(ClientCmd::EndpointChanged).ok();
    }

    /// Retrieve the current endpoint URL if set.
    pub fn endpoint(&self) -> Option<String> {
        self.url.read().clone()
    }

    /// Control whether the transport automatically attempts to reconnect on drops.
    pub fn set_auto_reconnect(&self, enabled: bool) {
        self.auto_reconnect.store(enabled, Ordering::SeqCst);
        if enabled {
            self.reconnect();
        }
    }

    /// Returns whether auto-reconnect is enabled.
    pub fn auto_reconnect(&self) -> bool {
        self.auto_reconnect.load(Ordering::SeqCst)
    }

    /// Instantly trigger a connection or reconnect attempt, resetting backoff timers.
    pub fn reconnect(&self) {
        self.cmd_tx.try_send(ClientCmd::Reconnect).ok();
    }

    /// Shutdown the client and terminate the background connection loop.
    pub fn shutdown(&self) {
        self.cancel.cancel();
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
            .await
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
            .try_send(ClientCmd::Send { frame, priority })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => TransportError::BufferFull,
                _ => TransportError::ChannelError,
            })
    }

    /// Wake the connection loop (e.g., after new auth data is provided).
    pub fn wake(&self) {
        self.cmd_tx.try_send(ClientCmd::WakeUp).ok();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    pub(crate) fn dispatch_frame(&self, frame: Frame) {
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
                    .try_send(ClientCmd::Send {
                        frame: pong,
                        priority: MessagePriority::RealTime,
                    })
                    .ok();
            }
        }
    }
}
