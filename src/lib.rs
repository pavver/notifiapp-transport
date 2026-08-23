//! # notifiapp-transport
//!
//! Shared transport layer for notifiapp services.
//!
//! ## Overview
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────────┐
//! │                    Your protocol library                       │
//! │   (notifiapp-protocol, notifiapp-camera-protocol, …)          │
//! │                                                               │
//! │   Serialize/deserialize business messages                     │
//! │   Implement AuthHandler for login/resume logic               │
//! └──────────────────────────┬────────────────────────────────────┘
//!                            │  raw Vec<u8>
//! ┌──────────────────────────▼────────────────────────────────────┐
//! │                  notifiapp-transport                          │
//! │                                                               │
//! │  WS+postcard (primary)       HTTP+JSON+SSE (fallback)        │
//! │  ─────────────────────       ──────────────────────────       │
//! │  WsClient (client mode)      HttpClient (client mode)        │
//! │  accept_ws_session (server)  SseEmitter + SseResponse        │
//! │  Noise XX encryption         Plain HTTPS                     │
//! │  WFQ scheduling              Simple request/response         │
//! │  Connection retry            SSE subscriptions               │
//! │  Heartbeat + reconnect       Auto-reconnect SSE              │
//! └───────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Feature flags
//!
//! | Feature  | Default | Description                                   |
//! |----------|---------|-----------------------------------------------|
//! | `ws`     | ✓       | WebSocket client and server session           |
//! | `crypto` | ✓       | Noise XX encryption for WS connections        |
//! | `http`   |         | HTTP+JSON client and Axum SSE server helpers  |
//!
//! ## Quick start (WS client)
//!
//! ```ignore
//! use notifiapp_transport::{
//!     ws::{WsClient, WsClientConfig},
//!     scheduler::MessagePriority,
//! };
//!
//! let config = WsClientConfig::new("MY_SERVICE", "1.0.0");
//! let client = WsClient::new(config, None);
//!
//! client.set_endpoint("ws://192.168.1.10:8080")?;
//!
//! client.on_event(|data| {
//!     // deserialise `data` with your protocol codec
//! });
//!
//! let response_bytes = client
//!     .request(my_request_bytes, MessagePriority::Normal)
//!     .await?;
//! ```
//!
//! ## Quick start (WS server)
//!
//! ```ignore
//! use notifiapp_transport::ws::{WsServerConfig, accept_ws_session};
//!
//! async fn ws_handler(ws: WebSocketUpgrade, cfg: Arc<WsServerConfig>) -> impl IntoResponse {
//!     ws.on_upgrade(|socket| async move {
//!         match accept_ws_session(socket, &cfg).await {
//!             Ok((handle, mut inbox, task)) => {
//!                 tokio::spawn(task);
//!                 while let Some(frame) = inbox.recv().await {
//!                     // handle frame.data bytes
//!                     handle.respond(frame.id, response_bytes).ok();
//!                 }
//!             }
//!             Err(e) => tracing::warn!("WS accept failed: {}", e),
//!         }
//!     })
//! }
//! ```

pub mod auth;
pub mod error;
pub mod frame;
pub mod scheduler;
pub mod state;
pub mod transport;

#[cfg(feature = "crypto")]
pub mod crypto;

#[cfg(feature = "ws")]
pub mod ws;

#[cfg(feature = "http")]
pub mod http;

// ---------------------------------------------------------------------------
// Convenient re-exports
// ---------------------------------------------------------------------------

pub use auth::{AuthHandler, AuthOutcome, NoAuth};
pub use error::TransportError;
pub use frame::{Frame, FrameKind};
pub use scheduler::MessagePriority;
pub use state::ConnectionState;
pub use transport::Transport;

#[cfg(feature = "crypto")]
pub use crypto::NoiseSession;
#[cfg(feature = "crypto")]
pub use error::CryptoError;

#[cfg(feature = "ws")]
pub use ws::{
    BackoffStrategy, ConstantBackoff, ExponentialBackoff, LinearBackoff, WsClient, WsClientConfig,
};

#[cfg(all(feature = "ws", not(target_arch = "wasm32")))]
pub use ws::{ServerSessionHandle, WsServerConfig, accept_ws_session};

pub mod runtime;
pub(crate) mod utils;

#[cfg(feature = "http")]
pub use http::{HttpClient, HttpClientConfig, SseEmitter, SseResponse, SseSubscription, sse_pair};
