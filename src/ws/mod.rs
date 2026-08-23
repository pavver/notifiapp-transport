//! WebSocket transport — primary, encrypted, binary.
//! Requires feature `ws`.

pub mod client;
#[cfg(not(target_arch = "wasm32"))]
pub mod server;

pub use client::backoff::{BackoffStrategy, ConstantBackoff, ExponentialBackoff, LinearBackoff};
pub use client::{WsClient, WsClientConfig};
#[cfg(not(target_arch = "wasm32"))]
pub use server::{ServerSessionHandle, WsServerConfig, accept_ws_session};
