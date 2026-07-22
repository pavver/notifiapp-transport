//! WebSocket transport — primary, encrypted, binary.
//! Requires feature `ws`.

mod client;
mod server;

pub use client::{WsClient, WsClientConfig};
pub use server::{ServerSessionHandle, WsServerConfig, accept_ws_session};
