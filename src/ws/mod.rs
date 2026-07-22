//! WebSocket transport — primary, encrypted, binary.
//! Requires feature `ws`.

pub mod client;
pub mod server;

pub use client::{WsClient, WsClientConfig};
pub use server::{ServerSessionHandle, WsServerConfig, accept_ws_session};
