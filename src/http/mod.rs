//! HTTP+JSON+SSE transport — optional debug/fallback.
//! Enable with feature `http`.

mod client;
mod sse;

pub use client::{HttpClient, HttpClientConfig, SseSubscription};
pub use sse::{SseEmitter, SseResponse, broadcast_sse_json, sse_pair};
