use axum::{
    http::StatusCode,
    response::{IntoResponse, Response, sse::Event, sse::Sse},
};
use serde::Serialize;
use std::convert::Infallible;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::error::TransportError;

// ---------------------------------------------------------------------------
// SseEmitter
// ---------------------------------------------------------------------------

/// Server-side handle for pushing events to an SSE client.
///
/// Create via [`sse_pair`]. One `SseEmitter` per connected SSE client.
/// Dropping the emitter closes the SSE stream for that client.
#[derive(Clone)]
pub struct SseEmitter {
    tx: mpsc::UnboundedSender<Result<Event, Infallible>>,
}

impl SseEmitter {
    /// Send a plain-text SSE event with the given type and data string.
    pub fn send(&self, event_type: &str, data: String) -> Result<(), TransportError> {
        let event = Event::default().event(event_type).data(data);
        self.tx
            .send(Ok(event))
            .map_err(|_| TransportError::SseStreamClosed)
    }

    /// Serialise `data` to JSON and send it as an SSE event.
    pub fn send_json<T: Serialize>(
        &self,
        event_type: &str,
        data: &T,
    ) -> Result<(), TransportError> {
        let json =
            serde_json::to_string(data).map_err(|e| TransportError::EncodeError(e.to_string()))?;
        self.send(event_type, json)
    }

    /// Returns `true` if the client is still connected.
    pub fn is_alive(&self) -> bool {
        !self.tx.is_closed()
    }
}

// ---------------------------------------------------------------------------
// SseResponse — opaque wrapper to hide internal Axum stream types
// ---------------------------------------------------------------------------

/// Axum-compatible SSE response. Holds a pre-built [`Response`] so that
/// internal stream types do not leak into the public API.
pub struct SseResponse(Response);

impl IntoResponse for SseResponse {
    fn into_response(self) -> Response {
        self.0
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Create a linked `(SseEmitter, SseResponse)` pair.
///
/// - Return `SseResponse` from an Axum handler.
/// - Keep `SseEmitter` to push events to that specific client.
///
/// When the client disconnects the underlying `mpsc` channel closes, and
/// subsequent calls to [`SseEmitter::send`] return
/// [`TransportError::SseStreamClosed`].
pub fn sse_pair() -> (SseEmitter, SseResponse) {
    use futures_util::StreamExt;
    let (tx, rx) = mpsc::unbounded_channel();
    let stream = UnboundedReceiverStream::new(rx);
    // Eagerly build the Response so the KeepAliveStream type is erased.
    let response = Sse::new(stream.boxed())
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("keep-alive"),
        )
        .into_response();
    (SseEmitter { tx }, SseResponse(response))
}

// ---------------------------------------------------------------------------
// Broadcast helper
// ---------------------------------------------------------------------------

/// Broadcast an event to a collection of [`SseEmitter`]s, removing dead ones.
///
/// ```ignore
/// broadcast_sse_json(&mut state.emitters, "camera_status_changed", &status);
/// ```
pub fn broadcast_sse_json<T: Serialize>(
    emitters: &mut Vec<SseEmitter>,
    event_type: &str,
    data: &T,
) {
    emitters.retain(|e| {
        if !e.is_alive() {
            return false;
        }
        e.send_json(event_type, data).is_ok()
    });
}

// ---------------------------------------------------------------------------
// Error response helper
// ---------------------------------------------------------------------------

/// Convenience: return an SSE error response with a given HTTP status.
#[allow(dead_code)]
pub fn sse_error(status: StatusCode, message: &str) -> Response {
    (status, message.to_string()).into_response()
}
