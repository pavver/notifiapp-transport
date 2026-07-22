use crate::error::TransportError;
use async_trait::async_trait;

// ---------------------------------------------------------------------------
// AuthOutcome
// ---------------------------------------------------------------------------

/// Outcome returned by [`AuthHandler::process_auth_response`].
#[derive(Debug)]
pub enum AuthOutcome {
    /// Credentials accepted — connection moves to `Online`.
    Success,
    /// Server explicitly rejected credentials (wrong password, banned, etc.).
    /// The connection loop will stop retrying without new credentials.
    Unauthorized,
    /// Network or protocol error — try the next endpoint.
    Failed,
    /// Session token expired; new credentials provided in the returned bytes.
    /// The transport will send them as a fresh auth request on the next attempt.
    RetryWithNewPayload(Vec<u8>),
}

// ---------------------------------------------------------------------------
// AuthHandler
// ---------------------------------------------------------------------------

/// Protocol-agnostic authentication hook.
///
/// The transport calls these methods at the right moments during connection
/// setup — the implementor does not need to know about Noise or WS framing.
///
/// ## How auth is integrated into the connection lifecycle
///
/// 1. After the Noise XX handshake completes the transport calls
///    [`auth_payload`]. If it returns `Some(bytes)`, those bytes are sent
///    to the server as the first [`Frame`] (kind = `Message`, `id > 0`).
/// 2. The transport waits for the server's response frame.
/// 3. The response payload bytes are forwarded to [`process_auth_response`].
/// 4. Based on the returned [`AuthOutcome`] the transport either moves to
///    `Online`, stops reconnecting, or tries the next endpoint.
///
/// If [`auth_payload`] returns `None`, auth is skipped and the connection
/// goes directly to `Online` after the Noise handshake.
#[async_trait]
pub trait AuthHandler: Send + Sync {
    /// Returns the serialized auth request payload to send to the server.
    ///
    /// Called once after each successful Noise handshake.
    /// Return `None` to skip authentication (anonymous connection).
    async fn auth_payload(&self) -> Option<Vec<u8>>;

    /// Process the server's raw response to the auth request.
    ///
    /// The implementor deserialises `response` according to the application
    /// protocol and returns the appropriate [`AuthOutcome`].
    async fn process_auth_response(&self, response: &[u8]) -> AuthOutcome;

    /// Called when the server sends a session-expired signal during normal
    /// operation (after the connection was previously `Online`).
    ///
    /// The implementor should clear any cached session tokens so the next
    /// reconnect attempt starts with fresh credentials.
    async fn on_session_expired(&self);
}

// ---------------------------------------------------------------------------
// NoAuthHandler
// ---------------------------------------------------------------------------

/// A no-op [`AuthHandler`] for services that do not require authentication.
pub struct NoAuth;

#[async_trait]
impl AuthHandler for NoAuth {
    async fn auth_payload(&self) -> Option<Vec<u8>> {
        None
    }

    async fn process_auth_response(&self, _response: &[u8]) -> AuthOutcome {
        AuthOutcome::Success
    }

    async fn on_session_expired(&self) {}
}

// ---------------------------------------------------------------------------
// Error integration
// ---------------------------------------------------------------------------

impl From<AuthOutcome> for Result<(), TransportError> {
    fn from(outcome: AuthOutcome) -> Self {
        match outcome {
            AuthOutcome::Success => Ok(()),
            AuthOutcome::Unauthorized => Err(TransportError::Unauthorized),
            AuthOutcome::Failed => Err(TransportError::AuthTimeout),
            AuthOutcome::RetryWithNewPayload(_) => Err(TransportError::AuthTimeout),
        }
    }
}
