use serde::{Deserialize, Serialize};

use std::time::Duration;

/// Lifecycle state of a transport connection.
/// Mirrors the states in notifiapp-protocol but is protocol-agnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionState {
    /// No active connection. No endpoints set, or all endpoints failed.
    Disconnected,
    /// Resolving DNS / opening TCP / TLS.
    Connecting,
    /// Automatically attempting to restore a previously active connection.
    Reconnecting {
        /// Number of failed reconnect attempts in the current sequence.
        attempt: u32,
        /// How long to wait before the next connection attempt.
        delay: Duration,
    },
    /// WS connection open; exchanging protocol version string.
    Handshaking,
    /// Performing Noise XX key exchange.
    Authenticating,
    /// Connection established; waiting for auth confirmation from server.
    /// This state is used when an `AuthHandler` is provided.
    WaitingForAuth,
    /// Fully established and ready for normal message exchange.
    Online,
    /// Server rejected credentials. Will NOT retry without new credentials.
    Unauthorized,
    /// Server and client speak different protocol versions.
    VersionMismatch { client: String, server: String },
    /// Transient network / protocol error. Will retry with backoff.
    Error(String),
}

impl ConnectionState {
    /// Returns `true` if the connection can accept outgoing messages.
    pub fn is_online(&self) -> bool {
        matches!(self, Self::Online)
    }

    /// Returns `true` if the connection is in any transitional state.
    pub fn is_connecting(&self) -> bool {
        matches!(
            self,
            Self::Connecting
                | Self::Reconnecting { .. }
                | Self::Handshaking
                | Self::Authenticating
                | Self::WaitingForAuth
        )
    }

    /// Returns `true` if reconnect attempts should stop (terminal states).
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Unauthorized | Self::VersionMismatch { .. })
    }
}
