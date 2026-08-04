use super::backoff::{BackoffStrategy, ExponentialBackoff};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Timeouts and limits
// ---------------------------------------------------------------------------

/// Maximum time to wait for a server response to any single request.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum time to wait for the server's auth response.
pub const DEFAULT_AUTH_TIMEOUT: Duration = Duration::from_secs(15);
/// Ping sent after this interval of incoming silence.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// Connection considered dead if no Pong arrives within this window after Ping.
pub const DEFAULT_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default maximum payload size (4 MiB).
pub const DEFAULT_MAX_PAYLOAD: usize = 4 * 1024 * 1024;
/// Default maximum number of commands/messages buffered for sending.
pub const DEFAULT_BUFFER_SIZE: usize = 100;

/// Configuration for `WsClient`.
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
    /// Strategy for calculating delays between reconnection attempts.
    pub backoff: Arc<dyn BackoffStrategy>,
    /// Maximum number of commands/messages buffered for sending.
    pub buffer_size: usize,
}

impl WsClientConfig {
    pub fn new(protocol_name: impl Into<String>, protocol_version: impl Into<String>) -> Self {
        let protocol_name = protocol_name.into();
        let protocol_version = protocol_version.into();
        crate::utils::validate_protocol_string(&protocol_name);
        crate::utils::validate_protocol_string(&protocol_version);
        Self {
            protocol_name,
            protocol_version,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_timeout: DEFAULT_HEARTBEAT_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            auth_timeout: DEFAULT_AUTH_TIMEOUT,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD,
            #[cfg(feature = "crypto")]
            noise_server_key: None,
            backoff: Arc::new(ExponentialBackoff::new(
                Duration::from_secs(2),
                2.0,
                Duration::from_secs(30),
            )),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }

    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    pub fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_auth_timeout(mut self, timeout: Duration) -> Self {
        self.auth_timeout = timeout;
        self
    }

    pub fn with_max_payload_bytes(mut self, max_payload_bytes: usize) -> Self {
        self.max_payload_bytes = max_payload_bytes;
        self
    }

    #[cfg(feature = "crypto")]
    pub fn with_noise_server_key(mut self, key: Vec<u8>) -> Self {
        self.noise_server_key = Some(key);
        self
    }

    pub fn with_backoff(mut self, backoff: Arc<dyn BackoffStrategy>) -> Self {
        self.backoff = backoff;
        self
    }

    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }
}
