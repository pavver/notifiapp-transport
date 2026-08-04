use std::time::Duration;

/// Configuration for accepting incoming WS sessions.
pub struct WsServerConfig {
    /// Protocol name this server expects from connecting clients.
    pub protocol_name: String,
    /// Server protocol version. Clients not matching receive PROTOCOL_REJECTED.
    pub protocol_version: String,
    /// Server Noise private key (32 bytes, X25519).
    /// Required when feature `crypto` is enabled.
    #[cfg(feature = "crypto")]
    pub noise_private_key: Vec<u8>,
    /// Maximum accepted payload size in bytes.
    pub max_payload_bytes: usize,
    /// How often the server sends heartbeat Pings.
    pub heartbeat_interval: Duration,
    /// How long to wait for a Pong before closing the connection.
    pub heartbeat_timeout: Duration,
    /// Maximum number of commands/messages buffered for sending.
    pub buffer_size: usize,
}

impl WsServerConfig {
    pub fn new(
        protocol_name: impl Into<String>,
        protocol_version: impl Into<String>,
        #[cfg(feature = "crypto")] noise_private_key: Vec<u8>,
    ) -> Self {
        let protocol_name = protocol_name.into();
        let protocol_version = protocol_version.into();
        crate::utils::validate_protocol_string(&protocol_name);
        crate::utils::validate_protocol_string(&protocol_version);
        Self {
            protocol_name,
            protocol_version,
            #[cfg(feature = "crypto")]
            noise_private_key,
            max_payload_bytes: 4 * 1024 * 1024,
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(10),
            buffer_size: 100,
        }
    }
}
