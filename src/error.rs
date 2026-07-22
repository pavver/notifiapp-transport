use thiserror::Error;

/// Top-level error type for all transport operations.
#[derive(Debug, Error, Clone)]
pub enum TransportError {
    // --- Connection errors ---
    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("endpoint list is empty")]
    NoEndpoints,

    #[error("invalid endpoint URL: {0}")]
    InvalidUrl(String),

    // --- Crypto / Noise errors ---
    #[cfg(feature = "crypto")]
    #[error("noise handshake failed: {0}")]
    NoiseHandshakeFailed(String),

    #[cfg(feature = "crypto")]
    #[error("noise encryption failed: {0}")]
    NoiseEncryptFailed(String),

    #[cfg(feature = "crypto")]
    #[error("noise decryption failed: {0}")]
    NoiseDecryptFailed(String),

    // --- Auth errors ---
    #[error("authentication failed: unauthorized")]
    Unauthorized,

    #[error("authentication timed out")]
    AuthTimeout,

    #[error("session expired")]
    SessionExpired,

    // --- Protocol errors ---
    #[error("version mismatch: client={client}, server={server}")]
    VersionMismatch { client: String, server: String },

    #[error("protocol handshake failed")]
    ProtocolHandshakeFailed,

    #[error("payload exceeds maximum size of {max} bytes")]
    PayloadTooLarge { max: usize },

    #[error("frame encoding failed: {0}")]
    EncodeError(String),

    #[error("frame decoding failed: {0}")]
    DecodeError(String),

    // --- Request/response errors ---
    #[error("request timed out")]
    RequestTimeout,

    #[error("internal channel error")]
    ChannelError,

    // --- HTTP/SSE errors (feature = "http") ---
    #[cfg(feature = "http")]
    #[error("http error: {0}")]
    HttpError(String),

    #[cfg(feature = "http")]
    #[error("sse stream closed")]
    SseStreamClosed,
}

impl From<postcard::Error> for TransportError {
    fn from(e: postcard::Error) -> Self {
        Self::DecodeError(e.to_string())
    }
}
