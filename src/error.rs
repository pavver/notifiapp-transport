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

    #[error("buffer is full, cannot enqueue frame")]
    BufferFull,

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

/// Cryptographic error type for Noise protocol implementation.
#[cfg(feature = "crypto")]
#[derive(Debug, Error, Clone)]
pub enum CryptoError {
    #[error("noise pattern parse error: {0}")]
    PatternParse(String),
    #[error("noise handshake failed: {0}")]
    HandshakeFailed(String),
    #[error("noise encryption failed: {0}")]
    EncryptFailed(String),
    #[error("noise decryption failed: {0}")]
    DecryptFailed(String),
    #[error("invalid noise state")]
    InvalidState,
    #[error("unexpected key length")]
    InvalidKeyLength,
}

#[cfg(feature = "crypto")]
impl From<CryptoError> for TransportError {
    fn from(e: CryptoError) -> Self {
        match e {
            CryptoError::HandshakeFailed(s) => Self::NoiseHandshakeFailed(s),
            CryptoError::EncryptFailed(s) => Self::NoiseEncryptFailed(s),
            CryptoError::DecryptFailed(s) => Self::NoiseDecryptFailed(s),
            other => Self::ConnectionFailed(other.to_string()),
        }
    }
}
