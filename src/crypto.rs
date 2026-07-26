// Noise XX crypto session.
// Moved from notifiapp-protocol::common::crypto.
// Requires feature = "crypto".

use crate::error::CryptoError;
use snow::{Builder, HandshakeState, TransportState};

/// Noise_XX_25519_ChaChaPoly_BLAKE2b
/// XX = mutual authentication (both sides exchange static keys).
pub const NOISE_PATTERN: &str = "Noise_XX_25519_ChaChaPoly_BLAKE2b";

/// Maximum Noise plaintext chunk size (65535 - 16 bytes AEAD tag).
const MAX_CHUNK_PLAIN: usize = 65535 - 16;

pub struct NoiseSession {
    pub state: Option<NoiseState>,
}

pub enum NoiseState {
    Handshake(Box<HandshakeState>),
    Transport(TransportState),
}

impl NoiseSession {
    /// Create a client-side (initiator) session.
    /// `server_static_key` — optional pinned server public key.
    /// If `None`, any server key is accepted (trust-on-first-use).
    pub fn client(server_static_key: Option<&[u8]>) -> Result<Self, CryptoError> {
        let pattern = NOISE_PATTERN
            .parse()
            .map_err(|e: snow::Error| CryptoError::PatternParse(e.to_string()))?;
        let builder = Builder::new(pattern);
        let keypair = builder
            .generate_keypair()
            .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
        let mut builder = builder
            .local_private_key(&keypair.private)
            .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
        if let Some(sk) = server_static_key {
            builder = builder
                .remote_public_key(sk)
                .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
        }
        let handshake = builder
            .build_initiator()
            .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
        Ok(Self {
            state: Some(NoiseState::Handshake(Box::new(handshake))),
        })
    }

    /// Create a server-side (responder) session.
    /// `static_key` — server's own private key (32 bytes for X25519).
    pub fn server(static_key: &[u8]) -> Result<Self, CryptoError> {
        let pattern = NOISE_PATTERN
            .parse()
            .map_err(|e: snow::Error| CryptoError::PatternParse(e.to_string()))?;
        let builder = Builder::new(pattern);
        let handshake = builder
            .local_private_key(static_key)
            .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?
            .build_responder()
            .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
        Ok(Self {
            state: Some(NoiseState::Handshake(Box::new(handshake))),
        })
    }

    /// Process an incoming handshake or transport message.
    /// Returns the number of bytes written to `payload`.
    pub fn read_message(
        &mut self,
        packet: &[u8],
        payload: &mut [u8],
    ) -> Result<usize, CryptoError> {
        let state = self.state.take().ok_or(CryptoError::InvalidState)?;
        match state {
            NoiseState::Handshake(mut s) => {
                let len = s
                    .read_message(packet, payload)
                    .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
                if s.is_handshake_finished() {
                    let transport = s
                        .into_transport_mode()
                        .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
                    self.state = Some(NoiseState::Transport(transport));
                } else {
                    self.state = Some(NoiseState::Handshake(s));
                }
                Ok(len)
            }
            NoiseState::Transport(mut s) => {
                let len = s
                    .read_message(packet, payload)
                    .map_err(|e| CryptoError::DecryptFailed(e.to_string()))?;
                self.state = Some(NoiseState::Transport(s));
                Ok(len)
            }
        }
    }

    /// Produce an outgoing handshake or transport message.
    /// Returns the number of bytes written to `packet`.
    pub fn write_message(
        &mut self,
        payload: &[u8],
        packet: &mut [u8],
    ) -> Result<usize, CryptoError> {
        let state = self.state.take().ok_or(CryptoError::InvalidState)?;
        match state {
            NoiseState::Handshake(mut s) => {
                let len = s
                    .write_message(payload, packet)
                    .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
                if s.is_handshake_finished() {
                    let transport = s
                        .into_transport_mode()
                        .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
                    self.state = Some(NoiseState::Transport(transport));
                } else {
                    self.state = Some(NoiseState::Handshake(s));
                }
                Ok(len)
            }
            NoiseState::Transport(mut s) => {
                let len = s
                    .write_message(payload, packet)
                    .map_err(|e| CryptoError::EncryptFailed(e.to_string()))?;
                self.state = Some(NoiseState::Transport(s));
                Ok(len)
            }
        }
    }

    /// Returns `true` after the 3-step XX handshake is complete.
    pub fn is_finished(&self) -> bool {
        matches!(&self.state, Some(NoiseState::Transport(_)))
    }

    /// Encrypt `payload` into one or more chunks, each ≤ 65535 bytes.
    /// Each chunk should be sent as a separate WS binary message.
    pub fn encrypt_chunked(&mut self, payload: &[u8]) -> Result<Vec<Vec<u8>>, CryptoError> {
        let mut chunks = Vec::new();
        for chunk in payload.chunks(MAX_CHUNK_PLAIN) {
            let mut packet = vec![0u8; chunk.len() + 16];
            let len = self.write_message(chunk, &mut packet)?;
            packet.truncate(len);
            chunks.push(packet);
        }
        Ok(chunks)
    }

    /// Decrypt a sequence of chunks (one per WS message) back into a single buffer.
    pub fn decrypt_chunked(&mut self, packets: &[Vec<u8>]) -> Result<Vec<u8>, CryptoError> {
        let mut out = Vec::new();
        for packet in packets {
            let mut buf = vec![0u8; packet.len()];
            let len = self.read_message(packet, &mut buf)?;
            out.extend_from_slice(&buf[..len]);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Key generation helpers
// ---------------------------------------------------------------------------

/// Generate a fresh X25519 static key pair for use with Noise XX.
/// Returns `(private_key, public_key)`, each 32 bytes.
pub fn generate_keypair() -> Result<([u8; 32], [u8; 32]), CryptoError> {
    let pattern = NOISE_PATTERN
        .parse()
        .map_err(|e: snow::Error| CryptoError::PatternParse(e.to_string()))?;
    let builder = Builder::new(pattern);
    let kp = builder
        .generate_keypair()
        .map_err(|e| CryptoError::HandshakeFailed(e.to_string()))?;
    let private: [u8; 32] = kp
        .private
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength)?;
    let public: [u8; 32] = kp
        .public
        .try_into()
        .map_err(|_| CryptoError::InvalidKeyLength)?;
    Ok((private, public))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_small_payload() -> Result<(), CryptoError> {
        let (server_priv, _server_pub) = generate_keypair()?;
        let mut client = NoiseSession::client(None)?;
        let mut server = NoiseSession::server(&server_priv)?;
        perform_handshake(&mut client, &mut server)?;

        let plain = b"hello noise world";
        let chunks = client.encrypt_chunked(plain)?;
        let decrypted = server.decrypt_chunked(&chunks)?;
        assert_eq!(decrypted, plain);
        Ok(())
    }

    #[test]
    fn round_trip_large_payload() -> Result<(), CryptoError> {
        let (server_priv, _) = generate_keypair()?;
        let mut client = NoiseSession::client(None)?;
        let mut server = NoiseSession::server(&server_priv)?;
        perform_handshake(&mut client, &mut server)?;

        let plain = vec![0x42u8; 200_000]; // 200 KB — requires ≥4 chunks
        let chunks = client.encrypt_chunked(&plain)?;
        let decrypted = server.decrypt_chunked(&chunks)?;
        assert_eq!(decrypted, plain);
        Ok(())
    }

    fn perform_handshake(
        client: &mut NoiseSession,
        server: &mut NoiseSession,
    ) -> Result<(), CryptoError> {
        let mut buf = vec![0u8; 65535];
        // -> E
        let n = client.write_message(&[], &mut buf)?;
        let _ = server.read_message(&buf[..n], &mut buf.clone())?;
        // <- E, EE, S, ES
        let n = server.write_message(&[], &mut buf)?;
        let _ = client.read_message(&buf[..n], &mut buf.clone())?;
        // -> S, SE
        let n = client.write_message(&[], &mut buf)?;
        let _ = server.read_message(&buf[..n], &mut buf.clone())?;
        assert!(client.is_finished());
        assert!(server.is_finished());
        Ok(())
    }
}
