use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Frame — the lowest-level transport message unit
// ---------------------------------------------------------------------------

/// Internal binary frame used by the transport layer.
/// Serialized with postcard, optionally encrypted with Noise.
///
/// Wire format (per WS message):
///   [4-byte LE u32 total_payload_length] [postcard(Frame) bytes]
///
/// For large payloads the Noise layer splits encrypted output into multiple
/// WS binary messages; the receiver accumulates them using the length prefix.
#[derive(Debug, Serialize, Deserialize)]
pub struct Frame {
    /// Request/response correlation ID.
    /// - `0` → server-push event (no response expected).
    /// - `>0` → request; server responds with the same `id`.
    pub id: u32,

    /// Frame kind — controls routing in the connection loop.
    pub kind: FrameKind,

    /// Opaque application-level payload (postcard / JSON / binary).
    pub data: Vec<u8>,
}

/// Discriminator for the frame routing logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameKind {
    /// Application request or response (matched by `id`).
    Message,
    /// Server-pushed event (broadcast to event callbacks, `id = 0`).
    Event,
    /// Transport-level heartbeat ping.
    Ping,
    /// Transport-level heartbeat pong (response to Ping).
    Pong,
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Encode a frame into a length-prefixed byte buffer ready for Noise/WS.
pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, crate::error::TransportError> {
    let payload = postcard::to_stdvec(frame)
        .map_err(|e| crate::error::TransportError::EncodeError(e.to_string()))?;
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Attempt to decode a frame from a raw decrypted byte slice.
pub fn decode_frame(data: &[u8]) -> Result<Frame, crate::error::TransportError> {
    postcard::from_bytes(data).map_err(|e| crate::error::TransportError::DecodeError(e.to_string()))
}

// ---------------------------------------------------------------------------
// Reassembly helper
// ---------------------------------------------------------------------------

/// Stateful accumulator for reassembling multi-WS-message frames.
///
/// The transport layer sends large payloads across multiple WS binary
/// messages. This struct accumulates bytes until a complete frame arrives.
#[derive(Default)]
pub struct FrameAccumulator {
    expected_len: usize,
    buffer: Vec<u8>,
}

impl FrameAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a decrypted chunk. Returns `Some(frame_bytes)` when a complete
    /// frame has been assembled, or `None` if more data is needed.
    ///
    /// Returns an error if the declared length exceeds `max_bytes`.
    pub fn feed(
        &mut self,
        chunk: &[u8],
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, crate::error::TransportError> {
        if self.expected_len == 0 {
            if chunk.len() < 4 {
                return Err(crate::error::TransportError::DecodeError(
                    "frame too short for length prefix".into(),
                ));
            }
            let len_bytes: [u8; 4] = chunk[0..4]
                .try_into()
                .map_err(|_| crate::error::TransportError::DecodeError("length prefix".into()))?;
            self.expected_len = u32::from_le_bytes(len_bytes) as usize;
            if self.expected_len > max_bytes {
                return Err(crate::error::TransportError::PayloadTooLarge { max: max_bytes });
            }
            self.buffer.clear();
            self.buffer.extend_from_slice(&chunk[4..]);
        } else {
            self.buffer.extend_from_slice(chunk);
            if self.buffer.len() > max_bytes {
                return Err(crate::error::TransportError::PayloadTooLarge { max: max_bytes });
            }
        }

        if self.buffer.len() >= self.expected_len {
            let complete = self.buffer[..self.expected_len].to_vec();
            self.expected_len = 0;
            self.buffer.clear();
            return Ok(Some(complete));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_encode_decode_round_trip() {
        let frame = Frame {
            id: 42,
            kind: FrameKind::Message,
            data: b"hello world".to_vec(),
        };

        let encoded = encode_frame(&frame).unwrap();
        // The first 4 bytes are the length of the payload
        let len_bytes: [u8; 4] = encoded[0..4].try_into().unwrap();
        let payload_len = u32::from_le_bytes(len_bytes) as usize;
        assert_eq!(payload_len, encoded.len() - 4);

        // Decode from raw bytes (excluding the 4-byte length prefix as FrameAccumulator does)
        let decoded = decode_frame(&encoded[4..]).unwrap();
        assert_eq!(decoded.id, frame.id);
        assert_eq!(decoded.kind, frame.kind);
        assert_eq!(decoded.data, frame.data);
    }

    #[test]
    fn test_frame_accumulator_single_chunk() {
        let frame = Frame {
            id: 1,
            kind: FrameKind::Event,
            data: vec![0xAA; 100],
        };
        let encoded = encode_frame(&frame).unwrap();

        let mut accumulator = FrameAccumulator::new();
        let res = accumulator.feed(&encoded, 1024).unwrap();
        assert!(res.is_some());
        let decoded = decode_frame(&res.unwrap()).unwrap();
        assert_eq!(decoded.id, 1);
        assert_eq!(decoded.data.len(), 100);
    }

    #[test]
    fn test_frame_accumulator_multiple_chunks() {
        let frame = Frame {
            id: 2,
            kind: FrameKind::Message,
            data: vec![0xBB; 200],
        };
        let encoded = encode_frame(&frame).unwrap();

        let mut accumulator = FrameAccumulator::new();

        // Split the encoded buffer into 3 chunks
        let chunk1 = &encoded[..50];
        let chunk2 = &encoded[50..150];
        let chunk3 = &encoded[150..];

        assert_eq!(accumulator.feed(chunk1, 1024).unwrap(), None);
        assert_eq!(accumulator.feed(chunk2, 1024).unwrap(), None);

        let final_res = accumulator.feed(chunk3, 1024).unwrap();
        assert!(final_res.is_some());
        let decoded = decode_frame(&final_res.unwrap()).unwrap();
        assert_eq!(decoded.id, 2);
        assert_eq!(decoded.data.len(), 200);
    }

    #[test]
    fn test_frame_accumulator_payload_too_large() {
        let mut accumulator = FrameAccumulator::new();

        // Declare a length of 5000 bytes (longer than max_bytes = 1000)
        let mut invalid_chunk = vec![0u8; 10];
        let bad_len = 5000u32;
        invalid_chunk[0..4].copy_from_slice(&bad_len.to_le_bytes());

        let res = accumulator.feed(&invalid_chunk, 1000);
        assert!(res.is_err());
    }

    #[test]
    fn test_frame_accumulator_too_short() {
        let mut accumulator = FrameAccumulator::new();
        let res = accumulator.feed(&[0, 1], 1000);
        assert!(res.is_err());
    }
}
