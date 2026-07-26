pub mod config;
pub mod handle;
pub mod session;

use crate::{error::TransportError, frame::Frame, state::ConnectionState};
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Message};
use uuid::Uuid;

#[cfg(feature = "crypto")]
use crate::crypto::NoiseSession;

use std::sync::Arc;

pub use config::WsServerConfig;
pub use handle::ServerSessionHandle;
use handle::SessionCmd;
use session::run_server_session;

/// Perform the transport handshake on an incoming raw WS stream and return a
/// session handle + inbox receiver + background task future.
pub async fn accept_ws_session<S>(
    mut ws: WebSocketStream<S>,
    config: Arc<WsServerConfig>,
) -> Result<
    (
        ServerSessionHandle,
        mpsc::UnboundedReceiver<Frame>,
        impl std::future::Future<Output = ()> + Send,
    ),
    TransportError,
>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    // --- Protocol version handshake ---
    let hello_msg = match ws.next().await {
        Some(Ok(Message::Text(t))) => t.to_string(),
        _ => return Err(TransportError::ProtocolHandshakeFailed),
    };

    let expected_prefix = format!("{} ", config.protocol_name);
    if !hello_msg.starts_with(&expected_prefix) {
        let _ = ws
            .send(Message::Text(
                format!("PROTOCOL_REJECTED {}", config.protocol_version).into(),
            ))
            .await;
        return Err(TransportError::ProtocolHandshakeFailed);
    }

    let client_version = hello_msg
        .trim_start_matches(expected_prefix.as_str())
        .trim();
    if client_version != config.protocol_version {
        let _ = ws
            .send(Message::Text(
                format!("PROTOCOL_REJECTED {}", config.protocol_version).into(),
            ))
            .await;
        return Err(TransportError::VersionMismatch {
            client: client_version.to_string(),
            server: config.protocol_version.clone(),
        });
    }

    ws.send(Message::Text("PROTOCOL_ACCEPTED".into()))
        .await
        .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

    // --- Noise XX handshake (server responder) ---
    #[cfg(feature = "crypto")]
    let noise = {
        let mut noise = NoiseSession::server(&config.noise_private_key)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;
        let mut h_buf = vec![0u8; 65535];

        // <- E (initiator → responder)
        let data = match ws.next().await {
            Some(Ok(Message::Binary(d))) => d,
            _ => return Err(TransportError::NoiseHandshakeFailed("step 1: no E".into())),
        };
        noise
            .read_message(&data, &mut h_buf)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;

        // -> E, EE, S, ES
        let n = noise
            .write_message(&[], &mut h_buf)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;
        ws.send(Message::Binary(h_buf[..n].to_vec().into()))
            .await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        // <- S, SE
        let data = match ws.next().await {
            Some(Ok(Message::Binary(d))) => d,
            _ => {
                return Err(TransportError::NoiseHandshakeFailed(
                    "step 3: no S,SE".into(),
                ));
            }
        };
        noise
            .read_message(&data, &mut h_buf)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;

        if !noise.is_finished() {
            return Err(TransportError::NoiseHandshakeFailed(
                "handshake not complete".into(),
            ));
        }
        noise
    };

    // --- Build handles ---
    let session_id = Uuid::new_v4();
    let (state_tx, state_rx) = watch::channel(ConnectionState::Online);
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<SessionCmd>();
    let (inbox_tx, inbox_rx) = mpsc::unbounded_channel::<Frame>();

    let handle = ServerSessionHandle {
        id: session_id,
        cmd_tx: std::sync::Arc::new(cmd_tx),
        state_rx,
    };

    let max_bytes = config.max_payload_bytes;
    let heartbeat_interval = config.heartbeat_interval;
    let heartbeat_timeout = config.heartbeat_timeout;

    let task = async move {
        run_server_session(
            ws,
            #[cfg(feature = "crypto")]
            noise,
            cmd_rx,
            inbox_tx,
            state_tx,
            max_bytes,
            heartbeat_interval,
            heartbeat_timeout,
        )
        .await;
    };

    Ok((handle, inbox_rx, task))
}
