use crate::{
    frame::{Frame, FrameAccumulator, FrameKind, decode_frame, encode_frame},
    scheduler::MessagePriority,
    scheduler::WfqScheduler,
    state::ConnectionState,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio::time::Duration;
use tokio_tungstenite::tungstenite::protocol::Message;

#[cfg(feature = "crypto")]
use crate::crypto::NoiseSession;

use super::handle::SessionCmd;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_server_session<S, E>(
    mut ws: S,
    #[cfg(feature = "crypto")] mut noise: NoiseSession,
    mut cmd_rx: mpsc::Receiver<SessionCmd>,
    inbox_tx: mpsc::Sender<Frame>,
    state_tx: watch::Sender<ConnectionState>,
    max_bytes: usize,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
) where
    S: futures_util::Stream<Item = Result<Message, E>>
        + futures_util::Sink<Message, Error = E>
        + Unpin
        + Send,
    E: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static,
{
    let mut accumulator = FrameAccumulator::new();
    let mut scheduler = WfqScheduler::<Frame>::new();
    let mut heartbeat_deadline = tokio::time::Instant::now() + heartbeat_interval;
    let mut waiting_for_pong = false;

    loop {
        let hb = tokio::time::sleep_until(heartbeat_deadline);
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Binary(data))) => {
                        heartbeat_deadline = tokio::time::Instant::now() + heartbeat_interval;
                        waiting_for_pong = false;

                        #[cfg(feature = "crypto")]
                        let decrypted = {
                            let mut buf = vec![0u8; data.len()];
                            match noise.read_message(&data, &mut buf) {
                                Ok(n) => { buf.truncate(n); buf }
                                Err(e) => {
                                    tracing::warn!("Server session noise decrypt error: {}", e);
                                    break;
                                }
                            }
                        };
                        #[cfg(not(feature = "crypto"))]
                        let decrypted = data.to_vec();

                        match accumulator.feed(&decrypted, max_bytes) {
                            Ok(Some(frame_bytes)) => {
                                match decode_frame(&frame_bytes) {
                                    Ok(frame) => {
                                        match frame.kind {
                                            FrameKind::Ping => {
                                                scheduler.enqueue(
                                                    Frame { id: 0, kind: FrameKind::Pong, data: vec![] },
                                                    MessagePriority::RealTime,
                                                );
                                            }
                                            FrameKind::Pong => { /* timer already reset */ }
                                            _ => {
                                                if inbox_tx.send(frame).await.is_err() {
                                                    break; // reader dropped
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("Server session frame decode error: {}", e);
                                        break;
                                    }
                                }
                            }
                            Ok(None) => {} // accumulating more chunks
                            Err(e) => {
                                tracing::warn!("Server session accumulator: {}", e);
                                break;
                            }
                        }
                    }
                    _ => break,
                }
            }

            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    SessionCmd::Send { frame, priority } => {
                        scheduler.enqueue(frame, priority);
                        let mut error = false;
                        #[allow(clippy::while_let_on_iterator)]
                        while let Some(frame) = scheduler.next() {
                            let encoded = match encode_frame(&frame) {
                                Ok(b) => b,
                                Err(_) => { error = true; break; }
                            };
                            #[cfg(feature = "crypto")]
                            let chunks = match noise.encrypt_chunked(&encoded) {
                                Ok(c) => c,
                                Err(_) => { error = true; break; }
                            };
                            #[cfg(not(feature = "crypto"))]
                            let chunks = vec![encoded];

                            for chunk in chunks {
                                if ws.send(Message::Binary(chunk.into())).await.is_err() {
                                    error = true;
                                    break;
                                }
                            }
                            if error { break; }
                        }
                        if error { break; }
                    }
                    SessionCmd::Close => break,
                }
            }

            _ = hb => {
                if waiting_for_pong {
                    tracing::debug!("Server session heartbeat timeout");
                    break;
                }
                let ping = Frame { id: 0, kind: FrameKind::Ping, data: vec![] };
                if let Ok(encoded) = encode_frame(&ping) {
                    #[cfg(feature = "crypto")]
                    if let Ok(chunks) = noise.encrypt_chunked(&encoded) {
                        for chunk in chunks {
                            if ws.send(Message::Binary(chunk.into())).await.is_err() {
                                break;
                            }
                        }
                    }
                    #[cfg(not(feature = "crypto"))]
                    let _ = ws.send(Message::Binary(encoded.into())).await;
                }
                waiting_for_pong = true;
                heartbeat_deadline = tokio::time::Instant::now() + heartbeat_timeout;
            }
        }
    }

    state_tx.send(ConnectionState::Disconnected).ok();
    let _ = ws.close().await;
}
