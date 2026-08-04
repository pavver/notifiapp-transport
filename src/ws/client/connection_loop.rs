use super::{WsClient, cmd::ClientCmd};
use crate::{
    auth::AuthOutcome,
    error::TransportError,
    frame::{Frame, FrameAccumulator, decode_frame, encode_frame},
    scheduler::WfqScheduler,
    state::ConnectionState,
};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::time::{Duration, sleep, timeout};
use tokio_tungstenite::{WebSocketStream, connect_async, tungstenite::protocol::Message};

#[cfg(feature = "crypto")]
use crate::crypto::NoiseSession;

// Use the stream type returned by tokio-tungstenite connect_async
type ConnectStream = WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

impl WsClient {
    pub(crate) async fn connection_loop(
        self: Arc<Self>,
        mut cmd_rx: tokio::sync::mpsc::UnboundedReceiver<ClientCmd>,
    ) {
        let mut reconnect_attempt: u32 = 0;
        let mut ever_connected = false;

        loop {
            if self.cancel.is_cancelled() {
                return;
            }
            // Retrieve current endpoint
            let url = {
                let u = self.url.read().clone();
                match u {
                    Some(val) => val,
                    None => {
                        // Sleep and wait for an EndpointChanged command
                        self.state_tx.send(ConnectionState::Disconnected).ok();
                        let mut cancelled = false;
                        loop {
                            tokio::select! {
                                _ = self.cancel.cancelled() => {
                                    cancelled = true;
                                    break;
                                }
                                Some(cmd) = cmd_rx.recv() => {
                                    if let ClientCmd::EndpointChanged = cmd {
                                        break;
                                    }
                                }
                            }
                        }
                        if cancelled {
                            println!("mock: connection_loop exiting!");
                            return;
                        }
                        continue;
                    }
                }
            };

            // Calculate backoff
            let delay = self.config.backoff.next_delay(reconnect_attempt);

            if delay > Duration::from_secs(0) {
                if ever_connected {
                    self.state_tx
                        .send(ConnectionState::Reconnecting {
                            attempt: reconnect_attempt,
                            delay,
                        })
                        .ok();
                }

                // Sleep or wake up on commands
                tokio::select! {
                    _ = self.cancel.cancelled() => {
                        return;
                    }
                    _ = sleep(delay) => {}
                    Some(cmd) = cmd_rx.recv() => {
                        match cmd {
                            ClientCmd::Reconnect => {
                                reconnect_attempt = 0;
                            }
                            ClientCmd::EndpointChanged => {
                                reconnect_attempt = 0;
                            }
                            _ => {}
                        }
                        continue;
                    }
                }
            }

            self.state_tx.send(ConnectionState::Connecting).ok();

            // --- Open WS connection ---
            let mut ws = match connect_async(url.as_str()).await {
                Ok((ws, _)) => ws,
                Err(e) => {
                    self.state_tx
                        .send(ConnectionState::Error(e.to_string()))
                        .ok();
                    reconnect_attempt += 1;
                    continue;
                }
            };

            // --- Protocol version handshake ---
            if let Err(state) = self.handle_version_handshake(&mut ws, &mut cmd_rx).await {
                if let Some(s) = state {
                    self.state_tx.send(s.clone()).ok();
                    if matches!(s, ConnectionState::VersionMismatch { .. }) {
                        self.pending.retain(|_, _| false);
                        let mut cancelled = false;
                        loop {
                            tokio::select! {
                                _ = self.cancel.cancelled() => {
                                    cancelled = true;
                                    break;
                                }
                                Some(cmd) = cmd_rx.recv() => {
                                    if matches!(cmd, ClientCmd::WakeUp | ClientCmd::Reconnect | ClientCmd::EndpointChanged) {
                                        reconnect_attempt = 0;
                                        break;
                                    }
                                }
                            }
                        }
                        if cancelled {
                            return;
                        }
                        continue;
                    }
                }
                reconnect_attempt += 1;
                continue;
            }

            // --- Noise XX handshake ---
            #[cfg(feature = "crypto")]
            let mut noise = match self.handle_noise_handshake(&mut ws).await {
                Ok(n) => n,
                Err(_) => {
                    reconnect_attempt += 1;
                    continue;
                }
            };

            // --- Application-level auth ---
            match self
                .handle_auth(
                    &mut ws,
                    #[cfg(feature = "crypto")]
                    &mut noise,
                )
                .await
            {
                Ok(_) => {}
                Err(TransportError::Unauthorized) => {
                    // Unauthorized is terminal. Stop trying and wait for wake.
                    self.state_tx.send(ConnectionState::Unauthorized).ok();
                    self.pending.retain(|_, _| false);
                    let mut cancelled = false;
                    loop {
                        tokio::select! {
                            _ = self.cancel.cancelled() => {
                                cancelled = true;
                                break;
                            }
                            Some(cmd) = cmd_rx.recv() => {
                                if matches!(cmd, ClientCmd::WakeUp | ClientCmd::Reconnect | ClientCmd::EndpointChanged) {
                                    reconnect_attempt = 0;
                                    break;
                                }
                            }
                        }
                    }
                    if cancelled {
                        return;
                    }
                    continue;
                }
                Err(e) => {
                    self.state_tx
                        .send(ConnectionState::Error(e.to_string()))
                        .ok();
                    reconnect_attempt += 1;
                    continue;
                }
            }

            // --- Online ---
            reconnect_attempt = 0;
            ever_connected = true;
            self.state_tx.send(ConnectionState::Online).ok();

            // Run main message loop
            self.run_message_loop(
                &mut ws,
                #[cfg(feature = "crypto")]
                &mut noise,
                &mut cmd_rx,
            )
            .await;

            // Handle clean/unclean disconnect
            self.state_tx.send(ConnectionState::Disconnected).ok();
            self.pending.retain(|_, _| false);

            if !self.auto_reconnect() {
                // Wait indefinitely until user triggers reconnect
                let mut cancelled = false;
                loop {
                    tokio::select! {
                        _ = self.cancel.cancelled() => {
                            cancelled = true;
                            break;
                        }
                        Some(cmd) = cmd_rx.recv() => {
                            if matches!(cmd, ClientCmd::Reconnect | ClientCmd::EndpointChanged) {
                                break;
                            }
                        }
                    }
                }
                if cancelled {
                    return;
                }
            } else {
                reconnect_attempt = 1; // Start reconnect sequence
            }
        }
    }

    async fn handle_version_handshake(
        &self,
        ws: &mut ConnectStream,
        cmd_rx: &mut tokio::sync::mpsc::UnboundedReceiver<ClientCmd>,
    ) -> Result<(), Option<ConnectionState>> {
        self.state_tx.send(ConnectionState::Handshaking).ok();
        let hello = format!(
            "{} {}",
            self.config.protocol_name, self.config.protocol_version
        );
        if let Err(e) = ws.send(Message::Text(hello.into())).await {
            return Err(Some(ConnectionState::Error(e.to_string())));
        }
        match ws.next().await {
            Some(Ok(Message::Text(reply))) if reply.contains("PROTOCOL_ACCEPTED") => Ok(()),
            Some(Ok(Message::Text(reply))) if reply.contains("PROTOCOL_REJECTED") => {
                let server_ver = reply
                    .strip_prefix("PROTOCOL_REJECTED ")
                    .unwrap_or("unknown")
                    .trim()
                    .to_string();
                let state = ConnectionState::VersionMismatch {
                    client: self.config.protocol_version.clone(),
                    server: server_ver,
                };
                self.state_tx.send(state.clone()).ok();
                self.pending.retain(|_, _| false);

                // Version mismatch is terminal until configuration/endpoint changes.
                let mut cancelled = false;
                loop {
                    tokio::select! {
                        _ = self.cancel.cancelled() => {
                            cancelled = true;
                            break;
                        }
                        Some(cmd) = cmd_rx.recv() => {
                            if matches!(cmd, ClientCmd::EndpointChanged | ClientCmd::Reconnect) {
                                break;
                            }
                        }
                    }
                }
                if cancelled {
                    return Err(None);
                }
                Err(None)
            }
            Some(Err(e)) => Err(Some(ConnectionState::Error(e.to_string()))),
            None => Err(Some(ConnectionState::Error(
                "Connection closed during handshake".into(),
            ))),
            _ => Err(Some(ConnectionState::Error(
                "Invalid handshake reply".into(),
            ))),
        }
    }

    #[cfg(feature = "crypto")]
    async fn handle_noise_handshake(
        &self,
        ws: &mut ConnectStream,
    ) -> Result<NoiseSession, TransportError> {
        self.state_tx.send(ConnectionState::Authenticating).ok();
        let key = self.config.noise_server_key.as_deref();
        let mut noise = NoiseSession::client(key)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;
        let mut h_buf = vec![0u8; 65535];

        // -> E
        let n = noise
            .write_message(&[], &mut h_buf)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;
        ws.send(Message::Binary(h_buf[..n].to_vec().into()))
            .await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        // <- E, EE, S, ES
        match ws.next().await {
            Some(Ok(Message::Binary(data))) => {
                noise
                    .read_message(&data, &mut h_buf)
                    .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;
            }
            _ => return Err(TransportError::ConnectionClosed),
        }

        // -> S, SE
        let n = noise
            .write_message(&[], &mut h_buf)
            .map_err(|e| TransportError::NoiseHandshakeFailed(e.to_string()))?;
        ws.send(Message::Binary(h_buf[..n].to_vec().into()))
            .await
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        if !noise.is_finished() {
            return Err(TransportError::NoiseHandshakeFailed(
                "Handshake not finished".to_string(),
            ));
        }
        Ok(noise)
    }

    async fn handle_auth(
        &self,
        ws: &mut ConnectStream,
        #[cfg(feature = "crypto")] noise: &mut NoiseSession,
    ) -> Result<(), TransportError> {
        if let Some(auth_bytes) = self.auth.auth_payload().await {
            self.state_tx.send(ConnectionState::WaitingForAuth).ok();
            let auth_frame = Frame {
                id: 1,
                kind: crate::frame::FrameKind::Message,
                data: auth_bytes,
            };
            let encoded = encode_frame(&auth_frame)?;

            #[cfg(feature = "crypto")]
            let send_result = {
                match noise.encrypt_chunked(&encoded) {
                    Ok(chunks) => {
                        let mut ok = true;
                        for chunk in chunks {
                            if ws.send(Message::Binary(chunk.into())).await.is_err() {
                                ok = false;
                                break;
                            }
                        }
                        ok
                    }
                    Err(_) => false,
                }
            };
            #[cfg(not(feature = "crypto"))]
            let send_result = ws.send(Message::Binary(encoded.into())).await.is_ok();

            if !send_result {
                return Err(TransportError::ConnectionClosed);
            }

            // Await auth response with timeout.
            let auth_result = timeout(
                self.config.auth_timeout,
                Self::recv_one_frame(
                    ws,
                    #[cfg(feature = "crypto")]
                    noise,
                    self.config.max_payload_bytes,
                ),
            )
            .await;

            let response_data = match auth_result {
                Ok(Ok(frame)) if frame.id == 1 => {
                    println!(
                        "Transport: received auth frame data length: {}",
                        frame.data.len()
                    );
                    frame.data
                }
                x => {
                    println!("Transport: auth_result mismatch: {:?}", x);
                    return Err(TransportError::ConnectionClosed);
                }
            };

            let outcome = self.auth.process_auth_response(&response_data).await;
            println!("Transport: auth outcome: {:?}", outcome);
            match outcome {
                AuthOutcome::Success => {}
                AuthOutcome::Unauthorized => {
                    self.state_tx.send(ConnectionState::Unauthorized).ok();
                    return Err(TransportError::Unauthorized);
                }
                AuthOutcome::Failed => return Err(TransportError::ConnectionClosed),
                AuthOutcome::RetryWithNewPayload(_) => {
                    return Err(TransportError::ConnectionClosed);
                }
            }
        }
        Ok(())
    }

    async fn run_message_loop(
        &self,
        ws: &mut ConnectStream,
        #[cfg(feature = "crypto")] noise: &mut NoiseSession,
        cmd_rx: &mut tokio::sync::mpsc::UnboundedReceiver<ClientCmd>,
    ) {
        let mut accumulator = FrameAccumulator::new();
        let mut scheduler = WfqScheduler::<Frame>::new();
        let mut heartbeat_deadline = tokio::time::Instant::now() + self.config.heartbeat_interval;
        let mut waiting_for_pong = false;

        loop {
            let hb = tokio::time::sleep_until(heartbeat_deadline);
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    break;
                }
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Binary(data))) => {
                            heartbeat_deadline =
                                tokio::time::Instant::now() + self.config.heartbeat_interval;
                            waiting_for_pong = false;

                            #[cfg(feature = "crypto")]
                            let decrypted = {
                                let mut buf = vec![0u8; data.len()];
                                match noise.read_message(&data, &mut buf) {
                                    Ok(n) => { buf.truncate(n); buf }
                                    Err(_) => break,
                                }
                            };
                            #[cfg(not(feature = "crypto"))]
                            let decrypted = data.to_vec();

                            println!("client: feeding accumulator {} bytes", decrypted.len());
                            match accumulator.feed(&decrypted, self.config.max_payload_bytes) {
                                Ok(Some(frame_bytes)) => {
                                    println!("client: assembled frame of {} bytes", frame_bytes.len());
                                    match decode_frame(&frame_bytes) {
                                        Ok(frame) => {
                                            println!("client: dispatched frame id={}", frame.id);
                                            self.dispatch_frame(frame);
                                        }
                                        Err(e) => {
                                            println!("mock: Frame decode error: {}", e);
                                            break;
                                        }
                                    }
                                }
                                Ok(None) => {
                                    println!("client: frame incomplete");
                                }
                                Err(e) => {
                                    tracing::warn!("Frame accumulator error: {}", e);
                                    break;
                                }
                            }
                        }
                        _ => break,
                    }
                }

                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        ClientCmd::Send { frame, priority } => {
                            scheduler.enqueue(frame, priority);
                            let mut send_error = false;
                            #[allow(clippy::while_let_on_iterator)]
                            while let Some(frame) = scheduler.next() {
                                let encoded = match encode_frame(&frame) {
                                    Ok(b) => b,
                                    Err(e) => { println!("mock: encode_frame error: {:?}", e); send_error = true; break; }
                                };

                                #[cfg(feature = "crypto")]
                                let chunks = match noise.encrypt_chunked(&encoded) {
                                    Ok(c) => c,
                                    Err(e) => { println!("mock: encrypt error: {:?}", e); send_error = true; break; }
                                };
                                #[cfg(not(feature = "crypto"))]
                                let chunks = vec![encoded];

                                for chunk in chunks {
                                    if let Err(e) = ws.send(Message::Binary(chunk.into())).await {
                                        println!("mock: ws.send error: {:?}", e);
                                        send_error = true;
                                        break;
                                    }
                                }
                                if send_error { break; }

                                // Drain any commands that arrived while sending.
                                let mut disconnect = false;
                                while let Ok(queued) = cmd_rx.try_recv() {
                                    match queued {
                                        ClientCmd::Send { frame: f, priority: p } => {
                                            scheduler.enqueue(f, p);
                                        }
                                        ClientCmd::EndpointChanged | ClientCmd::Reconnect => {
                                            let _ = ws.close(None).await;
                                            disconnect = true;
                                        }
                                        ClientCmd::WakeUp => {}
                                    }
                                }
                                if disconnect || send_error { break; }
                            }
                            if send_error { break; }
                        }
                        ClientCmd::EndpointChanged | ClientCmd::Reconnect => {
                            let _ = ws.close(None).await;
                            break;
                        }
                        ClientCmd::WakeUp => {}
                    }
                }

                _ = hb => {
                    if waiting_for_pong {
                        let _ = ws.close(None).await;
                        break;
                    }
                    let ping = Frame { id: 0, kind: crate::frame::FrameKind::Ping, data: vec![] };
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
                    heartbeat_deadline =
                        tokio::time::Instant::now() + self.config.heartbeat_timeout;
                }
            }
        }
    }

    /// Read exactly one complete frame from the WS stream.
    /// Used during auth handshake before the main message loop starts.
    pub(crate) async fn recv_one_frame<S>(
        ws: &mut S,
        #[cfg(feature = "crypto")] noise: &mut NoiseSession,
        max_bytes: usize,
    ) -> Result<Frame, TransportError>
    where
        S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        let mut acc = FrameAccumulator::new();
        while let Some(Ok(Message::Binary(data))) = ws.next().await {
            #[cfg(feature = "crypto")]
            let decrypted = {
                let mut buf = vec![0u8; data.len()];
                let n = noise
                    .read_message(&data, &mut buf)
                    .map_err(|e| TransportError::NoiseDecryptFailed(e.to_string()))?;
                buf.truncate(n);
                buf
            };
            #[cfg(not(feature = "crypto"))]
            let decrypted = data.to_vec();

            if let Some(frame_bytes) = acc.feed(&decrypted, max_bytes)? {
                return decode_frame(&frame_bytes);
            }
        }
        Err(TransportError::ConnectionClosed)
    }
}
