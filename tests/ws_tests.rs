use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::accept_async;

use notifiapp_transport::{
    AuthHandler, AuthOutcome, ConnectionState,
    ws::{WsClient, WsClientConfig, WsServerConfig, accept_ws_session},
};

// A mock authenticator that accepts a token "valid_token"
// and fails on any other credentials.
struct MockAuth {
    token: Arc<Mutex<Option<String>>>,
    expired_called: Arc<Mutex<bool>>,
}

#[async_trait::async_trait]
impl AuthHandler for MockAuth {
    async fn auth_payload(&self) -> Option<Vec<u8>> {
        self.token.lock().await.clone().map(|t| t.into_bytes())
    }

    async fn process_auth_response(&self, response: &[u8]) -> AuthOutcome {
        let resp_str = String::from_utf8_lossy(response);
        if resp_str == "OK" {
            AuthOutcome::Success
        } else if resp_str == "UNAUTHORIZED" {
            AuthOutcome::Unauthorized
        } else {
            AuthOutcome::Failed
        }
    }

    async fn on_session_expired(&self) {
        *self.expired_called.lock().await = true;
        self.token.lock().await.take();
    }
}

#[tokio::test]
async fn test_ws_auth_unauthorized_stops_reconnect() {
    // 1. Setup a simple TCP listener for our mock server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{}", addr);

    // Private key for Noise XX handshake (32 bytes)
    let private_key = vec![0u8; 32];
    #[cfg(feature = "crypto")]
    let public_key = {
        let noise = snow::Builder::new("Noise_XX_25519_ChaChaPoly_BLAKE2s".parse().unwrap())
            .local_private_key(&private_key)
            .unwrap()
            .build_responder()
            .unwrap();
        noise
            .get_remote_static()
            .map(|s| s.to_vec())
            .unwrap_or_else(|| vec![0u8; 32])
    };

    let server_config = Arc::new(WsServerConfig::new(
        "TEST_PROTOCOL",
        "1.0.0",
        #[cfg(feature = "crypto")]
        private_key,
    ));

    // 2. Spawn Mock Server
    let server_handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let ws_stream = accept_async(stream).await.unwrap();
            let server_cfg = server_config.clone();
            tokio::spawn(async move {
                match accept_ws_session(ws_stream, server_cfg).await {
                    Ok((handle, mut inbox, task)) => {
                        let _task_handle = tokio::spawn(task);

                        // Read the auth frame
                        if let Some(frame) = inbox.recv().await {
                            let token = String::from_utf8_lossy(&frame.data);
                            if token == "valid_token" {
                                handle.respond(frame.id, b"OK".to_vec()).unwrap();
                            } else {
                                handle.respond(frame.id, b"UNAUTHORIZED".to_vec()).unwrap();
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Err(e) => {
                        println!("Server accept_ws_session failed: {:?}", e);
                    }
                }
            });
        }
    });

    // 3. Client Setup (With invalid token initially)
    let client_token = Arc::new(Mutex::new(Some("invalid_token".to_string())));
    let expired_called = Arc::new(Mutex::new(false));

    let auth = Arc::new(MockAuth {
        token: client_token.clone(),
        expired_called: expired_called.clone(),
    });

    let mut client_config = WsClientConfig::new("TEST_PROTOCOL", "1.0.0");
    client_config.auth_timeout = Duration::from_secs(5);
    client_config.request_timeout = Duration::from_secs(5);
    #[cfg(feature = "crypto")]
    {
        client_config.noise_server_key = Some(public_key);
    }

    let client = WsClient::new(client_config, Some(auth.clone()));
    client.set_endpoint(&ws_url).unwrap();

    // 4. Wait for client to attempt connection and get Unauthorized
    let mut state_rx = client.subscribe_state();
    let mut got_unauthorized = false;

    // Timeout of 10 seconds for the test
    let timeout_fut = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(timeout_fut);

    loop {
        tokio::select! {
            _ = &mut timeout_fut => {
                println!("Test timed out!");
                break;
            }
            Ok(_) = state_rx.changed() => {
                let state = state_rx.borrow().clone();
                println!("Client connection state changed: {:?}", state);
                if matches!(state, ConnectionState::Unauthorized) {
                    got_unauthorized = true;
                    break;
                }
            }
        }
    }

    assert!(
        got_unauthorized,
        "Client should transition to Unauthorized state"
    );

    // 5. Ensure that the connection loop stays in Unauthorized state and DOES NOT reconnect
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        matches!(client.state(), ConnectionState::Unauthorized),
        "Client must stay in Unauthorized state and not try to reconnect automatically"
    );

    // Clean up
    server_handle.abort();
}

#[tokio::test]
async fn test_ws_reconnect_backoff_states() {
    // 1. Setup a simple TCP listener for our mock server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let ws_url = format!("ws://{}", addr);

    // Private key for Noise XX handshake (32 bytes)
    let private_key = vec![0u8; 32];
    #[cfg(feature = "crypto")]
    let public_key = {
        let noise = snow::Builder::new("Noise_XX_25519_ChaChaPoly_BLAKE2s".parse().unwrap())
            .local_private_key(&private_key)
            .unwrap()
            .build_responder()
            .unwrap();
        noise
            .get_remote_static()
            .map(|s| s.to_vec())
            .unwrap_or_else(|| vec![0u8; 32])
    };

    let server_config = Arc::new(WsServerConfig::new(
        "TEST_PROTOCOL",
        "1.0.0",
        #[cfg(feature = "crypto")]
        private_key,
    ));

    // 2. Spawn Mock Server that accepts, authenticates "valid_token", and then closes after a message
    let server_handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let ws_stream = accept_async(stream).await.unwrap();
            let server_cfg = server_config.clone();
            tokio::spawn(async move {
                if let Ok((handle, mut inbox, task)) =
                    accept_ws_session(ws_stream, server_cfg).await
                {
                    let _task_handle = tokio::spawn(task);

                    // Read the auth frame
                    if let Some(frame) = inbox.recv().await {
                        handle.respond(frame.id, b"OK".to_vec()).unwrap();
                    }

                    // Close session shortly to trigger a client-side disconnect
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    handle.close();
                }
            });
        }
    });

    // 3. Client Setup with valid token
    let client_token = Arc::new(Mutex::new(Some("valid_token".to_string())));
    let expired_called = Arc::new(Mutex::new(false));

    let auth = Arc::new(MockAuth {
        token: client_token.clone(),
        expired_called: expired_called.clone(),
    });

    let mut client_config = WsClientConfig::new("TEST_PROTOCOL", "1.0.0");
    client_config.auth_timeout = Duration::from_secs(5);
    client_config.request_timeout = Duration::from_secs(5);
    #[cfg(feature = "crypto")]
    {
        client_config.noise_server_key = Some(public_key);
    }

    let client = WsClient::new(client_config, Some(auth.clone()));
    client.set_endpoint(&ws_url).unwrap();

    let mut state_rx = client.subscribe_state();
    let mut got_reconnecting = false;

    let timeout_fut = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(timeout_fut);

    loop {
        tokio::select! {
            _ = &mut timeout_fut => {
                break;
            }
            Ok(_) = state_rx.changed() => {
                let state = state_rx.borrow().clone();
                println!("Reconnection test: State changed: {:?}", state);
                if let ConnectionState::Reconnecting { attempt, delay } = state
                    && attempt == 1 && delay == Duration::from_secs(2) {
                        got_reconnecting = true;
                        break;
                    }
            }
        }
    }

    assert!(
        got_reconnecting,
        "Client should transition to Reconnecting state with attempt 1 and 2s delay"
    );

    server_handle.abort();
}
