use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::error::TransportError;

// ---------------------------------------------------------------------------
// HttpClientConfig
// ---------------------------------------------------------------------------

/// Configuration for the HTTP+JSON fallback client.
pub struct HttpClientConfig {
    /// Base URL of the API server (e.g., `http://localhost:8080`).
    pub base_url: String,
    /// Request timeout for individual HTTP calls.
    pub request_timeout: Duration,
    /// Bearer token or API key for authentication headers.
    /// The value is sent as `Authorization: Bearer <token>`.
    pub auth_token: Option<String>,
}

impl HttpClientConfig {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            request_timeout: Duration::from_secs(30),
            auth_token: None,
        }
    }

    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }
}

// ---------------------------------------------------------------------------
// HttpClient
// ---------------------------------------------------------------------------

/// HTTP+JSON fallback transport client.
///
/// Provides two communication modes:
/// 1. **Request/response** (`post`) — POST JSON body, receive JSON response.
/// 2. **Subscriptions** (`subscribe_sse`) — GET SSE stream from the server,
///    with callbacks dispatched per event type.
///
/// This transport is intended as a **debug / fallback** alternative to the
/// primary WS+postcard transport. It does not support Noise encryption.
pub struct HttpClient {
    pub(crate) client: Client,
    pub(crate) base_url: parking_lot::RwLock<Option<Url>>,
    #[allow(dead_code)]
    pub(crate) config: Arc<HttpClientConfig>,
    #[allow(dead_code)]
    pub(crate) state_tx: tokio::sync::watch::Sender<crate::state::ConnectionState>,
    pub(crate) state_rx: tokio::sync::watch::Receiver<crate::state::ConnectionState>,
    pub(crate) cancel: CancellationToken,
    pub(crate) tasks: std::sync::Mutex<tokio::task::JoinSet<()>>,
}

impl HttpClient {
    pub fn new(config: HttpClientConfig) -> Result<Self, TransportError> {
        let base_url_opt = if config.base_url.is_empty() {
            None
        } else {
            Some(
                Url::parse(&config.base_url)
                    .map_err(|_| TransportError::InvalidUrl(config.base_url.clone()))?,
            )
        };

        let mut builder = Client::builder()
            .timeout(config.request_timeout)
            .connection_verbose(false);

        if let Some(token) = &config.auth_token {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", token)
                    .parse()
                    .map_err(|_| TransportError::ConnectionFailed("invalid auth token".into()))?,
            );
            builder = builder.default_headers(headers);
        }

        let client = builder
            .build()
            .map_err(|e| TransportError::ConnectionFailed(e.to_string()))?;

        let (state_tx, state_rx) =
            tokio::sync::watch::channel(crate::state::ConnectionState::Online);

        Ok(Self {
            client,
            base_url: parking_lot::RwLock::new(base_url_opt),
            config: Arc::new(config),
            state_tx,
            state_rx,
            cancel: CancellationToken::new(),
            tasks: std::sync::Mutex::new(tokio::task::JoinSet::new()),
        })
    }

    /// Send a JSON POST request and deserialise the JSON response.
    ///
    /// ```ignore
    /// let resp: MyResponse = client.post("/api/v1/cameras", &CreateCamera { name: "cam1" }).await?;
    /// ```
    pub async fn post<Req, Resp>(&self, path: &str, body: &Req) -> Result<Resp, TransportError>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let url = self.resolve(path)?;
        let response = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|e| TransportError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(TransportError::HttpError(format!(
                "HTTP {} {}",
                response.status().as_u16(),
                response.status().canonical_reason().unwrap_or("")
            )));
        }

        response
            .json::<Resp>()
            .await
            .map_err(|e| TransportError::DecodeError(e.to_string()))
    }

    /// Send a JSON GET request and deserialise the JSON response.
    pub async fn get<Resp>(&self, path: &str) -> Result<Resp, TransportError>
    where
        Resp: DeserializeOwned,
    {
        let url = self.resolve(path)?;
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| TransportError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(TransportError::HttpError(format!(
                "HTTP {}",
                response.status().as_u16()
            )));
        }

        response
            .json::<Resp>()
            .await
            .map_err(|e| TransportError::DecodeError(e.to_string()))
    }

    /// Send a JSON DELETE request.
    pub async fn delete(&self, path: &str) -> Result<(), TransportError> {
        let url = self.resolve(path)?;
        let response = self
            .client
            .delete(url)
            .send()
            .await
            .map_err(|e| TransportError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(TransportError::HttpError(format!(
                "HTTP {}",
                response.status().as_u16()
            )));
        }
        Ok(())
    }

    /// Open a Server-Sent Events subscription stream.
    ///
    /// The server must serve an SSE endpoint at `path` that emits
    /// `text/event-stream` events in the standard format:
    ///
    /// ```text
    /// event: camera_status_changed
    /// data: {"camera_id": "...", "status": "online"}
    ///
    /// ```
    ///
    /// Returns an [`SseSubscription`] handle. Dropping the handle cancels the
    /// SSE stream. The `handler` callback is called for every received event
    /// with `(event_type: String, data: String)`.
    ///
    /// ## Reconnection
    ///
    /// If the SSE stream drops, it is reconnected automatically with a 2-second
    /// delay. The reconnection loop runs until the `SseSubscription` is dropped.
    pub async fn subscribe_sse(
        &self,
        path: &str,
        handler: impl Fn(String, String) + Send + Sync + 'static,
    ) -> Result<SseSubscription, TransportError> {
        let url = self.resolve(path)?;
        let client = self.client.clone();
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let handler = Arc::new(handler);

        tokio::spawn(async move {
            sse_loop(client, url, handler, cancel_clone).await;
        });

        Ok(SseSubscription { _cancel: cancel })
    }

    pub(crate) fn resolve(&self, path: &str) -> Result<Url, TransportError> {
        let lock = self.base_url.read();
        let base_url = lock
            .as_ref()
            .ok_or_else(|| TransportError::InvalidUrl("No endpoint set".to_string()))?;
        base_url
            .join(path)
            .map_err(|_| TransportError::InvalidUrl(path.to_string()))
    }
}

impl Drop for HttpClient {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// ---------------------------------------------------------------------------
// SSE subscription handle
// ---------------------------------------------------------------------------

/// Handle for an active SSE subscription.
/// Dropping this value cancels the SSE stream.
pub struct SseSubscription {
    _cancel: CancellationToken,
}

impl Drop for SseSubscription {
    fn drop(&mut self) {
        self._cancel.cancel();
    }
}

// ---------------------------------------------------------------------------
// Internal SSE loop
// ---------------------------------------------------------------------------

pub(crate) async fn sse_loop(
    client: Client,
    url: Url,
    handler: Arc<dyn Fn(String, String) + Send + Sync>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            result = connect_sse(&client, url.clone(), &handler) => {
                if cancel.is_cancelled() {
                    return;
                }
                match result {
                    Ok(()) => {} // stream closed normally, reconnect
                    Err(e) => tracing::warn!("SSE stream error: {} — reconnecting in 2s", e),
                }
            }
        }
        // Brief pause before reconnect.
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }
}

async fn connect_sse(
    client: &Client,
    url: Url,
    handler: &Arc<dyn Fn(String, String) + Send + Sync>,
) -> Result<(), TransportError> {
    use futures_util::StreamExt;

    let response = client
        .get(url)
        .header("Accept", "text/event-stream")
        .header("Cache-Control", "no-cache")
        .send()
        .await
        .map_err(|e| TransportError::HttpError(e.to_string()))?;

    if !response.status().is_success() {
        return Err(TransportError::HttpError(format!(
            "SSE endpoint returned HTTP {}",
            response.status()
        )));
    }

    // Parse SSE stream manually from the raw byte stream.
    let mut stream = response.bytes_stream();
    let mut event_type = String::from("message");
    let mut data_lines: Vec<String> = Vec::new();
    let mut leftover = String::new();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| TransportError::HttpError(e.to_string()))?;
        let text = String::from_utf8_lossy(&chunk);
        leftover.push_str(&text);

        // Process complete lines (SSE uses \n\n to delimit events).
        while let Some(newline_pos) = leftover.find('\n') {
            let line: String = leftover.drain(..=newline_pos).collect();
            let line = line.trim_end_matches('\n').trim_end_matches('\r');

            if line.is_empty() {
                // Empty line = dispatch event if we have data.
                if !data_lines.is_empty() {
                    let data = data_lines.join("\n");
                    handler(event_type.clone(), data);
                    data_lines.clear();
                    event_type = String::from("message");
                }
            } else if let Some(value) = line.strip_prefix("event:") {
                event_type = value.trim().to_string();
            } else if let Some(value) = line.strip_prefix("data:") {
                data_lines.push(value.trim().to_string());
            }
            // Ignore "id:", "retry:", and comments (":").
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::Transport;

    #[test]
    fn test_http_client_endpoint_management() {
        let config = HttpClientConfig::new("");
        let client = HttpClient::new(config).unwrap();

        // Initially empty
        assert!(client.endpoint().is_none());
        assert!(client.resolve("/api").is_err());

        // Set endpoint
        assert!(client.set_endpoint("http://127.0.0.1:8080").is_ok());
        assert_eq!(
            client.endpoint(),
            Some("http://127.0.0.1:8080/".to_string())
        );
        assert_eq!(
            client.resolve("/api/v1").unwrap().to_string(),
            "http://127.0.0.1:8080/api/v1"
        );

        // Clear endpoint
        client.clear_endpoint();
        assert!(client.endpoint().is_none());
        assert!(client.resolve("/api").is_err());
    }
}
