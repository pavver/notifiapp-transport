use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    error::TransportError, scheduler::MessagePriority, state::ConnectionState, transport::Transport,
};

use super::HttpClient;

#[async_trait]
impl Transport for HttpClient {
    async fn request(
        &self,
        data: Vec<u8>,
        _priority: MessagePriority,
    ) -> Result<Vec<u8>, TransportError> {
        let url = self.resolve("/request")?;
        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/octet-stream")
            .body(data)
            .send()
            .await
            .map_err(|e| TransportError::HttpError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(TransportError::HttpError(format!(
                "HTTP {}",
                response.status().as_u16()
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| TransportError::DecodeError(e.to_string()))?;
        Ok(bytes.to_vec())
    }

    fn send_event(&self, data: Vec<u8>, _priority: MessagePriority) -> Result<(), TransportError> {
        let client = self.client.clone();
        let url = self.resolve("/event")?;
        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.spawn(async move {
                let _ = client
                    .post(url)
                    .header("Content-Type", "application/octet-stream")
                    .body(data)
                    .send()
                    .await;
            });
        }
        Ok(())
    }

    fn on_event(&self, handler: Arc<dyn Fn(Vec<u8>) + Send + Sync + 'static>) {
        let client = self.client.clone();
        let url = match self.resolve("/events") {
            Ok(url) => url,
            Err(_) => return,
        };
        let cancel = self.cancel.clone();
        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.spawn(async move {
                let handler_wrapper = Arc::new(move |_event_type: String, data: String| {
                    handler(data.into_bytes());
                });
                super::client::sse_loop(client, url, handler_wrapper, cancel).await;
            });
        }
    }

    fn subscribe_state(&self) -> tokio::sync::watch::Receiver<ConnectionState> {
        self.state_rx.clone()
    }

    fn shutdown(&self) {
        self.cancel.cancel();
    }
}
