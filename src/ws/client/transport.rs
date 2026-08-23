use async_trait::async_trait;
use std::sync::Arc;

use crate::{
    error::TransportError, scheduler::MessagePriority, state::ConnectionState, transport::Transport,
};

use super::WsClient;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl Transport for WsClient {
    async fn request(
        &self,
        data: Vec<u8>,
        priority: MessagePriority,
    ) -> Result<Vec<u8>, TransportError> {
        self.request(data, priority).await
    }

    fn send_event(&self, data: Vec<u8>, priority: MessagePriority) -> Result<(), TransportError> {
        self.send_event(data, priority)
    }

    fn on_event(&self, handler: Arc<dyn Fn(Vec<u8>) + Send + Sync + 'static>) {
        *self.event_handler.write() = Some(handler);
    }

    fn subscribe_state(&self) -> tokio::sync::watch::Receiver<ConnectionState> {
        self.subscribe_state()
    }

    fn set_endpoint(&self, url_str: &str) -> Result<(), TransportError> {
        self.set_endpoint(url_str)
    }

    fn clear_endpoint(&self) {
        self.clear_endpoint()
    }

    fn endpoint(&self) -> Option<String> {
        self.endpoint()
    }

    fn shutdown(&self) {
        self.shutdown();
    }
}
