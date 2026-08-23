use async_trait::async_trait;
use std::sync::Arc;

use crate::{error::TransportError, scheduler::MessagePriority, state::ConnectionState};

/// Common trait for all transport implementations.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait Transport: Send + Sync {
    /// Send a request and await the response.
    async fn request(
        &self,
        data: Vec<u8>,
        priority: MessagePriority,
    ) -> Result<Vec<u8>, TransportError>;

    /// Send a fire-and-forget event.
    fn send_event(&self, data: Vec<u8>, priority: MessagePriority) -> Result<(), TransportError>;

    /// Register a callback to handle incoming server push events.
    fn on_event(&self, handler: Arc<dyn Fn(Vec<u8>) + Send + Sync + 'static>);

    /// Subscribe to connection state changes.
    fn subscribe_state(&self) -> tokio::sync::watch::Receiver<ConnectionState>;

    /// Set the endpoint URL.
    fn set_endpoint(&self, url_str: &str) -> Result<(), TransportError>;

    /// Clear current endpoint.
    fn clear_endpoint(&self);

    /// Retrieve the current endpoint URL if set.
    fn endpoint(&self) -> Option<String>;

    /// Shutdown the transport, terminating any background tasks.
    fn shutdown(&self);
}
