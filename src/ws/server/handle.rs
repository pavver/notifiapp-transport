use crate::{
    error::TransportError,
    frame::{Frame, FrameKind},
    scheduler::MessagePriority,
    state::ConnectionState,
};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

pub enum SessionCmd {
    Send {
        frame: Frame,
        priority: MessagePriority,
    },
    Close,
}

/// Cheap, cloneable handle for pushing frames to a connected client.
#[derive(Clone)]
pub struct ServerSessionHandle {
    pub id: Uuid,
    pub(crate) cmd_tx: std::sync::Arc<mpsc::Sender<SessionCmd>>,
    pub(crate) state_rx: watch::Receiver<ConnectionState>,
}

impl ServerSessionHandle {
    /// Send a response frame matched by the client's request `id`.
    pub fn respond(&self, id: u32, data: Vec<u8>) -> Result<(), TransportError> {
        self.cmd_tx
            .try_send(SessionCmd::Send {
                frame: Frame {
                    id,
                    kind: FrameKind::Message,
                    data,
                },
                priority: MessagePriority::Normal,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => TransportError::BufferFull,
                _ => TransportError::ChannelError,
            })
    }

    /// Push a server-initiated event frame (`id = 0`).
    pub fn push_event(
        &self,
        data: Vec<u8>,
        priority: MessagePriority,
    ) -> Result<(), TransportError> {
        self.cmd_tx
            .try_send(SessionCmd::Send {
                frame: Frame {
                    id: 0,
                    kind: FrameKind::Event,
                    data,
                },
                priority,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => TransportError::BufferFull,
                _ => TransportError::ChannelError,
            })
    }

    /// Gracefully close the session.
    pub fn close(&self) {
        self.cmd_tx.try_send(SessionCmd::Close).ok();
    }

    pub fn state(&self) -> ConnectionState {
        self.state_rx.borrow().clone()
    }

    pub fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.state_rx.clone()
    }

    /// Returns `true` if the underlying WS connection is still alive.
    pub fn is_alive(&self) -> bool {
        matches!(self.state(), ConnectionState::Online)
    }
}

impl Drop for ServerSessionHandle {
    fn drop(&mut self) {
        if std::sync::Arc::strong_count(&self.cmd_tx) == 1 {
            self.cmd_tx.try_send(SessionCmd::Close).ok();
        }
    }
}
