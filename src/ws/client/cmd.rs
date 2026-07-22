use crate::{frame::Frame, scheduler::MessagePriority};

pub enum ClientCmd {
    Send {
        frame: Frame,
        priority: MessagePriority,
    },
    EndpointChanged,
    Reconnect,
    WakeUp,
}
