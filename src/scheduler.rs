use std::collections::VecDeque;

// ---------------------------------------------------------------------------
// MessagePriority
// ---------------------------------------------------------------------------

/// Priority level for outgoing messages.
///
/// Used by `WfqScheduler` to interleave messages fairly.
/// Weights (messages sent per scheduling round):
/// - `RealTime` → 8   (e.g., audio frames)
/// - `Normal`   → 4   (queries, control messages)
/// - `Stream`   → 2   (video, large data)
/// - `Bulk`     → 1   (file transfers, lowest urgency)
///
/// Each level gets at least 1 slot per round, so lower priorities are
/// never fully starved even on a saturated connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    /// Real-time, latency-sensitive (audio frames, urgent control).
    RealTime = 0,
    /// Normal priority (queries, subscriptions, events).
    Normal = 1,
    /// Streaming data (video frames).
    Stream = 2,
    /// Bulk transfers (file chunks, recordings).
    Bulk = 3,
}

impl MessagePriority {
    pub(crate) const COUNT: usize = 4;
}

// ---------------------------------------------------------------------------
// WfqScheduler
// ---------------------------------------------------------------------------

/// Credit-based Weighted Fair Queuing scheduler.
///
/// Maintains one queue per priority level. On each `next()` call it dequeues
/// from the highest-priority non-empty queue that still has credits.
/// When all credits are exhausted, budgets reset — starting a new round.
pub struct WfqScheduler<T> {
    queues: [VecDeque<T>; MessagePriority::COUNT],
    credits: [usize; MessagePriority::COUNT],
}

/// Credits per scheduling round, indexed by `MessagePriority as usize`.
const WFQ_WEIGHTS: [usize; MessagePriority::COUNT] = [8, 4, 2, 1];

impl<T> WfqScheduler<T> {
    pub fn new() -> Self {
        Self {
            queues: Default::default(),
            credits: WFQ_WEIGHTS,
        }
    }

    /// Enqueue an item at the given priority level.
    pub fn enqueue(&mut self, item: T, priority: MessagePriority) {
        self.queues[priority as usize].push_back(item);
    }

    /// Dequeue the next item according to WFQ credit rules.
    /// Returns `None` if all queues are empty.
    pub fn pop(&mut self) -> Option<T> {
        // First pass: respect credit budgets.
        for i in 0..MessagePriority::COUNT {
            if self.credits[i] > 0 && !self.queues[i].is_empty() {
                self.credits[i] -= 1;
                return self.queues[i].pop_front();
            }
        }
        // All credits exhausted — reset budgets and try again.
        self.credits = WFQ_WEIGHTS;
        for i in 0..MessagePriority::COUNT {
            if !self.queues[i].is_empty() {
                self.credits[i] = self.credits[i].saturating_sub(1);
                return self.queues[i].pop_front();
            }
        }
        None // all queues empty
    }
}

impl<T> Iterator for WfqScheduler<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.pop()
    }
}

impl<T> WfqScheduler<T> {
    /// Returns `true` if all queues are empty.
    pub fn is_empty(&self) -> bool {
        self.queues.iter().all(|q| q.is_empty())
    }

    /// Total number of pending items across all queues.
    pub fn len(&self) -> usize {
        self.queues.iter().map(|q| q.len()).sum()
    }
}

impl<T> Default for WfqScheduler<T> {
    fn default() -> Self {
        Self::new()
    }
}
