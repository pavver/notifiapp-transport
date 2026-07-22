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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wfq_scheduler_empty() {
        let mut scheduler: WfqScheduler<i32> = WfqScheduler::new();
        assert!(scheduler.is_empty());
        assert_eq!(scheduler.len(), 0);
        assert_eq!(scheduler.next(), None);
    }

    #[test]
    fn test_wfq_scheduler_fifo_same_priority() {
        let mut scheduler = WfqScheduler::new();
        scheduler.enqueue("msg1", MessagePriority::Normal);
        scheduler.enqueue("msg2", MessagePriority::Normal);

        assert_eq!(scheduler.len(), 2);
        assert_eq!(scheduler.next(), Some("msg1"));
        assert_eq!(scheduler.next(), Some("msg2"));
        assert_eq!(scheduler.next(), None);
    }

    #[test]
    fn test_wfq_scheduler_weights_distribution() {
        let mut scheduler = WfqScheduler::new();

        // Enqueue plenty of items in each queue to verify weights
        for i in 0..20 {
            scheduler.enqueue(format!("RT-{}", i), MessagePriority::RealTime);
            scheduler.enqueue(format!("N-{}", i), MessagePriority::Normal);
            scheduler.enqueue(format!("S-{}", i), MessagePriority::Stream);
            scheduler.enqueue(format!("B-{}", i), MessagePriority::Bulk);
        }

        // In one full round, we expect to pop:
        // 8 RealTime, 4 Normal, 2 Stream, 1 Bulk
        let mut popped = Vec::new();
        for _ in 0..15 {
            if let Some(item) = scheduler.next() {
                popped.push(item);
            }
        }

        let rt_count = popped.iter().filter(|x| x.starts_with("RT-")).count();
        let n_count = popped.iter().filter(|x| x.starts_with("N-")).count();
        let s_count = popped.iter().filter(|x| x.starts_with("S-")).count();
        let b_count = popped.iter().filter(|x| x.starts_with("B-")).count();

        assert_eq!(rt_count, 8);
        assert_eq!(n_count, 4);
        assert_eq!(s_count, 2);
        assert_eq!(b_count, 1);
    }

    #[test]
    fn test_wfq_scheduler_no_starvation() {
        let mut scheduler = WfqScheduler::new();

        // Only Stream and Bulk queues have items
        scheduler.enqueue("stream1", MessagePriority::Stream);
        scheduler.enqueue("bulk1", MessagePriority::Bulk);

        // Since RealTime and Normal are empty, credits for Stream and Bulk are consumed.
        assert_eq!(scheduler.next(), Some("stream1"));
        assert_eq!(scheduler.next(), Some("bulk1"));
        assert_eq!(scheduler.next(), None);
    }
}
