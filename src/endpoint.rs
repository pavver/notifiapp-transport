use parking_lot::RwLock;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// EndpointPriority
// ---------------------------------------------------------------------------

/// Priority of an endpoint — lower value = tried first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EndpointPriority {
    /// Local network (LAN, loopback) — highest priority.
    Local = 0,
    /// Remote / internet access — tried if local fails.
    Remote = 1,
    /// Dev / staging — only used for manual override or explicit testing.
    Dev = 2,
}

// ---------------------------------------------------------------------------
// EndpointHandle
// ---------------------------------------------------------------------------

/// Opaque handle returned by [`EndpointRegistry::add`].
/// Used to switch to, or remove, a specific endpoint later.
#[derive(Debug, Clone)]
pub struct EndpointHandle(Arc<EndpointHandleInner>);

#[derive(Debug)]
struct EndpointHandleInner {
    id: Uuid,
}

impl EndpointHandle {
    pub(crate) fn new() -> Self {
        Self(Arc::new(EndpointHandleInner { id: Uuid::new_v4() }))
    }

    pub(crate) fn id(&self) -> Uuid {
        self.0.id
    }
}

// ---------------------------------------------------------------------------
// Internal endpoint data
// ---------------------------------------------------------------------------

pub(crate) struct EndpointEntry {
    pub id: Uuid,
    pub url: Url,
    pub priority: EndpointPriority,
}

// ---------------------------------------------------------------------------
// EndpointRegistry
// ---------------------------------------------------------------------------

/// Thread-safe registry of connection endpoints sorted by priority.
///
/// Handles the logic for choosing which endpoint to try next, including:
/// - Forced manual override (`switch_to`)
/// - Promotion of the last successful endpoint within the same tier
#[derive(Clone)]
pub struct EndpointRegistry {
    entries: Arc<RwLock<Vec<EndpointEntry>>>,
    forced: Arc<RwLock<Option<Uuid>>>,
    pub(crate) last_connected: Arc<RwLock<Option<Uuid>>>,
}

impl EndpointRegistry {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(Vec::new())),
            forced: Arc::new(RwLock::new(None)),
            last_connected: Arc::new(RwLock::new(None)),
        }
    }

    /// Register a new endpoint. Returns a handle for later reference.
    pub fn add(
        &self,
        url_str: &str,
        priority: EndpointPriority,
    ) -> Result<EndpointHandle, crate::error::TransportError> {
        let mut url = Url::parse(url_str)
            .map_err(|_| crate::error::TransportError::InvalidUrl(url_str.to_string()))?;
        // Default path to "/ws" when omitted.
        if url.path() == "/" || url.path().is_empty() {
            url.set_path("/ws");
        }
        let handle = EndpointHandle::new();
        let entry = EndpointEntry {
            id: handle.id(),
            url,
            priority,
        };
        {
            let mut entries = self.entries.write();
            entries.push(entry);
            entries.sort_by_key(|e| e.priority);
        }
        Ok(handle)
    }

    /// Remove a previously registered endpoint.
    pub fn remove(&self, handle: &EndpointHandle) {
        let id = handle.id();
        self.entries.write().retain(|e| e.id != id);
        let mut forced = self.forced.write();
        if *forced == Some(id) {
            *forced = None;
        }
    }

    /// Force the next connection attempt to use this specific endpoint,
    /// bypassing the normal priority order.
    pub fn switch_to(&self, handle: &EndpointHandle) {
        *self.forced.write() = Some(handle.id());
    }

    /// Clear the forced override and resume automatic priority selection.
    pub fn clear_forced(&self) {
        *self.forced.write() = None;
    }

    /// Returns `true` if no endpoints are registered.
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    /// Ordered list of (id, url_string) pairs for the connection loop.
    ///
    /// If a forced endpoint is set, only that entry is returned.
    /// Otherwise, entries are sorted by priority. Within the same priority
    /// tier, the last successful endpoint is promoted to the front to avoid
    /// unnecessary re-handshakes after transient drops.
    pub fn ordered(&self) -> Vec<(Uuid, String)> {
        let forced = *self.forced.read();
        let last = *self.last_connected.read();
        let entries = self.entries.read();

        if let Some(forced_id) = forced {
            return entries
                .iter()
                .filter(|e| e.id == forced_id)
                .map(|e| (e.id, e.url.as_str().to_string()))
                .collect();
        }

        let mut result: Vec<(Uuid, String)> = Vec::with_capacity(entries.len());

        // Promote last_connected within its tier.
        if let Some(last_id) = last
            && let Some(e) = entries.iter().find(|e| e.id == last_id)
        {
            let top_priority = entries.first().map(|e| e.priority);
            if Some(e.priority) != top_priority {
                result.push((e.id, e.url.as_str().to_string()));
            }
        }

        for e in entries.iter() {
            if !result.iter().any(|(id, _)| *id == e.id) {
                result.push((e.id, e.url.as_str().to_string()));
            }
        }
        result
    }
}

impl Default for EndpointRegistry {
    fn default() -> Self {
        Self::new()
    }
}
