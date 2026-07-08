//! The system under test: a primary-backup replicated key-value store.
//!
//! This is a deliberately realistic design -- the kind a team might ship -- with
//! a subtle correctness flaw that appears only under specific fault
//! interleavings. That's the point: the deterministic simulator hunts for the
//! interleaving that breaks it and hands back a seed that reproduces it.
//!
//! Protocol (primary-backup / primary-copy replication):
//!   - One node is the primary; the rest are backups.
//!   - A client `Write` goes to the primary. The primary applies it locally,
//!     bumps a version, and asynchronously replicates to backups.
//!   - A client `Read` can be served by any node from its local copy (this is
//!     the performance-motivated choice that introduces the bug: backups can
//!     serve *stale* reads, and under a partition a client can even read from a
//!     node that missed writes -- a linearizability violation).
//!
//! Messages carry a monotonic `version` so backups apply updates in order and
//! ignore stale ones. The known weakness: nothing prevents a client from
//! reading a value that is older than a write already acknowledged elsewhere.

use crate::net::NodeId;

/// Application-level messages exchanged between nodes.
#[derive(Debug, Clone)]
pub enum Msg {
    /// Primary -> backup: replicate this key/value at this version.
    Replicate { key: String, value: u64, version: u64 },
    /// Backup -> primary: acknowledge replication up to this version.
    Ack { version: u64 },
}

/// A single node's state.
pub struct Node {
    pub id: NodeId,
    pub is_primary: bool,
    /// Local key-value store.
    pub data: std::collections::HashMap<String, u64>,
    /// Per-key version this node has applied.
    pub versions: std::collections::HashMap<String, u64>,
    /// Primary's global version counter (only meaningful on the primary).
    pub next_version: u64,
    /// Whether the node is currently crashed (ignores all input).
    pub crashed: bool,
}

impl Node {
    pub fn new(id: NodeId, is_primary: bool) -> Self {
        Node {
            id,
            is_primary,
            data: std::collections::HashMap::new(),
            versions: std::collections::HashMap::new(),
            next_version: 1,
            crashed: false,
        }
    }

    /// Local read (may be stale on a backup).
    pub fn read(&self, key: &str) -> Option<u64> {
        self.data.get(key).copied()
    }

    /// Apply a client write on the primary. Returns the replication messages to
    /// send to backups (the caller hands them to the network) and the version
    /// assigned. No-op (returns empty) if crashed or not primary.
    pub fn client_write(&mut self, key: &str, value: u64, backups: &[NodeId]) -> Vec<(NodeId, Msg)> {
        if self.crashed || !self.is_primary {
            return Vec::new();
        }
        let version = self.next_version;
        self.next_version += 1;
        self.data.insert(key.to_string(), value);
        self.versions.insert(key.to_string(), version);
        backups
            .iter()
            .map(|&b| (b, Msg::Replicate { key: key.to_string(), value, version }))
            .collect()
    }

    /// Handle an incoming message. Returns any reply messages to send.
    pub fn handle(&mut self, from: NodeId, msg: Msg) -> Vec<(NodeId, Msg)> {
        if self.crashed {
            return Vec::new();
        }
        match msg {
            Msg::Replicate { key, value, version } => {
                // Apply only if newer than what we have for this key (messages
                // can arrive out of order thanks to the network).
                let cur = self.versions.get(&key).copied().unwrap_or(0);
                if version > cur {
                    self.data.insert(key.clone(), value);
                    self.versions.insert(key, version);
                }
                vec![(from, Msg::Ack { version })]
            }
            Msg::Ack { .. } => Vec::new(), // primary could track acks; not needed here
        }
    }

    pub fn crash(&mut self) {
        self.crashed = true;
    }

    /// Restart: comes back up but keeps whatever state it had replicated (a
    /// warm restart). A cold restart (losing state) would be a harsher fault.
    pub fn restart(&mut self) {
        self.crashed = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primary_write_produces_replication() {
        let mut p = Node::new(0, true);
        let msgs = p.client_write("x", 42, &[1, 2]);
        assert_eq!(msgs.len(), 2, "one replicate message per backup");
        assert_eq!(p.read("x"), Some(42));
    }

    #[test]
    fn backup_applies_newer_ignores_older() {
        let mut b = Node::new(1, false);
        b.handle(0, Msg::Replicate { key: "x".into(), value: 1, version: 5 });
        assert_eq!(b.read("x"), Some(1));
        // Older version arrives late -> ignored.
        b.handle(0, Msg::Replicate { key: "x".into(), value: 999, version: 3 });
        assert_eq!(b.read("x"), Some(1), "stale replicate must be ignored");
        // Newer version -> applied.
        b.handle(0, Msg::Replicate { key: "x".into(), value: 7, version: 6 });
        assert_eq!(b.read("x"), Some(7));
    }

    #[test]
    fn crashed_node_ignores_everything() {
        let mut b = Node::new(1, false);
        b.crash();
        b.handle(0, Msg::Replicate { key: "x".into(), value: 1, version: 1 });
        assert_eq!(b.read("x"), None, "crashed node must not apply writes");
    }
}
