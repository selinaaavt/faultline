//! A from-scratch Raft consensus implementation -- the harder system under test.
//!
//! Raft keeps a replicated log consistent across nodes despite crashes,
//! partitions, and message loss, by electing a leader per term and replicating
//! log entries through it. The safety properties are subtle and exactly the kind
//! of thing that breaks under adversarial timing -- which is why running Raft
//! inside the deterministic simulator is a real test, not a toy.
//!
//! This implements the core of the Raft paper (Ongaro & Ousterhout):
//!   - leader election with randomized election timeouts and terms,
//!   - RequestVote with the up-to-date-log restriction,
//!   - AppendEntries with the log-matching consistency check,
//!   - commit-index advance once an entry is on a majority.
//!
//! It is driven entirely by the simulator's logical clock and seeded RNG: a node
//! never reads a real clock, it's ticked by scheduled timer events. That's what
//! makes any safety violation reproducible from the seed.

use crate::net::NodeId;

/// A single replicated log entry: a client command tagged with the term in
/// which the leader created it. The term is what makes the log-matching and
/// election-safety proofs work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u64,
    pub command: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Raft RPCs (and their replies), exchanged over the simulated network.
#[derive(Debug, Clone)]
pub enum RaftMsg {
    RequestVote {
        term: u64,
        candidate: NodeId,
        last_log_index: usize,
        last_log_term: u64,
    },
    RequestVoteReply {
        term: u64,
        granted: bool,
    },
    AppendEntries {
        term: u64,
        leader: NodeId,
        prev_log_index: usize,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: usize,
    },
    AppendEntriesReply {
        term: u64,
        success: bool,
        // On success, the follower's new last-log index, so the leader can
        // advance matchIndex without re-deriving it.
        match_index: usize,
    },
}

pub struct RaftNode {
    pub id: NodeId,
    pub n_nodes: usize,

    // --- Persistent state (survives restart in real Raft; we keep it in-memory
    //     but never wipe it on a warm restart) ---
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    /// Log is 1-indexed in the paper; we use a Vec with a synthetic index-0
    /// sentinel term of 0 so `log[i]` is entry i and index 0 means "empty".
    pub log: Vec<LogEntry>,

    // --- Volatile state ---
    pub role: Role,
    pub commit_index: usize,
    pub leader_id: Option<NodeId>,
    pub crashed: bool,

    /// Randomized election-timeout countdown (in ticks). Decrements each tick;
    /// when it hits 0 a follower/candidate starts an election. Reset to
    /// `base_timeout` whenever the node hears from a current leader or grants a
    /// vote. Randomization (per-node `base_timeout`) is what breaks election
    /// livelock -- if every node used the same timeout they'd all time out
    /// together, split the vote, and repeat.
    pub election_timer: u64,
    /// This node's randomized election timeout, set once by the runner from the
    /// seeded RNG. The timer resets to this on contact from a leader.
    pub base_timeout: u64,

    /// Deliberately reintroduce the (now-fixed) match-index safety bug, for
    /// demonstrating that the simulator catches it. When true, a follower reports
    /// its raw log length instead of the index this RPC confirmed -- the exact
    /// bug that let a leader commit on a false majority. Off in normal operation;
    /// a "mutation test" of our own checker.
    pub inject_match_index_bug: bool,

    // --- Candidate state ---
    votes_received: Vec<bool>,

    // --- Leader state (per follower) ---
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

impl RaftNode {
    pub fn new(id: NodeId, n_nodes: usize) -> Self {
        RaftNode {
            id,
            n_nodes,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            role: Role::Follower,
            commit_index: 0,
            leader_id: None,
            crashed: false,
            election_timer: 0,
            base_timeout: 10,
            inject_match_index_bug: false,
            votes_received: vec![false; n_nodes],
            next_index: vec![1; n_nodes],
            match_index: vec![0; n_nodes],
        }
    }

    fn majority(&self) -> usize {
        self.n_nodes / 2 + 1
    }

    /// Set this node's randomized election timeout (from the runner's seeded
    /// RNG) and arm the countdown. Called once at setup per node.
    pub fn set_base_timeout(&mut self, ticks: u64) {
        self.base_timeout = ticks.max(1);
        self.election_timer = self.base_timeout;
    }

    /// Decrement the election timer; returns true if it just expired (the runner
    /// should then call `start_election`). Leaders don't run an election timer.
    pub fn tick_election_timer(&mut self) -> bool {
        if self.role == Role::Leader || self.crashed {
            return false;
        }
        if self.election_timer > 0 {
            self.election_timer -= 1;
        }
        self.election_timer == 0
    }

    /// Last log index (0 = empty log) and its term.
    fn last_log(&self) -> (usize, u64) {
        match self.log.last() {
            Some(e) => (self.log.len(), e.term),
            None => (0, 0),
        }
    }

    fn log_term_at(&self, index: usize) -> u64 {
        if index == 0 || index > self.log.len() {
            0
        } else {
            self.log[index - 1].term
        }
    }

    /// If we see a term newer than ours, step down to follower and adopt it.
    /// Returns true if we stepped down. This rule is pervasive in Raft.
    fn observe_term(&mut self, term: u64) -> bool {
        if term > self.current_term {
            self.current_term = term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.leader_id = None;
            true
        } else {
            false
        }
    }

    // ---- Timer-driven transitions (called by the runner on scheduled ticks) ----

    /// Election timeout fired: become a candidate for a new term and return the
    /// RequestVote messages to broadcast (empty if crashed or already leader).
    pub fn start_election(&mut self) -> Vec<(NodeId, RaftMsg)> {
        if self.crashed || self.role == Role::Leader {
            return Vec::new();
        }
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.votes_received = vec![false; self.n_nodes];
        self.votes_received[self.id] = true; // vote for self
        self.election_timer = self.base_timeout; // re-arm; retry if this election stalls

        let (last_log_index, last_log_term) = self.last_log();
        (0..self.n_nodes)
            .filter(|&peer| peer != self.id)
            .map(|peer| {
                (
                    peer,
                    RaftMsg::RequestVote {
                        term: self.current_term,
                        candidate: self.id,
                        last_log_index,
                        last_log_term,
                    },
                )
            })
            .collect()
    }

    /// Heartbeat timer fired on the leader: send AppendEntries (possibly with new
    /// entries) to every follower. Empty if not leader / crashed.
    pub fn heartbeat(&mut self) -> Vec<(NodeId, RaftMsg)> {
        if self.crashed || self.role != Role::Leader {
            return Vec::new();
        }
        (0..self.n_nodes)
            .filter(|&peer| peer != self.id)
            .map(|peer| (peer, self.build_append_for(peer)))
            .collect()
    }

    fn build_append_for(&self, peer: NodeId) -> RaftMsg {
        let next = self.next_index[peer];
        let prev_log_index = next - 1;
        let prev_log_term = self.log_term_at(prev_log_index);
        let entries = if next <= self.log.len() {
            self.log[next - 1..].to_vec()
        } else {
            Vec::new()
        };
        RaftMsg::AppendEntries {
            term: self.current_term,
            leader: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        }
    }

    /// Client submits a command. Only the leader accepts it; it appends to its
    /// own log and lets the next heartbeat replicate. Returns true if accepted.
    pub fn client_command(&mut self, command: u64) -> bool {
        if self.crashed || self.role != Role::Leader {
            return false;
        }
        self.log.push(LogEntry { term: self.current_term, command });
        true
    }

    // ---- Message handling ----

    /// Handle an incoming RPC; return replies/outgoing messages to send.
    pub fn handle(&mut self, from: NodeId, msg: RaftMsg) -> Vec<(NodeId, RaftMsg)> {
        if self.crashed {
            return Vec::new();
        }
        match msg {
            RaftMsg::RequestVote { term, candidate, last_log_index, last_log_term } => {
                self.observe_term(term);
                let mut granted = false;
                if term == self.current_term {
                    let can_vote =
                        self.voted_for.is_none() || self.voted_for == Some(candidate);
                    // Up-to-date restriction: candidate's log must be at least as
                    // current as ours, or we must not grant the vote.
                    let (my_idx, my_term) = self.last_log();
                    let up_to_date = last_log_term > my_term
                        || (last_log_term == my_term && last_log_index >= my_idx);
                    if can_vote && up_to_date {
                        granted = true;
                        self.voted_for = Some(candidate);
                        // Granting a vote resets our timer: we've endorsed a
                        // candidate, so don't immediately start a rival election.
                        self.election_timer = self.base_timeout;
                    }
                }
                vec![(from, RaftMsg::RequestVoteReply { term: self.current_term, granted })]
            }

            RaftMsg::RequestVoteReply { term, granted } => {
                self.observe_term(term);
                if self.role == Role::Candidate && term == self.current_term && granted {
                    self.votes_received[from] = true;
                    let count = self.votes_received.iter().filter(|&&v| v).count();
                    if count >= self.majority() {
                        self.become_leader();
                    }
                }
                Vec::new()
            }

            RaftMsg::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.observe_term(term);
                if term < self.current_term {
                    // Stale leader; reject.
                    return vec![(
                        from,
                        RaftMsg::AppendEntriesReply {
                            term: self.current_term,
                            success: false,
                            match_index: 0,
                        },
                    )];
                }
                // Valid leader for our term: become/stay follower, note the
                // leader, and reset the election timer -- hearing from a live
                // leader is exactly what should suppress a follower's election.
                self.role = Role::Follower;
                self.leader_id = Some(leader);
                self.election_timer = self.base_timeout;

                // Log-matching check: our log must contain prev_log_index with
                // prev_log_term, or we reject so the leader backs up.
                let consistent = prev_log_index == 0
                    || (prev_log_index <= self.log.len()
                        && self.log_term_at(prev_log_index) == prev_log_term);
                if !consistent {
                    return vec![(
                        from,
                        RaftMsg::AppendEntriesReply {
                            term: self.current_term,
                            success: false,
                            match_index: 0,
                        },
                    )];
                }

                // Append/overwrite entries after prev_log_index. On any term
                // conflict, truncate our log from that point (Raft's rule).
                let mut idx = prev_log_index; // 0-based position to write next
                for entry in entries {
                    idx += 1;
                    if idx <= self.log.len() {
                        if self.log[idx - 1].term != entry.term {
                            self.log.truncate(idx - 1); // drop conflicting suffix
                            self.log.push(entry);
                        }
                        // else: already have a matching entry; skip.
                    } else {
                        self.log.push(entry);
                    }
                }
                // `idx` is now the index of the last entry this RPC confirmed
                // matches the leader (prev_log_index + entries.len()).
                let last_new_index = idx;

                // Advance commit index -- but cap it by the last entry THIS RPC
                // confirmed, not our whole log length. Capping by log.len() is a
                // real Raft safety bug: a heartbeat (no entries) carrying a high
                // leader_commit would commit stale entries still in our log from
                // a previous leader, which a later AppendEntries then truncates
                // -- committing an entry that gets overwritten. This is the
                // `min(leaderCommit, index of last new entry)` rule from the paper.
                if leader_commit > self.commit_index {
                    self.commit_index = leader_commit.min(last_new_index);
                }

                vec![(
                    from,
                    RaftMsg::AppendEntriesReply {
                        // Report the match index relative to what THIS RPC
                        // confirmed (prev_log_index + entries.len()), NOT our raw
                        // log length. Reporting log.len() is a real safety bug: a
                        // follower with a longer, *divergent* log from an old term
                        // would tell the leader it had replicated entries it never
                        // received, letting the leader count it toward a majority
                        // and commit an entry that follower doesn't actually hold
                        // -- producing two conflicting committed entries at one
                        // index (the seed-1111 violation this simulator caught).
                        term: self.current_term,
                        success: true,
                        // Correct: report only what this RPC confirmed. The
                        // injected bug reverts to the raw log length, which is
                        // what the simulator is designed to catch.
                        match_index: if self.inject_match_index_bug {
                            self.log.len()
                        } else {
                            last_new_index
                        },
                    },
                )]
            }

            RaftMsg::AppendEntriesReply { term, success, match_index } => {
                self.observe_term(term);
                if self.role != Role::Leader || term != self.current_term {
                    return Vec::new();
                }
                if success {
                    self.match_index[from] = match_index;
                    self.next_index[from] = match_index + 1;
                    self.advance_commit();
                } else {
                    // Follower rejected: back up nextIndex and retry next beat.
                    if self.next_index[from] > 1 {
                        self.next_index[from] -= 1;
                    }
                }
                Vec::new()
            }
        }
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        let last = self.log.len();
        for p in 0..self.n_nodes {
            self.next_index[p] = last + 1;
            self.match_index[p] = 0;
        }
        self.match_index[self.id] = last;
    }

    /// Leader commit rule: an entry is committed once it's on a majority AND is
    /// from the current term. Advance commit_index to the highest such index.
    fn advance_commit(&mut self) {
        // Keep the leader's own matchIndex equal to its log length, so the
        // majority count is correct. Without this the leader counted "self"
        // (via the old `count = 1`) toward an index it might not actually hold
        // after a log truncation -- committing an entry no majority truly has.
        // That was a genuine safety bug the simulator surfaced at seed 1111.
        self.match_index[self.id] = self.log.len();

        for idx in (self.commit_index + 1..=self.log.len()).rev() {
            // Only commit entries from the current term (the Raft safety rule
            // that prevents committing a stale entry via count alone).
            if self.log_term_at(idx) != self.current_term {
                continue;
            }
            // Count every node (including self) whose matchIndex reaches idx.
            let mut count = 0;
            for p in 0..self.n_nodes {
                if self.match_index[p] >= idx {
                    count += 1;
                }
            }
            if count >= self.majority() {
                self.commit_index = idx;
                break;
            }
        }
    }

    pub fn crash(&mut self) {
        self.crashed = true;
    }

    /// Warm restart: comes back as a follower but KEEPS its persistent state
    /// (term, votedFor, log) -- as real Raft requires for safety.
    pub fn restart(&mut self) {
        self.crashed = false;
        self.role = Role::Follower;
        self.leader_id = None;
    }

    /// The committed prefix of the log (what a client is guaranteed).
    pub fn committed_log(&self) -> &[LogEntry] {
        &self.log[..self.commit_index.min(self.log.len())]
    }
}
