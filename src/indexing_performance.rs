use std::{collections::HashMap, ops::Deref, time::Duration};

use parking_lot::RwLock;
use thegraph_core::{DeploymentId, IndexerId, alloy::primitives::BlockNumber};
use tokio::{self, sync::mpsc, time::MissedTickBehavior};

use crate::network::NetworkService;

#[derive(Default)]
pub struct Snapshot {
    pub response: indexer_selection::Performance,
    pub latest_block: Option<BlockNumber>,
}

#[derive(Clone)]
pub struct IndexingPerformance {
    data: &'static DoubleBuffer,
    msgs: mpsc::UnboundedSender<Feedback>,
}

struct Feedback {
    indexer: IndexerId,
    deployment: DeploymentId,
    success: bool,
    latency_ms: u16,
    latest_block: Option<BlockNumber>,
}

impl IndexingPerformance {
    pub fn new(network: NetworkService) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let data: &'static DoubleBuffer = Box::leak(Box::default());
        Actor::spawn(data, rx, network);
        Self { data, msgs: tx }
    }

    pub fn latest(&self) -> impl Deref<Target = HashMap<(IndexerId, DeploymentId), Snapshot>> + '_ {
        loop {
            // This is guaranteed to only move forward in time, and is almost guaranteed to acquire
            // the lock "immediately". These guarantees come from the invariant that there is a
            // single writer, and it can only be in a few possible states.
            for unlocked in &self.data.0 {
                if let Some(data) = unlocked.try_read() {
                    return data;
                }
            }
        }
    }

    pub fn feedback(
        &self,
        indexer: IndexerId,
        deployment: DeploymentId,
        success: bool,
        latency_ms: u32,
        latest_block: Option<BlockNumber>,
    ) {
        self.msgs
            .send(Feedback {
                indexer,
                deployment,
                success,
                // The selection library accepts u16 milliseconds. Never wrap slow requests.
                latency_ms: u16::try_from(latency_ms).unwrap_or(u16::MAX),
                latest_block,
            })
            .unwrap();
    }
}

#[derive(Default)]
struct DoubleBuffer([RwLock<HashMap<(IndexerId, DeploymentId), Snapshot>>; 2]);

struct Actor {
    data: &'static DoubleBuffer,
}

impl Actor {
    fn spawn(
        data: &'static DoubleBuffer,
        mut messages: mpsc::UnboundedReceiver<Feedback>,
        mut network: NetworkService,
    ) {
        let mut actor = Self { data };
        let mut timer = tokio::time::interval(Duration::from_secs(1));
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        tokio::spawn(async move {
            let batch_limit = 32;
            let mut msg_buf = Vec::with_capacity(batch_limit);
            loop {
                tokio::select! {
                    _ = timer.tick() => actor.decay(),
                    _ = messages.recv_many(&mut msg_buf, batch_limit) => actor.handle_msgs(&mut msg_buf),
                    _ = network.changed() => actor.handle_network(&network),
                }
            }
        });
    }

    fn decay(&mut self) {
        for unlocked in &self.data.0 {
            for snapshot in unlocked.write().values_mut() {
                snapshot.response.decay();
            }
        }
    }

    fn handle_msgs(&mut self, msgs: &mut Vec<Feedback>) {
        for unlocked in &self.data.0 {
            let mut locked = unlocked.write();
            for Feedback {
                indexer,
                deployment,
                success,
                latency_ms,
                latest_block,
            } in msgs.drain(..)
            {
                let snapshot = locked.entry((indexer, deployment)).or_default();
                snapshot.response.feedback(success, latency_ms);
                snapshot.latest_block = match (snapshot.latest_block, latest_block) {
                    (None, block) => block,
                    (Some(a), Some(b)) if b > a => Some(b),
                    (Some(a), _) => Some(a),
                };
            }
        }
        debug_assert!(msgs.is_empty());
    }

    fn handle_network(&mut self, network: &NetworkService) {
        let progress = network.indexing_progress();
        for unlocked in &self.data.0 {
            let mut locked = unlocked.write();
            for (indexing, latest_block) in progress.iter() {
                let snapshot = locked
                    .entry((indexing.indexer, indexing.deployment))
                    .or_default();
                if *latest_block >= snapshot.latest_block.unwrap_or(0) {
                    snapshot.latest_block = Some(*latest_block);
                };
            }
        }
    }
}

#[cfg(test)]
mod latency_tests {
    use super::*;

    #[test]
    fn selection_feedback_saturates_instead_of_wrapping_slow_requests() {
        let data = Box::leak(Box::new(DoubleBuffer::default()));
        let (msgs, mut rx) = mpsc::unbounded_channel();
        let performance = IndexingPerformance { data, msgs };
        for (milliseconds, expected) in [
            (250, 250),
            (65_535, 65_535),
            (65_536, 65_535),
            (70_000, 65_535),
        ] {
            performance.feedback(
                thegraph_core::alloy::primitives::Address::ZERO.into(),
                [0; 32].into(),
                false,
                milliseconds,
                None,
            );
            assert_eq!(rx.try_recv().unwrap().latency_ms, expected);
        }
    }
}
