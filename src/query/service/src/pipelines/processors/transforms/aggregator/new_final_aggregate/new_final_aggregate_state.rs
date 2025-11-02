// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;

use super::split_partitioned_meta_into_datablocks;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;

pub struct RepartitionedQueues(pub Vec<Vec<AggregateMeta>>);

impl RepartitionedQueues {
    pub fn create(partition_count: usize) -> Self {
        let queues = (0..partition_count).map(|_| Vec::new()).collect();
        Self(queues)
    }

    pub fn take_queues(&mut self) -> Self {
        let partition_count = self.0.len();
        std::mem::replace(self, Self::create(partition_count))
    }

    pub fn take_queue(&mut self, partition_idx: usize) -> Vec<AggregateMeta> {
        std::mem::take(&mut self.0[partition_idx])
    }

    pub fn merge_queues(&mut self, other: Self) {
        for (idx, mut queue) in other.0.into_iter().enumerate() {
            self.0[idx].append(&mut queue);
        }
    }

    pub fn push_to_queue(&mut self, partition_idx: usize, meta: AggregateMeta) {
        self.0[partition_idx].push(meta);
    }
}

pub enum RoundPhase {
    Idle,
    NewTask(AggregateMeta),
    OutputReady(DataBlock),
    Aggregate,
    AsyncWait,
}

impl Display for RoundPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoundPhase::Idle => write!(f, "Idle"),
            RoundPhase::NewTask(_) => write!(f, "NewTask"),
            RoundPhase::OutputReady(_) => write!(f, "OutputReady"),
            RoundPhase::AsyncWait => write!(f, "AsyncWait"),
            RoundPhase::Aggregate => write!(f, "Aggregate"),
        }
    }
}

pub struct LocalRoundState {
    pub phase: RoundPhase,
    pub first_data_ready: bool,
    pub is_spilled: bool,
    pub is_reported: bool,
    pub current_queue_spill_round: usize,
    pub max_aggregate_spill_level: usize,
    pub working_queue: Vec<AggregateMeta>,
}

impl LocalRoundState {
    pub fn new(max_aggregate_spill_level: usize) -> Self {
        Self {
            phase: RoundPhase::Idle,
            first_data_ready: false,
            is_spilled: false,
            is_reported: false,
            current_queue_spill_round: 0,
            max_aggregate_spill_level,
            working_queue: Vec::new(),
        }
    }

    /// Resets round-specific flags when a new round begins.
    pub fn reset_for_new_round(&mut self, spill_round: usize) {
        self.is_spilled = false;
        self.is_reported = false;
        self.current_queue_spill_round = spill_round;
    }

    pub fn take_phase(&mut self) -> RoundPhase {
        std::mem::replace(&mut self.phase, RoundPhase::Idle)
    }

    pub fn schedule_next_task(&mut self) -> Option<Event> {
        self.working_queue.pop().map(|aggregate_meta| {
            self.phase = RoundPhase::NewTask(aggregate_meta);
            Event::Sync
        })
    }

    pub fn schedule_async_wait(&mut self) -> Event {
        self.is_reported = true;
        self.phase = RoundPhase::AsyncWait;
        Event::Async
    }

    pub fn enqueue_partitioned_meta(&mut self, datablock: &mut DataBlock) -> Result<()> {
        if let Some(block_meta) = datablock.take_meta().and_then(AggregateMeta::downcast_from) {
            match block_meta {
                AggregateMeta::Partitioned { data, .. } => {
                    self.working_queue.extend(data);
                }
                _ => {
                    return Err(ErrorCode::Internal(
                        "NewFinalAggregateTransform expects Partitioned AggregateMeta from upstream",
                    ));
                }
            }
        }
        Ok(())
    }
}

struct PendingQueue {
    data: Vec<AggregateMeta>,
    spill_round: usize,
}

pub struct FinalAggregateSharedState {
    pub is_spilled: bool,
    pub last_round_is_spilled: bool,
    finished_count: usize,
    partition_count: usize,

    /// Collects the per-partition aggregate metadata reported by each processor during the current round.
    pub repartitioned_queues: RepartitionedQueues,

    pub need_aggregate_queues: RepartitionedQueues,

    /// Partition queues waiting for block-level repartitioning (typically restored from spill).
    pending_queues: Vec<PendingQueue>,

    /// DataBlocks prepared from the pending queues for processors to consume in the next round.
    next_round: Vec<DataBlock>,

    /// Spill counter of the queue currently scheduled in `next_round`.
    current_queue_spill_round: Option<usize>,
}

impl FinalAggregateSharedState {
    pub fn new(partition_count: usize) -> Self {
        Self {
            is_spilled: false,
            last_round_is_spilled: false,
            finished_count: 0,
            repartitioned_queues: RepartitionedQueues::create(partition_count),
            need_aggregate_queues: RepartitionedQueues::create(partition_count),
            pending_queues: vec![],
            next_round: Vec::with_capacity(partition_count),
            current_queue_spill_round: None,
            partition_count,
        }
    }

    pub fn add_repartitioned_queue(&mut self, queues: RepartitionedQueues) {
        self.repartitioned_queues.merge_queues(queues);

        self.finished_count += 1;
        if self.finished_count == self.partition_count {
            self.finished_count = 0;

            let previous_spill_round = self.current_queue_spill_round.take();
            self.last_round_is_spilled = self.is_spilled;

            let queues = self.repartitioned_queues.take_queues();

            if !self.is_spilled {
                self.need_aggregate_queues = queues;
            } else {
                self.is_spilled = false;

                // flush all repartitioned queues to pending queues
                for queue in queues.0.into_iter() {
                    if queue.is_empty() {
                        continue;
                    }
                    self.pending_queues.push(PendingQueue {
                        data: queue,
                        spill_round: previous_spill_round.unwrap_or(0) + 1,
                    });
                }
            }

            if self.next_round.is_empty() {
                if let Some(queue) = self.pending_queues.pop() {
                    self.current_queue_spill_round = Some(queue.spill_round);
                    self.next_round =
                        split_partitioned_meta_into_datablocks(0, queue.data, self.partition_count);
                }
            }
        }
    }

    pub fn get_next_datablock(&mut self) -> Option<(DataBlock, usize)> {
        match self.next_round.pop() {
            Some(block) => Some((block, self.current_queue_spill_round.unwrap_or(0))),
            None => {
                self.current_queue_spill_round = None;
                None
            }
        }
    }
}
