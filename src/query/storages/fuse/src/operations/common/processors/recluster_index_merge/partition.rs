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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use super::state::ReclusterIndexMergeState;

/// One partition of one index. Ids run through the indexes in order, so a later index's
/// partitions all follow an earlier index's.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexPartition {
    pub id: u32,
    pub index: u16,
    pub partition: u32,
}

#[typetag::serde(name = "recluster_index_partition")]
impl BlockMetaInfo for IndexPartition {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        IndexPartition::downcast_ref_from(info) == Some(self)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

/// Start of the index pipeline. Creates the merger of every index, which opens the sources
/// and cuts the partitions, waits for the data pipeline to publish the lineage, then hands the
/// partitions out in id order to whichever reader can take one, the way the k-way merge sort
/// partitioner hands out its tasks. Each reader therefore sees increasing ids, which is what
/// the scatter relies on. Finishing the outputs tells the stages downstream that every
/// partition was sent.
pub struct ReclusterIndexPartitioner {
    outputs: Vec<Arc<OutputPort>>,
    state: Arc<ReclusterIndexMergeState>,
    phase: Phase,
    partitions: VecDeque<IndexPartition>,
    next_output: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Mergers not yet created.
    Starting,
    /// Partitions known; the lineage from the data pipeline not yet seen.
    WaitingLineage,
    /// Handing out partitions.
    Dispatching,
}

impl ReclusterIndexPartitioner {
    pub fn create(
        outputs: Vec<Arc<OutputPort>>,
        state: Arc<ReclusterIndexMergeState>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(Self {
            outputs,
            state,
            phase: Phase::Starting,
            partitions: VecDeque::new(),
            next_output: 0,
        }))
    }

    /// Next output that can take a block, round-robin.
    fn find_output(&mut self) -> Option<usize> {
        let n = self.outputs.len();
        for step in 0..n {
            let index = (self.next_output + step) % n;
            let output = &self.outputs[index];
            if !output.is_finished() && output.can_push() {
                self.next_output = (index + 1) % n;
                return Some(index);
            }
        }
        None
    }

    /// Opens the sources of every index and lists the partitions.
    fn start(&mut self) -> Result<()> {
        let inputs = &self.state.inputs;
        let sources = inputs.merge_sources();
        let mut next_id = 0u32;
        for (index, spec) in inputs.specs().iter().enumerate() {
            self.state.context.abort_checker.try_check_aborting()?;
            let merger = spec.new_merger(&sources, &self.state.context)?;
            let num_partitions = merger.num_partitions();
            if num_partitions > u32::MAX as usize {
                return Err(ErrorCode::Internal(
                    "recluster index has too many partitions",
                ));
            }
            for partition in 0..num_partitions as u32 {
                self.partitions.push_back(IndexPartition {
                    id: next_id,
                    index: index as u16,
                    partition,
                });
                next_id += 1;
            }
            self.state.set_merger(index, merger)?;
        }
        self.state.set_partitions_total(next_id);
        self.phase = Phase::WaitingLineage;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Processor for ReclusterIndexPartitioner {
    fn name(&self) -> String {
        "ReclusterIndexPartitioner".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let mut all_finished = true;
        for output in &self.outputs {
            if !output.is_finished() {
                all_finished = false;
            }
        }
        if all_finished {
            return Ok(Event::Finished);
        }
        match self.phase {
            // Opening the sources is blocking IO plus CPU.
            Phase::Starting => Ok(Event::Sync),
            Phase::WaitingLineage => Ok(Event::Async),
            Phase::Dispatching => {
                if self.partitions.is_empty() {
                    for output in &self.outputs {
                        output.finish();
                    }
                    return Ok(Event::Finished);
                }
                let Some(index) = self.find_output() else {
                    return Ok(Event::NeedConsume);
                };
                let partition = self.partitions.pop_front().expect("checked above");
                let block = DataBlock::empty_with_meta(Box::new(partition));
                self.outputs[index].push_data(Ok(block));
                Ok(Event::NeedConsume)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if self.phase == Phase::Starting {
            self.start()?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        self.state.wait_lineage().await;
        self.phase = Phase::Dispatching;
        Ok(())
    }
}
