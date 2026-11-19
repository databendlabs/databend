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

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_index::RowMapping;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use tokio::sync::SetOnce;

use super::ReclusterIndexMergeInputs;
use crate::io::block_index::BlockIndexMergeContext;
use crate::io::block_index::BlockIndexMerger;

/// What the two pipelines share within one task: the mergers the partitioner creates, and the
/// lineage the sink at the end of the data pipeline publishes for the index pipeline to await.
pub struct ReclusterIndexMergeState {
    pub inputs: ReclusterIndexMergeInputs,
    pub context: BlockIndexMergeContext,
    /// Partition readers and output writers.
    pub threads: usize,
    /// `mergers[index]`, created by the partitioner.
    mergers: Vec<OnceLock<Arc<dyn BlockIndexMerger>>>,
    /// Partitions over every index; the writers check they saw them all.
    partitions_total: OnceLock<u32>,
    lineage: SetOnce<OutputLineage>,
}

/// Where every output's rows come from, and the outputs' block metadata.
struct OutputLineage {
    mapping: Arc<RowMapping>,
    /// Taken once by the writer that finishes the output.
    metas: Mutex<Vec<Option<ExtendedBlockMeta>>>,
}

impl ReclusterIndexMergeState {
    pub fn new(
        inputs: ReclusterIndexMergeInputs,
        context: BlockIndexMergeContext,
        threads: usize,
    ) -> Self {
        let mut mergers = Vec::with_capacity(inputs.specs().len());
        mergers.resize_with(inputs.specs().len(), OnceLock::new);
        Self {
            inputs,
            context,
            threads: threads.max(1),
            mergers,
            partitions_total: OnceLock::new(),
            lineage: SetOnce::new(),
        }
    }

    pub fn set_merger(&self, index: usize, merger: Arc<dyn BlockIndexMerger>) -> Result<()> {
        if self.mergers[index].set(merger).is_err() {
            return Err(ErrorCode::Internal("recluster index merger created twice"));
        }
        Ok(())
    }

    pub fn merger(&self, index: usize) -> Result<Arc<dyn BlockIndexMerger>> {
        match self.mergers.get(index).and_then(OnceLock::get) {
            Some(merger) => Ok(merger.clone()),
            None => Err(ErrorCode::Internal("recluster index merger is not ready")),
        }
    }

    pub fn set_partitions_total(&self, total: u32) {
        let _ = self.partitions_total.set(total);
    }

    pub fn partitions_total(&self) -> Option<u32> {
        self.partitions_total.get().copied()
    }

    /// Row mapping and output metadata, in output order; releases [`Self::wait_lineage`].
    pub fn set_lineage(&self, mapping: RowMapping, metas: Vec<ExtendedBlockMeta>) -> Result<()> {
        if metas.len() != mapping.num_outputs() {
            return Err(ErrorCode::Internal(
                "recluster index outputs differ from the row mapping",
            ));
        }
        let mut slots = Vec::with_capacity(metas.len());
        for meta in metas {
            slots.push(Some(meta));
        }
        let lineage = OutputLineage {
            mapping: Arc::new(mapping),
            metas: Mutex::new(slots),
        };
        if self.lineage.set(lineage).is_err() {
            return Err(ErrorCode::Internal("recluster index lineage set twice"));
        }
        Ok(())
    }

    /// Resolves once the data pipeline has set the lineage. If that pipeline fails instead, the
    /// executor cancels this wait along with everything else.
    pub async fn wait_lineage(&self) {
        self.lineage.wait().await;
    }

    fn lineage(&self) -> Result<&OutputLineage> {
        match self.lineage.get() {
            Some(lineage) => Ok(lineage),
            None => Err(ErrorCode::Internal("recluster index lineage is not ready")),
        }
    }

    pub fn mapping(&self) -> Result<Arc<RowMapping>> {
        Ok(self.lineage()?.mapping.clone())
    }

    pub fn num_outputs(&self) -> Result<usize> {
        Ok(self.lineage()?.mapping.num_outputs())
    }

    /// Takes the block metadata of `output` for its final `MutationLogs` entry.
    pub fn take_output_meta(&self, output: usize) -> Result<ExtendedBlockMeta> {
        let mut metas = self.lineage()?.metas.lock().unwrap();
        let taken = match metas.get_mut(output) {
            Some(slot) => slot.take(),
            None => None,
        };
        match taken {
            Some(meta) => Ok(meta),
            None => Err(ErrorCode::Internal(format!(
                "recluster index output {output} taken twice"
            ))),
        }
    }
}
