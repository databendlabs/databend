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

//! Merging the block indexes of a recluster task's source blocks into its output blocks.
//!
//! Index kinds take part through [`BlockIndexSpec::can_merge`] and [`BlockIndexSpec::new_merger`];
//! the stages here only schedule. Two pipelines share one [`ReclusterIndexMergeState`]:
//!
//! ```text
//! data:  ... → SerializeBlock(N) → resize(1) → LineageSink              ─ lineage ─┐
//! index: Partitioner(→W) → Slice(W) → SliceScatter(W→N) → Writer(N) → [CommitSink] ◂┘
//! ```
//!
//! The partitioner creates the mergers (which open the sources and cut the partitions) while
//! the data pipeline still writes, then waits for the lineage before it hands out partitions.
//! Partitioner, slice readers and scatter follow the k-way merge sort: partitions go to
//! whichever reader is free, each reader sees increasing ids, and the scatter holds one block
//! per reader and replays them in id order, so the partitions in flight stay bounded by the
//! ports. The end of the partitions travels as port finish, stage by stage.
//!
//! [`BlockIndexSpec::can_merge`]: crate::io::block_index::BlockIndexSpec::can_merge
//! [`BlockIndexSpec::new_merger`]: crate::io::block_index::BlockIndexSpec::new_merger

mod lineage;
mod partition;
mod scatter;
mod slice;
mod state;
mod writer;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::local_block_meta_serde;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::Sinker;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;

pub use self::state::ReclusterIndexMergeState;
use crate::FuseBlockPartInfo;
use crate::FuseTable;
use crate::io::block_index::BLOCK_INDEX_MERGE_PARTITION_BYTES;
use crate::io::block_index::BlockIndexMergeContext;
use crate::io::block_index::BlockIndexMergeSource;
pub use crate::io::block_index::BlockIndexSourceRows as ReclusterIndexRowRange;
use crate::io::block_index::BlockIndexSpec;
use crate::io::block_index::create_block_index_specs;

/// Final block lineage, attached after compact and removed before serialization.
#[derive(Debug)]
pub struct ReclusterIndexInput {
    pub rows: Vec<ReclusterIndexRowRange>,
    pub output_row: u64,
    pub merged_names: Vec<String>,
}

local_block_meta_serde!(ReclusterIndexInput);
#[typetag::serde(name = "recluster_index_input")]
impl BlockMetaInfo for ReclusterIndexInput {}

/// Block metadata of one written output plus where its rows came from.
#[derive(Debug)]
pub struct ReclusterIndexOutput {
    pub meta: ExtendedBlockMeta,
    pub output_row: u64,
    pub rows: Vec<ReclusterIndexRowRange>,
}

local_block_meta_serde!(ReclusterIndexOutput);
#[typetag::serde(name = "recluster_index_output")]
impl BlockMetaInfo for ReclusterIndexOutput {}

/// The indexes a task merges, with the row counts and index metadata of its sources.
#[derive(Clone)]
pub struct ReclusterIndexMergeInputs {
    specs: Vec<Arc<dyn BlockIndexSpec>>,
    source_rows: Vec<u32>,
    source_indexes: Vec<Vec<BlockIndexMeta>>,
}

impl ReclusterIndexMergeInputs {
    /// None when the task's indexes are rebuilt instead. Metadata only.
    pub fn try_create(table: &FuseTable, task: &ReclusterTask) -> Result<Option<Self>> {
        if task.parts.is_empty() || task.inverted_index_sources.len() != task.parts.len() {
            return Ok(None);
        }
        let mut source_rows = Vec::with_capacity(task.parts.len());
        for part in &task.parts.partitions {
            let Ok(rows) = u32::try_from(FuseBlockPartInfo::from_part(part)?.nums_rows) else {
                return Ok(None);
            };
            source_rows.push(rows);
        }
        let mut total_rows = 0u64;
        for &rows in &source_rows {
            total_rows += u64::from(rows);
        }
        if total_rows != task.total_rows as u64 {
            return Err(ErrorCode::Internal(
                "recluster source index row count differs from task",
            ));
        }
        let inputs = Self {
            specs: Vec::new(),
            source_rows,
            source_indexes: task.inverted_index_sources.clone(),
        };
        let sources = inputs.merge_sources();
        let mut specs = Vec::new();
        for spec in create_block_index_specs(table, table.schema_with_stream())? {
            if spec.can_merge(&sources)? {
                specs.push(spec);
            }
        }
        if specs.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self { specs, ..inputs }))
    }

    /// The indexes being merged, in index order.
    pub fn specs(&self) -> &[Arc<dyn BlockIndexSpec>] {
        &self.specs
    }

    pub fn source_rows(&self) -> &[u32] {
        &self.source_rows
    }

    /// The sources as the index kinds see them.
    pub fn merge_sources(&self) -> Vec<BlockIndexMergeSource<'_>> {
        let mut sources = Vec::with_capacity(self.source_rows.len());
        for (&num_rows, indexes) in self.source_rows.iter().zip(&self.source_indexes) {
            sources.push(BlockIndexMergeSource { num_rows, indexes });
        }
        sources
    }

    /// Names of the merged indexes, which the block writers must not rebuild.
    pub fn merged_names(&self) -> Vec<String> {
        let mut names = Vec::with_capacity(self.specs.len());
        for spec in &self.specs {
            if let Some(name) = spec.index_name() {
                names.push(name.to_owned());
            }
        }
        names
    }
}

/// Ends `data_pipeline`, whose blocks carry the [`ReclusterIndexOutput`] of every written
/// block, with the lineage sink, and builds the index merge into `pipeline`, which must be
/// empty and ends `threads` wide, emitting one `MutationLogs` block per output. Both pipelines
/// must run in the same executor.
pub fn build_recluster_index_merge_pipeline(
    data_pipeline: &mut Pipeline,
    pipeline: &mut Pipeline,
    ctx: Arc<dyn TableContext>,
    table: &FuseTable,
    inputs: ReclusterIndexMergeInputs,
    threads: usize,
) -> Result<()> {
    let context = BlockIndexMergeContext {
        operator: table.get_operator(),
        locations: table.meta_location_generator().clone(),
        abort_checker: ctx.get_abort_checker(),
        partition_bytes: BLOCK_INDEX_MERGE_PARTITION_BYTES,
    };
    let state = Arc::new(ReclusterIndexMergeState::new(inputs, context, threads));
    build_recluster_index_merge_pipeline_with_state(data_pipeline, pipeline, state)
}

/// [`build_recluster_index_merge_pipeline`] with a prepared state, so tests can shrink the
/// partitions and inspect the run afterwards.
pub fn build_recluster_index_merge_pipeline_with_state(
    data_pipeline: &mut Pipeline,
    pipeline: &mut Pipeline,
    state: Arc<ReclusterIndexMergeState>,
) -> Result<()> {
    let threads = state.threads;

    data_pipeline.try_resize(1)?;
    let lineage_state = state.clone();
    data_pipeline.add_sink(move |input| {
        Ok(ProcessorPtr::create(Sinker::create(
            input,
            lineage::ReclusterIndexLineageSink::new(lineage_state.clone()),
        )))
    })?;

    let outputs: Vec<_> = (0..threads).map(|_| OutputPort::create()).collect();
    let partitioner = partition::ReclusterIndexPartitioner::create(outputs.clone(), state.clone());
    pipeline.add_pipe(Pipe::create(0, threads, vec![PipeItem::create(
        partitioner,
        vec![],
        outputs,
    )]));

    let slice_state = state.clone();
    pipeline.add_transformer(move || slice::TransformReclusterIndexSlice::new(slice_state.clone()));

    let inputs: Vec<_> = (0..threads).map(|_| InputPort::create()).collect();
    let outputs: Vec<_> = (0..threads).map(|_| OutputPort::create()).collect();
    let scatter = scatter::ReclusterIndexSliceScatter::create(inputs.clone(), outputs.clone());
    pipeline.add_pipe(Pipe::create(threads, threads, vec![PipeItem::create(
        scatter, inputs, outputs,
    )]));

    let writer = AtomicUsize::new(0);
    pipeline.add_accumulating_transformer(move || {
        let index = writer.fetch_add(1, Ordering::Relaxed);
        writer::TransformReclusterIndexWriter::new(state.clone(), index, threads)
    });
    Ok(())
}
