// Copyright 2026 Datafuse Labs
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
use std::time::Instant;

use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_generate_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_index::InvertedIndexMerger;
use databend_storages_common_index::MergeOutput;
use databend_storages_common_index::MergeSource;
use databend_storages_common_index::SourceRows;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;

use crate::FuseBlockPartInfo;
use crate::FuseTable;
use crate::io::block_index::WrittenInvertedIndex;
use crate::io::create_inverted_index_builders;
use crate::operations::MutationLogEntry;
use crate::operations::MutationLogs;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclusterIndexRowRange {
    pub source: u32,
    pub rows: std::ops::Range<u32>,
}

/// Attached after final compact, before serialization. No generic metadata
/// propagation through take/concat/slice is required.
#[derive(Debug)]
pub struct ReclusterIndexInput {
    pub rows: Vec<ReclusterIndexRowRange>,
    pub output_row: u64,
    pub merged_names: Vec<String>,
}

databend_common_expression::local_block_meta_serde!(ReclusterIndexInput);
#[typetag::serde(name = "recluster_index_input")]
impl BlockMetaInfo for ReclusterIndexInput {}

/// Local-only envelope: never exposed to a Flight/commit consumer before the
/// task's index merge finishes successfully.
#[derive(Debug)]
pub struct ReclusterIndexOutput {
    pub meta: ExtendedBlockMeta,
    pub output_row: u64,
    pub rows: Vec<ReclusterIndexRowRange>,
}

databend_common_expression::local_block_meta_serde!(ReclusterIndexOutput);
#[typetag::serde(name = "recluster_index_output")]
impl BlockMetaInfo for ReclusterIndexOutput {}

#[derive(Clone)]
struct ReclusterMergeIndex {
    name: String,
    version: String,
    sources: Vec<BlockIndexMeta>,
    schema: tantivy::schema::Schema,
}

#[derive(Clone)]
pub struct ReclusterIndexMergeSpec {
    indexes: Vec<ReclusterMergeIndex>,
    source_rows: Vec<u32>,
    budget: usize,
    estimated_bytes: usize,
}

impl ReclusterIndexMergeSpec {
    /// Missing/old definitions rebuild normally. Metadata for a selected index
    /// is authoritative: open/IO/schema errors subsequently fail the task.
    pub fn try_create(
        table: &FuseTable,
        task: &ReclusterTask,
        budget: usize,
        output_rows: usize,
    ) -> Result<Option<Self>> {
        if task.inverted_index_sources.len() != task.parts.len() {
            return Ok(None);
        }
        if task.parts.is_empty() {
            return Ok(None);
        }
        let mut source_rows = Vec::with_capacity(task.parts.len());
        for part in &task.parts.partitions {
            let rows = FuseBlockPartInfo::from_part(part)?.nums_rows;
            let Ok(rows) = u32::try_from(rows) else {
                return Ok(None);
            };
            source_rows.push(rows);
        }
        if source_rows.iter().map(|&rows| u64::from(rows)).sum::<u64>() != task.total_rows as u64 {
            return Err(ErrorCode::Internal(
                "recluster source index row count differs from task",
            ));
        }
        let mut indexes = Vec::new();
        let mut largest_bundle_bytes = 0usize;
        for builder in create_inverted_index_builders(&table.get_table_info().meta) {
            let mut sources = Vec::with_capacity(source_rows.len());
            for metas in &task.inverted_index_sources {
                let Some(meta) = metas.iter().find(|meta| {
                    meta.index_name == builder.name
                        && meta.index_version == builder.version
                        && meta.location.1 == INVERTED_INDEX_FILE_FORMAT_VERSION
                        && meta.size > 0
                }) else {
                    break;
                };
                sources.push(meta.clone());
            }
            if sources.len() != source_rows.len() {
                continue;
            }
            let bytes = sources.iter().try_fold(0usize, |sum, meta| {
                let bytes = usize::try_from(meta.size).map_err(|_| {
                    ErrorCode::MemoryExceedsLimit("index size exceeds address space")
                })?;
                sum.checked_add(bytes)
                    .ok_or_else(|| ErrorCode::MemoryExceedsLimit("index merge size overflow"))
            })?;
            largest_bundle_bytes = largest_bundle_bytes.max(bytes);
            let (schema, _) =
                crate::io::create_index_schema(Arc::new(builder.schema.clone()), &builder.options)?;
            indexes.push(ReclusterMergeIndex {
                name: builder.name,
                version: builder.version,
                sources,
                schema,
            });
        }
        if indexes.is_empty() {
            return Ok(None);
        }
        // The current Tantivy merger expands doc mappings/origins and holds
        // all output writers. Conservative admission; not a strict peak bound
        // on postings scratch space or external sibling decoding.
        let outputs = task.total_rows.div_ceil(output_rows.max(1)).max(1);
        let estimated_bytes = task
            .total_rows
            .saturating_mul(64)
            .saturating_add(largest_bundle_bytes.saturating_mul(2));
        if outputs > u16::MAX as usize || estimated_bytes > budget {
            return Ok(None);
        }
        Ok(Some(Self {
            indexes,
            source_rows,
            budget,
            estimated_bytes,
        }))
    }

    pub fn merged_names(&self) -> Vec<String> {
        self.indexes
            .iter()
            .map(|index| index.name.clone())
            .collect()
    }
}

pub struct TransformReclusterIndexMerge {
    table: FuseTable,
    ctx: Arc<dyn TableContext>,
    merge: ReclusterIndexMergeSpec,
    outputs: Vec<ReclusterIndexOutput>,
    retained_range_bytes: usize,
}

impl TransformReclusterIndexMerge {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: FuseTable,
        merge: ReclusterIndexMergeSpec,
    ) -> Self {
        Self {
            ctx,
            table,
            merge,
            outputs: Vec::new(),
            retained_range_bytes: 0,
        }
    }
}

impl AccumulatingTransform for TransformReclusterIndexMerge {
    const NAME: &'static str = "ReclusterInvertedIndexMerge";

    fn transform(&mut self, mut block: DataBlock) -> Result<Vec<DataBlock>> {
        let meta = block
            .take_meta()
            .ok_or_else(|| ErrorCode::Internal("missing recluster index output"))?;
        let output = ReclusterIndexOutput::downcast_from(meta)
            .ok_or_else(|| ErrorCode::Internal("unexpected recluster index output"))?;
        let rows = output.rows.iter().try_fold(0u64, |rows, origin| {
            let source_rows = self
                .merge
                .source_rows
                .get(origin.source as usize)
                .ok_or_else(|| ErrorCode::Internal("unknown recluster index row source"))?;
            if origin.rows.start >= origin.rows.end || origin.rows.end > *source_rows {
                return Err(ErrorCode::Internal("invalid recluster index row range"));
            }
            Ok(rows + u64::from(origin.rows.end - origin.rows.start))
        })?;
        if output.meta.block_meta.row_count > u64::from(u32::MAX) {
            return Err(ErrorCode::MemoryExceedsLimit(
                "recluster index output exceeds UInt32 doc count",
            ));
        }
        if rows != output.meta.block_meta.row_count {
            return Err(ErrorCode::Internal(
                "recluster index lineage row count mismatch",
            ));
        }
        self.ctx
            .check_aborting()
            .map_err(|err| err.with_context("recluster index mapping"))?;
        self.retained_range_bytes = self.retained_range_bytes.saturating_add(
            output
                .rows
                .capacity()
                .saturating_mul(std::mem::size_of::<ReclusterIndexRowRange>()),
        );
        let retained = self
            .merge
            .estimated_bytes
            .saturating_add(self.retained_range_bytes);
        // Output count alone is not a memory estimate: small output indexes
        // may retain only a few KiB. Check actual allocation while Tantivy runs.
        if retained > self.merge.budget || self.outputs.len() >= u16::MAX as usize {
            return Err(ErrorCode::MemoryExceedsLimit(
                "recluster index output mapping exceeds admitted budget",
            ));
        }
        self.outputs.push(output);
        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output {
            self.outputs.clear();
            self.retained_range_bytes = 0;
            return Ok(vec![]);
        }
        self.ctx
            .check_aborting()
            .map_err(|err| err.with_context("recluster index merge"))?;
        let expected = self
            .merge
            .source_rows
            .iter()
            .map(|&rows| u64::from(rows))
            .sum::<u64>();
        let rows = self
            .outputs
            .iter()
            .map(|out| out.meta.block_meta.row_count)
            .sum::<u64>();
        if rows != expected {
            return Err(ErrorCode::Internal("recluster index merge lost input rows"));
        }
        if self.outputs.len() > u16::MAX as usize {
            return Err(ErrorCode::MemoryExceedsLimit(
                "too many recluster index outputs",
            ));
        }
        // Check actual mapping/metadata growth before opening any output index.
        let range_bytes = self
            .outputs
            .iter()
            .map(|out| {
                out.rows
                    .len()
                    .saturating_mul(std::mem::size_of::<ReclusterIndexRowRange>())
            })
            .sum::<usize>();
        let estimate = self.merge.estimated_bytes.saturating_add(range_bytes);
        if estimate > self.merge.budget {
            return Err(ErrorCode::MemoryExceedsLimit(
                "recluster index mapping exceeds admitted budget",
            ));
        }
        self.outputs
            .sort_unstable_by_key(|output| output.output_row);
        let mut next_row = 0u64;
        for output in &self.outputs {
            if output.output_row != next_row {
                return Err(ErrorCode::Internal(
                    "recluster index output order has gaps or overlaps",
                ));
            }
            next_row = next_row
                .checked_add(output.meta.block_meta.row_count)
                .ok_or_else(|| ErrorCode::Internal("recluster index output rows overflow"))?;
        }
        let mut source_positions = vec![0u32; self.merge.source_rows.len()];
        for output in &self.outputs {
            for origin in &output.rows {
                let position = &mut source_positions[origin.source as usize];
                if origin.rows.start != *position {
                    return Err(ErrorCode::Internal(
                        "recluster index lineage has reordered or missing source rows",
                    ));
                }
                *position = origin.rows.end;
            }
        }
        if source_positions != self.merge.source_rows {
            return Err(ErrorCode::Internal(
                "recluster index lineage does not cover all source rows",
            ));
        }
        let locations = self.table.meta_location_generator();
        for index in &self.merge.indexes {
            self.ctx
                .check_aborting()
                .map_err(|err| err.with_context("recluster index merge"))?;
            // Reserve for the two postings/positions upload streams per output.
            // Do not open all target Tantivy writers simultaneously.
            let writer_bytes = databend_storages_common_io::blocking_write_retained_bytes(
                &self.table.get_operator(),
                databend_storages_common_io::BLOCKING_WRITE_MAX_CHUNKS,
            )
            .saturating_mul(2)
            .max(1);
            let batch_outputs = (self.merge.budget / 2 / writer_bytes).max(1);
            let index_start = Instant::now();
            for output_batch in self.outputs.chunks_mut(batch_outputs) {
                let start = Instant::now();
                let sources = index
                    .sources
                    .iter()
                    .zip(&self.merge.source_rows)
                    .map(|(meta, &num_rows)| MergeSource {
                        location: meta.location.0.clone(),
                        bundle_size: meta.size,
                        num_rows,
                    })
                    .collect();
                let mut output_locations = Vec::with_capacity(output_batch.len());
                let mut outputs = Vec::with_capacity(output_batch.len());
                for output in output_batch.iter() {
                    let location = locations.gen_inverted_index_v2_location(&index.version);
                    output_locations.push(location.clone());
                    outputs.push(MergeOutput {
                        location,
                        rows: output
                            .rows
                            .iter()
                            .map(|origin| SourceRows {
                                source: origin.source,
                                rows: origin.rows.clone(),
                            })
                            .collect(),
                    });
                }
                let ctx = self.ctx.clone();
                let budget = self.merge.budget;
                let memory = databend_common_base::runtime::ThreadTracker::mem_stat().cloned();
                let parent = memory.clone().map_or(
                    databend_common_base::runtime::ParentMemStat::StaticRef(
                        &databend_common_base::runtime::GLOBAL_MEM_STAT,
                    ),
                    databend_common_base::runtime::ParentMemStat::Normal,
                );
                let merge_memory = databend_common_base::runtime::MemStat::create_child(
                    Some("recluster inverted index batch".into()),
                    0,
                    parent,
                );
                let mut payload =
                    databend_common_base::runtime::ThreadTracker::new_tracking_payload();
                payload.mem_stat = Some(merge_memory.clone());
                let _merge_tracking =
                    databend_common_base::runtime::ThreadTracker::tracking(payload);
                let settings = ctx.get_settings();
                let global_limit = settings.get_max_memory_usage()? as usize;
                let query_limit = settings.get_max_query_memory_usage()? as usize;
                let check = Box::new(move || -> std::io::Result<()> {
                    ctx.check_aborting()
                        .map_err(|err| std::io::Error::other(err.to_string()))?;
                    let used = memory.as_ref().map_or(0, |stat| stat.get_memory_usage());
                    let global_used =
                        databend_common_base::runtime::GLOBAL_MEM_STAT.get_memory_usage();
                    if (global_limit != 0 && global_used >= global_limit)
                        || (query_limit != 0 && used >= query_limit)
                        || merge_memory.get_memory_usage() > budget
                    {
                        return Err(std::io::Error::other(format!(
                            "recluster index merge exceeded memory allowance: used={used}, batch_used={}, budget={budget}, global_used={global_used}, global_limit={global_limit}, query_limit={query_limit}",
                            merge_memory.get_memory_usage(),
                        )));
                    }
                    Ok(())
                });
                let sizes = InvertedIndexMerger::try_create_for_recluster_batch(
                    self.table.get_operator(),
                    sources,
                    outputs,
                    &index.schema,
                    check,
                )
                .map_err(|err| {
                    ErrorCode::StorageOther(format!("open recluster inverted index merge: {err}"))
                })?
                .finish()
                .map_err(|err| {
                    ErrorCode::StorageOther(format!("finish recluster inverted index merge: {err}"))
                })?;
                let elapsed_ms = start.elapsed().as_millis() as u64;
                metrics_inc_block_inverted_index_generate_milliseconds(elapsed_ms);
                if sizes.len() != output_batch.len() {
                    return Err(ErrorCode::Internal("index merger output count mismatch"));
                }
                for ((output, location), sizes) in
                    output_batch.iter_mut().zip(output_locations).zip(sizes)
                {
                    let total_size = sizes
                        .bundle
                        .checked_add(sizes.siblings)
                        .ok_or_else(|| ErrorCode::Internal("inverted index total size overflow"))?;
                    metrics_inc_block_inverted_index_write_nums(1);
                    metrics_inc_block_inverted_index_write_bytes(total_size);
                    let written = WrittenInvertedIndex {
                        index_name: index.name.clone(),
                        index_version: index.version.clone(),
                        location: (location, INVERTED_INDEX_FILE_FORMAT_VERSION),
                        bundle_size: sizes.bundle,
                        total_size,
                    };
                    let block = &mut output.meta.block_meta;
                    block.inverted_index_size = Some(
                        block
                            .inverted_index_size
                            .unwrap_or(0)
                            .checked_add(written.total_size)
                            .ok_or_else(|| ErrorCode::Internal("inverted index size overflow"))?,
                    );
                    let metas = block.inverted_index_metas.get_or_insert_with(Vec::new);
                    if metas.iter().any(|meta| meta.index_name == index.name) {
                        return Err(ErrorCode::Internal("duplicate merged inverted index"));
                    }
                    metas.push(written.to_block_index_meta());
                    metas.sort_unstable_by(|a, b| a.index_name.cmp(&b.index_name));
                }
            }
            let elapsed_ms = index_start.elapsed().as_millis() as u64;
            metrics_inc_block_inverted_index_write_milliseconds(elapsed_ms);
            log::info!(
                "recluster merged inverted index: name={}, sources={}, outputs={}",
                index.name,
                index.sources.len(),
                self.outputs.len()
            );
        }
        self.ctx
            .check_aborting()
            .map_err(|err| err.with_context("publish recluster index metadata"))?;
        let mut result = Vec::with_capacity(self.outputs.len());
        for output in self.outputs.drain(..) {
            result.push(DataBlock::empty_with_meta(Box::new(MutationLogs {
                entries: vec![MutationLogEntry::AppendBlock {
                    block_meta: Arc::new(output.meta),
                    merge_hll: false,
                }],
                ..Default::default()
            })));
        }
        Ok(result)
    }
}
