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

use std::io;
use std::sync::Arc;

use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::local_block_meta_serde;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;

use crate::FuseBlockPartInfo;
use crate::FuseTable;
use crate::io::block_index::BlockIndexMerge;
use crate::io::block_index::BlockIndexMergeContext;
use crate::io::block_index::BlockIndexMergeSource;
pub use crate::io::block_index::BlockIndexSourceRows as ReclusterIndexRowRange;
use crate::io::block_index::create_block_index_specs;
use crate::operations::MutationLogEntry;
use crate::operations::MutationLogs;

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

/// Hold block metadata until all task index merges succeed.
#[derive(Debug)]
pub struct ReclusterIndexOutput {
    pub meta: ExtendedBlockMeta,
    pub output_row: u64,
    pub rows: Vec<ReclusterIndexRowRange>,
}

local_block_meta_serde!(ReclusterIndexOutput);
#[typetag::serde(name = "recluster_index_output")]
impl BlockMetaInfo for ReclusterIndexOutput {}

#[derive(Clone)]
pub struct ReclusterIndexMergeInputs {
    indexes: Vec<Arc<dyn BlockIndexMerge>>,
    source_rows: Vec<u32>,
}

impl ReclusterIndexMergeInputs {
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
        let total_rows: u64 = source_rows.iter().map(|&rows| u64::from(rows)).sum();
        if total_rows != task.total_rows as u64 {
            return Err(ErrorCode::Internal(
                "recluster source index row count differs from task",
            ));
        }
        let sources = source_rows
            .iter()
            .zip(&task.inverted_index_sources)
            .map(|(&num_rows, indexes)| BlockIndexMergeSource { num_rows, indexes })
            .collect::<Vec<_>>();
        let mut indexes = Vec::new();
        for spec in create_block_index_specs(table, table.schema_with_stream())? {
            if let Some(index) = spec.prepare_merge(&sources)? {
                indexes.push(index);
            }
        }
        if indexes.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            indexes,
            source_rows,
        }))
    }

    pub fn merged_names(&self) -> Vec<String> {
        self.indexes
            .iter()
            .map(|index| index.index_name().to_owned())
            .collect()
    }
}

pub struct TransformReclusterIndexMerge {
    table: FuseTable,
    ctx: Arc<dyn TableContext>,
    merge: ReclusterIndexMergeInputs,
    outputs: Vec<ReclusterIndexOutput>,
}

impl TransformReclusterIndexMerge {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: FuseTable,
        merge: ReclusterIndexMergeInputs,
    ) -> Self {
        Self {
            ctx,
            table,
            merge,
            outputs: Vec::new(),
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
        let mut rows = 0u64;
        for origin in &output.rows {
            let source_rows = self
                .merge
                .source_rows
                .get(origin.source as usize)
                .ok_or_else(|| ErrorCode::Internal("unknown recluster index row source"))?;
            if origin.rows.start >= origin.rows.end || origin.rows.end > *source_rows {
                return Err(ErrorCode::Internal("invalid recluster index row range"));
            }
            rows += u64::from(origin.rows.end - origin.rows.start);
        }
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
        self.outputs.push(output);
        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output {
            self.outputs.clear();
            return Ok(vec![]);
        }
        self.ctx
            .check_aborting()
            .map_err(|err| err.with_context("recluster index merge"))?;
        let source_rows = &self.merge.source_rows;
        let expected: u64 = source_rows.iter().map(|&rows| u64::from(rows)).sum();
        let rows = self
            .outputs
            .iter()
            .map(|out| out.meta.block_meta.row_count)
            .sum::<u64>();
        if rows != expected {
            return Err(ErrorCode::Internal("recluster index merge lost input rows"));
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
        // UInt16 destination ordinals are local to one merger, not the task.
        let batch_outputs =
            (self.ctx.get_settings().get_max_threads()? as usize).clamp(1, u16::MAX as usize);
        for index in &self.merge.indexes {
            self.ctx
                .check_aborting()
                .map_err(|err| err.with_context("recluster index merge"))?;
            for output_batch in self.outputs.chunks_mut(batch_outputs) {
                let outputs = output_batch
                    .iter()
                    .map(|output| output.rows.clone())
                    .collect();
                let ctx = self.ctx.clone();
                let check = Box::new(move || -> io::Result<()> {
                    ctx.check_aborting()
                        .map_err(|err| io::Error::other(err.to_string()))?;
                    Ok(())
                });
                let context = BlockIndexMergeContext {
                    operator: self.table.get_operator(),
                    locations: locations.clone(),
                    outputs,
                    check_interrupt: check,
                };
                let written = index.merge(context)?;
                if written.len() != output_batch.len() {
                    return Err(ErrorCode::Internal("index merger output count mismatch"));
                }
                for (output, written) in output_batch.iter_mut().zip(written) {
                    index.apply_output(&mut output.meta.block_meta, written)?;
                }
            }
            log::info!(
                "recluster merged inverted index: name={}, sources={}, outputs={}",
                index.index_name(),
                self.merge.source_rows.len(),
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
