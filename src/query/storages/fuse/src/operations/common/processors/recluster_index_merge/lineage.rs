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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::sinks::Sink;
use databend_storages_common_index::RowMapping;

use super::ReclusterIndexOutput;
use super::state::ReclusterIndexMergeState;

/// End of the data pipeline: collects the written outputs with their row lineage, checks that
/// they cover every source row exactly once, and publishes it to the index pipeline.
pub struct ReclusterIndexLineageSink {
    state: Arc<ReclusterIndexMergeState>,
    outputs: Vec<ReclusterIndexOutput>,
}

impl ReclusterIndexLineageSink {
    pub fn new(state: Arc<ReclusterIndexMergeState>) -> Self {
        Self {
            state,
            outputs: Vec::new(),
        }
    }
}

impl Sink for ReclusterIndexLineageSink {
    const NAME: &'static str = "ReclusterIndexLineage";

    fn consume(&mut self, mut block: DataBlock) -> Result<()> {
        let meta = block
            .take_meta()
            .ok_or_else(|| ErrorCode::Internal("missing recluster index output"))?;
        let output = ReclusterIndexOutput::downcast_from(meta)
            .ok_or_else(|| ErrorCode::Internal("unexpected recluster index output"))?;
        let source_rows = self.state.inputs.source_rows();
        let mut rows = 0u64;
        for origin in &output.rows {
            let num_rows = source_rows
                .get(origin.source as usize)
                .ok_or_else(|| ErrorCode::Internal("unknown recluster index row source"))?;
            if origin.rows.start >= origin.rows.end || origin.rows.end > *num_rows {
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
        self.state.context.abort_checker.try_check_aborting()?;
        self.outputs.push(output);
        Ok(())
    }

    fn on_finish(&mut self) -> Result<()> {
        self.state.context.abort_checker.try_check_aborting()?;
        let source_rows = self.state.inputs.source_rows();
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
        let mut source_positions = vec![0u32; source_rows.len()];
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
        if source_positions != source_rows {
            return Err(ErrorCode::Internal(
                "recluster index lineage does not cover all source rows",
            ));
        }

        let mut metas = Vec::with_capacity(self.outputs.len());
        let mut lineage = Vec::with_capacity(self.outputs.len());
        for output in std::mem::take(&mut self.outputs) {
            metas.push(output.meta);
            lineage.push(output.rows);
        }
        let mapping = RowMapping::build(source_rows, lineage)
            .map_err(|error| ErrorCode::Internal(error.to_string()))?;
        log::info!(
            "recluster index merge lineage ready: sources={}, outputs={}, indexes={}",
            source_rows.len(),
            metas.len(),
            self.state.inputs.specs().len()
        );
        self.state.set_lineage(mapping, metas)
    }
}
