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

use databend_base::uniq_id::GlobalUniq;
use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::core::check_interrupt;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::sorts::core::RowConverter;
use databend_common_pipeline_transforms::sorts::core::Rows;
use databend_common_pipeline_transforms::traits::Location;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ReadSettings;

use super::SpilledCandidates;
use super::TopNCandidates;
use super::split_block;
use crate::sessions::QueryContext;
use crate::spillers::Layout;
use crate::spillers::SpillAdapter;
use crate::spillers::SpillTarget;
use crate::spillers::SpillsBufferPool;

/// The partial stage of TopN.
///
/// It sifts each unsorted input block through the candidate set and emits the
/// sorted candidates (at most `capacity` rows, carrying the order column) at
/// the end of input. The order column is computed by the row converter, or
/// taken directly from a supported source sort column.
///
/// Spilling is synchronous: when memory pressure is detected after sifting a
/// block, the consolidated candidates are written out as one sorted candidate
/// file and restored (re-sifted) at the end of input. The boundary is kept
/// across spills, so filtering remains effective while the in-memory
/// candidates are empty.
pub struct TransformPartialTopN<R: Rows> {
    candidates: TopNCandidates<R>,
    /// `Some` when the order column must be computed from payload columns and
    /// appended to the block.
    row_converter: Option<R::Converter>,
    sort_row_offset: usize,
    max_block_size: usize,

    memory_settings: MemorySettings,
    spill_schema: DataSchemaRef,
    location_prefix: String,
    writer_pool_bytes: usize,
    read_settings: ReadSettings,
    spilled: Vec<SpilledCandidates>,
    ctx: Arc<QueryContext>,
    /// The shared boundary filter and the payload offset of the (single)
    /// source sort column, from which boundary values are published.
    runtime_top_n_filter: Option<(usize, Arc<RuntimeTopNFilter>)>,
}

impl<R: Rows> TransformPartialTopN<R> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<QueryContext>,
        capacity: usize,
        row_converter: Option<R::Converter>,
        sort_row_offset: usize,
        max_block_size: usize,
        memory_settings: MemorySettings,
        spill_schema: DataSchemaRef,
        writer_pool_bytes: usize,
        read_settings: ReadSettings,
        runtime_top_n_filter: Option<(usize, Arc<RuntimeTopNFilter>)>,
    ) -> Self {
        Self {
            candidates: TopNCandidates::new(capacity, sort_row_offset),
            row_converter,
            sort_row_offset,
            max_block_size,
            memory_settings,
            spill_schema,
            location_prefix: ctx.query_id_spill_prefix(),
            writer_pool_bytes,
            read_settings,
            spilled: vec![],
            ctx,
            runtime_top_n_filter,
        }
    }

    fn publish_runtime_top_n_boundary(&mut self) {
        let Some((source_offset, filter)) = &self.runtime_top_n_filter else {
            return;
        };
        // The value is read from the source sort column of the boundary row,
        // keeping it in the raw column domain even when the order column is a
        // row encoding. `update` ignores null boundary values.
        let Some((block, row)) = self.candidates.take_tightened_boundary_row() else {
            return;
        };
        if let Some(value) = block.get_by_offset(*source_offset).index(row) {
            filter.update(&value.to_owned());
        }
    }

    /// Pull the tightest boundary published by any local stream, so row
    /// filtering benefits from other streams' progress as well.
    fn absorb_runtime_top_n_boundary(&mut self) {
        // Absorbed scalars enter the order-column domain, which matches the
        // source column only when no row converter is involved.
        if self.row_converter.is_some() {
            return;
        }
        if let Some(bound) = self
            .runtime_top_n_filter
            .as_ref()
            .and_then(|(_, filter)| filter.boundary())
        {
            self.candidates.tighten_boundary(bound);
        }
    }

    /// Write the consolidated candidates out as one sorted candidate file and
    /// release the memory. The boundary survives in the candidate set.
    fn spill_candidates(&mut self) -> Result<()> {
        let Some((block, _)) = self.candidates.finish() else {
            return Ok(());
        };
        if block.is_empty() {
            return Ok(());
        }

        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();

        let path = format!("{}/{}", self.location_prefix, GlobalUniq::unique());
        let mut writer =
            buffer_pool.writer(operator, path.clone(), self.writer_pool_bytes, target)?;
        writer.write(block.consume_convert_to_full())?;
        let (written, row_groups) = writer.close()?;

        self.ctx
            .add_spill_file(Location::Remote(path.clone()), Layout::Parquet, written);

        if !row_groups.is_empty() {
            self.spilled.push(SpilledCandidates { path, row_groups });
        }
        Ok(())
    }

    /// Restore spilled candidate files one by one and sift them back into the
    /// candidate set. Blocks read back are slices of a sorted candidate file,
    /// so the sorted admission path applies. No spilling happens here: the
    /// in-memory footprint stays bounded by the candidate capacity.
    fn restore_spilled_candidates(&mut self) -> Result<()> {
        if self.spilled.is_empty() {
            return Ok(());
        }

        let data_operator = DataOperator::instance();
        let target = SpillTarget::from_storage_params(data_operator.spill_params());
        let operator = data_operator.spill_operator();
        let buffer_pool = SpillsBufferPool::instance();

        for spilled in std::mem::take(&mut self.spilled) {
            self.absorb_runtime_top_n_boundary();
            let mut reader = buffer_pool.reader(
                operator.clone(),
                spilled.path,
                self.spill_schema.clone(),
                spilled.row_groups,
                target,
                self.read_settings,
            )?;

            while let Some(block) = reader.read()? {
                check_interrupt()?;
                let rows = R::from_column(&block.get_by_offset(self.sort_row_offset).to_column())?;
                self.candidates.sift_sorted(block, rows)?;
                self.publish_runtime_top_n_boundary();
            }
        }
        Ok(())
    }
}

impl<R: Rows> AccumulatingTransform for TransformPartialTopN<R>
where R::Converter: Send
{
    const NAME: &'static str = "TransformPartialTopN";

    fn transform(&mut self, mut block: DataBlock) -> Result<Vec<DataBlock>> {
        if block.is_empty() {
            return Ok(vec![]);
        }

        self.absorb_runtime_top_n_boundary();
        let rows = match &self.row_converter {
            Some(converter) => {
                let rows = converter.convert(&block)?;
                block.add_column(rows.to_column());
                rows
            }
            None => R::from_column(&block.get_by_offset(self.sort_row_offset).to_column())?,
        };

        self.candidates.sift_unsorted(block, rows)?;
        self.publish_runtime_top_n_boundary();

        if self.memory_settings.check_spill() {
            self.spill_candidates()?;
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output {
            return Ok(vec![]);
        }

        self.restore_spilled_candidates()?;

        let Some((block, _)) = self.candidates.finish() else {
            return Ok(vec![]);
        };
        Ok(split_block(block, self.max_block_size))
    }
}
