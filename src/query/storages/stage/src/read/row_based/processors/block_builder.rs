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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use log::debug;

use crate::read::block_builder_state::BlockBuilderState;
use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::RowBasedFileFormat;
use crate::read::row_based::format::RowDecoder;

pub struct BlockBuilder {
    pub ctx: Arc<LoadContext>,
    pub state: BlockBuilderState,
    pub decoder: Arc<dyn RowDecoder>,
}

impl BlockBuilder {
    pub fn create(ctx: Arc<LoadContext>, fmt: &Arc<dyn RowBasedFileFormat>) -> Result<Self> {
        let state = BlockBuilderState::create(ctx.clone());
        let decoder = fmt.try_create_decoder(ctx.clone())?;
        Ok(BlockBuilder {
            ctx,
            state,
            decoder,
        })
    }

    pub fn flush_block(&mut self, on_finish: bool) -> Result<Vec<DataBlock>> {
        let num_rows = self.state.num_rows;
        let columns = self.state.take_columns(on_finish)?;
        let internal_columns = self.state.take_internal_columns(on_finish)?;
        if columns.is_empty() && internal_columns.is_empty() || num_rows == 0 {
            Ok(vec![])
        } else {
            let mut columns = self.decoder.flush(columns, num_rows);
            columns.extend(internal_columns);
            Ok(vec![DataBlock::new_from_columns(columns)])
        }
    }
    pub fn try_flush_block_by_memory(&mut self) -> Result<Vec<DataBlock>> {
        let mem = self.state.memory_size();
        debug!(
            "chunk builder added new batch: row {} size {}",
            self.state.num_rows, mem
        );
        if self.state.num_rows >= self.ctx.block_compact_thresholds.min_rows_per_block
            || mem > self.ctx.block_compact_thresholds.max_bytes_per_block
        {
            self.flush_block(false)
        } else {
            Ok(vec![])
        }
    }
}

impl AccumulatingTransform for BlockBuilder {
    const NAME: &'static str = "BlockBuilder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let batch = data
            .get_owned_meta()
            .and_then(RowBatchWithPosition::downcast_from)
            .unwrap();
        if self.state.file_path != batch.start_pos.path {
            self.state.file_path = batch.start_pos.path.clone();
            self.state.file_full_path = format!("{}{}", self.ctx.stage_root, batch.start_pos.path)
        }
        let num_rows = self.state.num_rows;
        self.decoder.add(&mut self.state, batch)?;
        self.state
            .add_internals_columns_batch(self.state.num_rows - num_rows);
        self.state.flush_status(&self.ctx.table_context)?;
        let blocks = self.try_flush_block_by_memory()?;
        Ok(blocks)
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output {
            self.flush_block(true)
        } else {
            Ok(vec![])
        }
    }
}
