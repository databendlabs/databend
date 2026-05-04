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

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::memory::basic::BasicHashJoin;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub struct CrossHashJoin {
    pub(crate) basic_hash_join: BasicHashJoin,
    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) basic_state: Arc<BasicHashJoinState>,
}

impl CrossHashJoin {
    pub fn create(
        ctx: &QueryContext,
        function_ctx: FunctionContext,
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        state: Arc<BasicHashJoinState>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let basic_hash_join = BasicHashJoin::create(
            &settings,
            function_ctx,
            method,
            desc.clone(),
            state.clone(),
            0,
        )?;

        Ok(CrossHashJoin {
            desc,
            basic_hash_join,
            basic_state: state,
        })
    }
}

impl Join for CrossHashJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.basic_hash_join.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        let Some(chunk_index) = self.basic_state.steal_chunk_index() else {
            return Ok(None);
        };

        let chunk = &self.basic_state.chunks[chunk_index];
        Ok(Some(ProgressValues {
            rows: chunk.num_rows(),
            bytes: chunk.memory_size(),
        }))
    }

    fn build_runtime_filter(&self) -> Result<JoinRuntimeFilterPacket> {
        Ok(JoinRuntimeFilterPacket::default())
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || *self.basic_state.build_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }

        self.basic_hash_join.finalize_chunks();
        let build_block =
            DataBlock::concat(&self.basic_state.chunks)?.project(&self.desc.build_projection);
        let mut probe_block = data.project(&self.desc.probe_projection);
        let build_rows = build_block.num_rows();

        if build_rows == 1 {
            for col in build_block.columns() {
                let scalar = unsafe { col.index_unchecked(0) };
                probe_block.add_const_column(scalar.to_owned(), col.data_type());
            }
            return Ok(Box::new(OneBlockJoinStream(Some(probe_block))));
        }

        Ok(Box::new(CrossHashJoinStream {
            probe_row: 0,
            build_block,
            probe_block,
        }))
    }
}

struct CrossHashJoinStream {
    probe_row: usize,
    probe_block: DataBlock,
    build_block: DataBlock,
}

impl JoinStream for CrossHashJoinStream {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        if self.probe_row >= self.probe_block.num_rows() {
            return Ok(None);
        }

        let build_rows = self.build_block.num_rows();
        let columns =
            Vec::with_capacity(self.probe_block.num_columns() + self.build_block.num_columns());
        let mut replicated_probe_block = DataBlock::new(columns, build_rows);

        for col in self.probe_block.columns() {
            let scalar = unsafe { col.index_unchecked(self.probe_row) };
            replicated_probe_block.add_const_column(scalar.to_owned(), col.data_type());
        }
        replicated_probe_block.merge_block(self.build_block.clone());

        self.probe_row += 1;
        Ok(Some(replicated_probe_block))
    }
}
