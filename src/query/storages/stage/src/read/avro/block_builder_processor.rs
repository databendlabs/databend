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
use databend_common_meta_app::principal::AvroFileFormatParams;
use databend_common_pipeline_transforms::AccumulatingTransform;

use crate::read::avro::decoder::AvroDecoder;
use crate::read::block_builder_state::BlockBuilderState;
use crate::read::load_context::LoadContext;
use crate::read::whole_file_reader::WholeFileData;

pub struct BlockBuilderProcessor {
    pub ctx: Arc<LoadContext>,
    pub state: BlockBuilderState,
    pub decoder: AvroDecoder,
}

impl BlockBuilderProcessor {
    pub(crate) fn create(
        ctx: Arc<LoadContext>,
        format_params: AvroFileFormatParams,
    ) -> Result<Self> {
        Ok(Self {
            decoder: AvroDecoder::new(ctx.clone(), format_params),
            state: BlockBuilderState::create(ctx.clone()),
            ctx,
        })
    }
}

impl AccumulatingTransform for BlockBuilderProcessor {
    const NAME: &'static str = "BlockBuilder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let data = data
            .get_owned_meta()
            .and_then(WholeFileData::downcast_from)
            .unwrap();
        self.state.file_path = data.path.clone();
        self.state.file_full_path = format!("{}{}", self.ctx.stage_root, data.path);
        self.decoder.add(&mut self.state, data)?;

        self.state.flush_status(&self.ctx.table_context)?;
        let blocks = self.state.try_flush_block_by_memory(&self.ctx)?;
        Ok(blocks)
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if output {
            self.state.flush_block(true)
        } else {
            Ok(vec![])
        }
    }
}
