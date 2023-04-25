// Copyright 2023 Datafuse Labs.
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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransform;
use opendal::Operator;

use crate::io::TableMetaLocationGenerator;
use crate::operations::mutation::VirtualColumnMeta;

pub struct VirtualColumnTransform {
    _ctx: Arc<dyn TableContext>,
    _dal: Operator,
    _location_gen: TableMetaLocationGenerator,
}

impl VirtualColumnTransform {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        location_gen: TableMetaLocationGenerator,
    ) -> Self {
        Self {
            _ctx: ctx,
            _dal: dal,
            _location_gen: location_gen,
        }
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for VirtualColumnTransform {
    const NAME: &'static str = "VirtualColumnTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        if let Some(_meta) = data
            .get_meta()
            .and_then(VirtualColumnMeta::downcast_ref_from)
        {
            // TODO
        }

        // no partial output
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        // TODO
        Ok(None)
    }
}
