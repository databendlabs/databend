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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;

use crate::sessions::QueryContext;

pub struct TransformRecursiveCteScan {
    ctx: Arc<QueryContext>,
    cte_name: String,
}

impl TransformRecursiveCteScan {
    pub fn create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        cte_name: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output_port, TransformRecursiveCteScan {
            ctx,
            cte_name,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for TransformRecursiveCteScan {
    const NAME: &'static str = "RecursiveCteScan";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let data = self.ctx.get_recursive_cte_scan(&self.cte_name)?;
        if data.is_empty() {
            return Ok(None);
        }
        self.ctx.update_recursive_cte_scan(&self.cte_name, vec![])?;
        let data = DataBlock::concat(&data)?;
        if data.is_empty() {
            Ok(None)
        } else {
            Ok(Some(data))
        }
    }
}
