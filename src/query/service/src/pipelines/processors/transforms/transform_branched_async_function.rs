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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_expression::SourceSchemaIndex;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_storages_fuse::TableContext;

use super::transform_cast_schema::cast_schema;
use crate::pipelines::processors::transforms::transform_async_function::transform_sequence;
use crate::sessions::QueryContext;
use crate::sql::executor::physical_plans::AsyncFunctionDesc;
use crate::sql::plans::AsyncFunctionArgument;

/// The key of branches is `SourceSchemaIndex`, see `TransformResortAddOnWithoutSourceSchema`.
pub struct TransformBranchedAsyncFunction {
    pub ctx: Arc<QueryContext>,
    pub branches: Arc<HashMap<SourceSchemaIndex, Branch>>,
}

pub struct Branch {
    pub async_func_descs: Vec<AsyncFunctionDesc>,
    pub to_schema: DataSchemaRef,
    pub from_schema: DataSchemaRef,
    pub exprs: Vec<Expr>,
}

#[async_trait::async_trait]
impl AsyncTransform for TransformBranchedAsyncFunction {
    const NAME: &'static str = "BranchedAsyncFunction";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        // see the comment details of `TransformResortAddOnWithoutSourceSchema`.
        if block.get_meta().is_none() {
            return Ok(block);
        }
        let input_schema_idx =
            SourceSchemaIndex::downcast_from(block.clone().get_owned_meta().unwrap()).unwrap();
        let Some(branch) = self.branches.get(&input_schema_idx) else {
            // no async function to execute, just return the original block
            return Ok(block);
        };

        let Branch {
            async_func_descs,
            to_schema,
            from_schema,
            exprs,
        } = branch;

        for async_func_desc in async_func_descs.iter() {
            match &async_func_desc.func_arg {
                AsyncFunctionArgument::SequenceFunction(sequence_name) => {
                    transform_sequence(
                        &self.ctx,
                        &mut block,
                        sequence_name,
                        &async_func_desc.data_type,
                    )
                    .await?;
                }
                AsyncFunctionArgument::DictGetFunction(_) => unreachable!(),
            }
        }
        cast_schema(
            block,
            from_schema.clone(),
            to_schema.clone(),
            exprs,
            &self.ctx.get_function_context()?,
        )
    }
}
