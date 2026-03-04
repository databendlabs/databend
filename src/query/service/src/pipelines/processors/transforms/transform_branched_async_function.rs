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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::SourceSchemaIndex;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_sql::binder::AsyncFunctionDesc;

use crate::pipelines::processors::transforms::AutoIncrementNextValFetcher;
use crate::pipelines::processors::transforms::ReadFileContext;
use crate::pipelines::processors::transforms::SequenceCounters;
use crate::pipelines::processors::transforms::SequenceNextValFetcher;
use crate::pipelines::processors::transforms::TransformAsyncFunction;
use crate::sessions::QueryContext;
use crate::sql::plans::AsyncFunctionArgument;

/// The key of branches is `SourceSchemaIndex`, see `TransformResortAddOnWithoutSourceSchema`.
pub struct TransformBranchedAsyncFunction {
    pub ctx: Arc<QueryContext>,
    pub branches: Arc<HashMap<SourceSchemaIndex, AsyncFunctionBranch>>,
    pub read_file_ctx: ReadFileContext,
}

pub struct AsyncFunctionBranch {
    pub async_func_descs: Vec<AsyncFunctionDesc>,
    pub sequence_counters: SequenceCounters,
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
            // no async function to execute in this branch, just return the original block
            return Ok(block);
        };

        let AsyncFunctionBranch {
            async_func_descs,
            sequence_counters,
        } = branch;

        for (i, async_func_desc) in async_func_descs.iter().enumerate() {
            match &async_func_desc.func_arg {
                AsyncFunctionArgument::SequenceFunction(sequence_name) => {
                    let counter_lock = sequence_counters[i].clone();
                    TransformAsyncFunction::transform(
                        self.ctx.clone(),
                        &mut block,
                        counter_lock,
                        SequenceNextValFetcher {
                            sequence_ident: SequenceIdent::new(
                                self.ctx.get_tenant(),
                                sequence_name.clone(),
                            ),
                        },
                    )
                    .await?;
                }
                AsyncFunctionArgument::AutoIncrement { key, expr } => {
                    let counter_lock = sequence_counters[i].clone();
                    TransformAsyncFunction::transform(
                        self.ctx.clone(),
                        &mut block,
                        counter_lock,
                        AutoIncrementNextValFetcher {
                            key: key.clone(),
                            expr: expr.clone(),
                        },
                    )
                    .await?;
                }
                AsyncFunctionArgument::DictGetFunction(_) => unreachable!(),
                AsyncFunctionArgument::ReadFile(read_file_arg) => {
                    self.read_file_ctx
                        .transform_read_file(
                            self.ctx.clone(),
                            &mut block,
                            &async_func_desc.arg_indices,
                            &async_func_desc.data_type,
                            read_file_arg,
                        )
                        .await?;
                }
            }
        }
        Ok(block)
    }
}
