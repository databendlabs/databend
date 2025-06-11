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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::processors::transforms::transform_dictionary::DictionaryOperator;
use crate::sessions::QueryContext;
use crate::sql::executor::physical_plans::AsyncFunctionDesc;
use crate::sql::plans::AsyncFunctionArgument;

pub struct TransformAsyncFunction {
    ctx: Arc<QueryContext>,
    // key is the index of async_func_desc
    pub(crate) operators: BTreeMap<usize, Arc<DictionaryOperator>>,
    async_func_descs: Vec<AsyncFunctionDesc>,
}

impl TransformAsyncFunction {
    pub(crate) fn new(
        ctx: Arc<QueryContext>,
        async_func_descs: Vec<AsyncFunctionDesc>,
        operators: BTreeMap<usize, Arc<DictionaryOperator>>,
    ) -> Self {
        Self {
            ctx,
            async_func_descs,
            operators,
        }
    }

    // transform add sequence nextval column.
    async fn transform_sequence(
        &self,
        data_block: &mut DataBlock,
        sequence_name: &String,
        data_type: &DataType,
    ) -> Result<()> {
        transform_sequence(&self.ctx, data_block, sequence_name, data_type).await
    }
}

#[async_trait::async_trait]
impl AsyncTransform for TransformAsyncFunction {
    const NAME: &'static str = "AsyncFunction";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        for (i, async_func_desc) in self.async_func_descs.iter().enumerate() {
            match &async_func_desc.func_arg {
                AsyncFunctionArgument::SequenceFunction(sequence_name) => {
                    self.transform_sequence(
                        &mut data_block,
                        sequence_name,
                        &async_func_desc.data_type,
                    )
                    .await?;
                }
                AsyncFunctionArgument::DictGetFunction(dict_arg) => {
                    self.transform_dict_get(
                        i,
                        &mut data_block,
                        dict_arg,
                        &async_func_desc.arg_indices,
                        &async_func_desc.data_type,
                    )
                    .await?;
                }
            }
        }
        Ok(data_block)
    }
}

pub async fn transform_sequence(
    ctx: &Arc<QueryContext>,
    data_block: &mut DataBlock,
    sequence_name: &String,
    _data_type: &DataType,
) -> Result<()> {
    let count = data_block.num_rows() as u64;
    let column = if count == 0 {
        UInt64Type::from_data(vec![])
    } else {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_default_catalog()?;
        let req = GetSequenceNextValueReq {
            ident: SequenceIdent::new(&tenant, sequence_name),
            count,
        };
        let resp = catalog.get_sequence_next_value(req).await?;
        let range = resp.start..resp.start + count;
        UInt64Type::from_data(range.collect::<Vec<u64>>())
    };
    data_block.add_column(column);

    Ok(())
}
