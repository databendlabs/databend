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

use core::str;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use databend_common_exception::Result;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_sql::plans::DictGetFunctionArgument;
use databend_common_sql::plans::DictionarySource;
use databend_common_storages_fuse::TableContext;
use lazy_static::lazy_static;
use opendal::services::Redis;
use opendal::Operator;

use crate::sessions::QueryContext;
use crate::sql::executor::physical_plans::AsyncFunctionDesc;
use crate::sql::plans::AsyncFunctionArgument;
use crate::sql::IndexType;

pub struct TransformAsyncFunction {
    ctx: Arc<QueryContext>,
    async_func_descs: Vec<AsyncFunctionDesc>,
}

lazy_static!(
    static ref OPERATORS: Arc<RwLock<HashMap<String, Operator>>> = Arc::new(RwLock::new(HashMap::new()));
    static ref CACHE: Arc<RwLock<HashMap<String, String>>> = Arc::new(RwLock::new(HashMap::new()));
);

impl TransformAsyncFunction {
    pub fn new(ctx: Arc<QueryContext>, async_func_descs: Vec<AsyncFunctionDesc>) -> Self {
        Self {
            ctx,
            async_func_descs,
        }
    }

    // transform add sequence nextval column.
    async fn transform_sequence(
        &self,
        data_block: &mut DataBlock,
        sequence_name: &String,
        data_type: &DataType,
    ) -> Result<()> {
        let count = data_block.num_rows() as u64;
        let value = if count == 0 {
            UInt64Type::from_data(vec![])
        } else {
            let tenant = self.ctx.get_tenant();
            let catalog = self.ctx.get_default_catalog()?;
            let req = GetSequenceNextValueReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
                count,
            };
            let resp = catalog.get_sequence_next_value(req).await?;
            let range = resp.start..resp.start + count;
            UInt64Type::from_data(range.collect::<Vec<u64>>())
        };
        let entry = BlockEntry {
            data_type: data_type.clone(),
            value: Value::Column(value),
        };
        data_block.add_column(entry);

        Ok(())
    }

    // transform add dict get column.
    async fn transform_dict_get(
        &self,
        data_block: &mut DataBlock,
        dict_arg: &DictGetFunctionArgument,
        arg_indices: &Vec<IndexType>,
        data_type: &DataType,
    ) -> Result<()> {
        let op = match &dict_arg.dict_source {
            DictionarySource::Redis(conn_str) => {
                let mut redis_operator = OPERATORS.write().unwrap();
                if let Some(op) = redis_operator.get(conn_str) {
                    op.clone()
                } else {
                    let builder = Redis::default().endpoint(&conn_str);
                    let op = Operator::new(builder)?.finish();
                    redis_operator.insert(conn_str.clone(), op.clone());
                    op.clone()
                }
            }
            _ => {
                todo!()
            }
        };
        // only support one key field.
        let arg_index = arg_indices[0];

        let entry = data_block.get_by_offset(arg_index);
        let value = match &entry.value {
            Value::Scalar(scalar) => {
                if let Scalar::String(key) = scalar {
                    let cache = CACHE.read().unwrap().clone();
                    if let Some(cached_val) = cache.get(key) {
                        Value::Scalar(Scalar::String(cached_val.clone()))
                    } else {
                        drop(cache);
                        let res = op.read(&key).await?;
                        let val = String::from_utf8((&res.current()).to_vec()).unwrap();
                        let mut cache = CACHE.write().unwrap();
                        cache.insert(key.clone(), val.clone());
                        Value::Scalar(Scalar::String(val))
                    }
                } else {
                    Value::Scalar(Scalar::String("".to_string()))
                }
            }
            Value::Column(column) => {
                let mut builder = StringColumnBuilder::with_capacity(column.len(), 0);
                for scalar in column.iter() {
                    let cache = CACHE.read().unwrap().clone();
                    if let ScalarRef::String(key) = scalar {
                        let value = if let Some(cached_val) = cache.get(key) {
                            cached_val.clone()
                        } else {
                            drop(cache);
                            let res = op.read(&key).await?;
                            let val = String::from_utf8((&res.current()).to_vec()).unwrap();
                            let mut cache = CACHE.write().unwrap();
                            cache.insert(key.to_string(), val.clone());
                            val
                        };
                        builder.put_str(value.as_str());
                    }
                    builder.commit_row();
                }
                Value::Column(Column::String(builder.build()))
            }
        };
        let entry = BlockEntry {
            data_type: data_type.clone(),
            value,
        };
        data_block.add_column(entry);
    
        Ok(())
    }   
}

#[async_trait::async_trait]
impl AsyncTransform for TransformAsyncFunction {
    const NAME: &'static str = "AsyncFunction";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        for async_func_desc in &self.async_func_descs {
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
