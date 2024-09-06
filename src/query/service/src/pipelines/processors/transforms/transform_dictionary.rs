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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_storage::build_operator;
use opendal::services::Redis;
use opendal::Operator;

use crate::pipelines::processors::transforms::TransformAsyncFunction;
use crate::sql::executor::physical_plans::AsyncFunctionDesc;
use crate::sql::plans::AsyncFunctionArgument;
use crate::sql::plans::DictGetFunctionArgument;
use crate::sql::plans::DictionarySource;
use crate::sql::IndexType;

impl TransformAsyncFunction {
    pub fn init_operators(
        async_func_descs: &[AsyncFunctionDesc],
    ) -> Result<BTreeMap<usize, Arc<Operator>>> {
        let mut operators = BTreeMap::new();
        for (i, async_func_desc) in async_func_descs.iter().enumerate() {
            if let AsyncFunctionArgument::DictGetFunction(dict_arg) = &async_func_desc.func_arg {
                match &dict_arg.dict_source {
                    DictionarySource::Redis(redis_source) => {
                        let mut builder = Redis::default().endpoint(&redis_source.connection_url);
                        if let Some(username) = redis_source.username.clone() {
                            builder = builder.username(&username);
                        }
                        if let Some(password) = redis_source.password.clone() {
                            builder = builder.password(&password);
                        }
                        if let Some(db_index) = redis_source.db_index {
                            builder = builder.db(db_index);
                        }
                        let op = build_operator(builder)?;
                        operators.insert(i, Arc::new(op));
                    }
                    DictionarySource::Mysql(_) => {
                        return Err(ErrorCode::Unimplemented("Mysql source is unsupported"));
                    }
                }
            }
        }
        Ok(operators)
    }

    // transform add dict get column.
    pub(crate) async fn transform_dict_get(
        &self,
        i: usize,
        data_block: &mut DataBlock,
        dict_arg: &DictGetFunctionArgument,
        arg_indices: &[IndexType],
        data_type: &DataType,
    ) -> Result<()> {
        let op = self.operators.get(&i).unwrap().clone();

        // only support one key field.
        let arg_index = arg_indices[0];
        let entry = data_block.get_by_offset(arg_index);
        let value = match &entry.value {
            Value::Scalar(scalar) => {
                if let Scalar::String(key) = scalar {
                    let buffer = op.read(key).await;
                    match buffer {
                        Ok(res) => {
                            let value =
                                unsafe { String::from_utf8_unchecked(res.current().to_vec()) };
                            Value::Scalar(Scalar::String(value))
                        }
                        Err(_) => Value::Scalar(dict_arg.default_value.clone()),
                    }
                } else {
                    Value::Scalar(dict_arg.default_value.clone())
                }
            }
            Value::Column(column) => {
                let mut builder = ColumnBuilder::with_capacity(data_type, column.len());
                for scalar in column.iter() {
                    if let ScalarRef::String(key) = scalar {
                        let buffer = op.read(key).await;
                        match buffer {
                            Ok(res) => {
                                let value =
                                    unsafe { String::from_utf8_unchecked(res.current().to_vec()) };
                                builder.push(ScalarRef::String(value.as_str()));
                            }
                            Err(_) => {
                                builder.push(dict_arg.default_value.as_ref());
                            }
                        };
                    } else {
                        builder.push(dict_arg.default_value.as_ref());
                    }
                }
                Value::Column(builder.build())
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
