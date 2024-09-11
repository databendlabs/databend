// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono_tz::Tz;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_storage::build_operator;
use opendal::services::Redis;
use sqlx::MySqlPool;

use crate::pipelines::processors::transforms::TransformAsyncFunction;
use crate::sql::executor::physical_plans::AsyncFunctionDesc;
use crate::sql::plans::AsyncFunctionArgument;
use crate::sql::plans::DictGetFunctionArgument;
use crate::sql::plans::DictionarySource;
use crate::sql::IndexType;

pub(crate) enum DictionaryOperator {
    Operator(Operator),
    Mysql((MySqlPool, String)),
}

impl DictionaryOperator {
    fn format_key(&self, key: ScalarRef<'_>) -> String {
        match key {
            ScalarRef::String(s) => s.to_string(),
            ScalarRef::Date(d) => format!("{}", date_to_string(d as i64, Tz::UTC)),
            ScalarRef::Timestamp(t) => format!("{}", timestamp_to_string(t, Tz::UTC)),
            _ => format!("{}", key),
        }
    }

    async fn dict_get(&self, key: ScalarRef<'_>, data_type: &DataType) -> Result<Option<Scalar>> {
        if key == ScalarRef::Null {
            return Ok(None);
        }
        match self {
            DictionaryOperator::Operator(op) => {
                if let ScalarRef::String(key) = key {
                    let buffer = op.read(key).await;
                    match buffer {
                        Ok(res) => {
                            let value =
                                unsafe { String::from_utf8_unchecked(res.current().to_vec()) };
                            Ok(Some(Scalar::String(value)))
                        }
                        Err(e) => {
                            if e.kind() == opendal::ErrorKind::NotFound {
                                Ok(None)
                            } else {
                                Err(ErrorCode::DictionarySourceError(format!(
                                    "dictionary source error: {e}"
                                )))
                            }
                        }
                    }
                } else {
                    Ok(None)
                }
            }
            DictionaryOperator::Mysql((pool, sql)) => match data_type.remove_nullable() {
                DataType::Boolean => {
                    let value: Option<bool> = sqlx::query_scalar(&sql)
                        .bind(self.format_key(key))
                        .fetch_optional(pool)
                        .await?;
                    Ok(value.map(|v| Scalar::Boolean(v)))
                }
                DataType::String => {
                    let value: Option<String> = sqlx::query_scalar(&sql)
                        .bind(self.format_key(key))
                        .fetch_optional(pool)
                        .await?;
                    Ok(value.map(|v| Scalar::String(v)))
                }
                DataType::Number(num_ty) => {
                    with_integer_mapped_type!(|NUM_TYPE| match num_ty {
                        NumberDataType::NUM_TYPE => {
                            let value: Option<NUM_TYPE> = sqlx::query_scalar(&sql)
                                .bind(self.format_key(key))
                                .fetch_optional(pool)
                                .await?;
                            Ok(value.map(|v| Scalar::Number(NUM_TYPE::upcast_scalar(v))))
                        }
                        NumberDataType::Float32 => {
                            let value: Option<f32> = sqlx::query_scalar(&sql)
                                .bind(self.format_key(key))
                                .fetch_optional(pool)
                                .await?;
                            Ok(value.map(|v| Scalar::Number(NumberScalar::Float32(v.into()))))
                        }
                        NumberDataType::Float64 => {
                            let value: Option<f64> = sqlx::query_scalar(&sql)
                                .bind(self.format_key(key))
                                .fetch_optional(pool)
                                .await?;
                            Ok(value.map(|v| Scalar::Number(NumberScalar::Float64(v.into()))))
                        }
                    })
                }
                DataType::Date => {
                    let value: Option<i32> = sqlx::query_scalar(&sql)
                        .bind(self.format_key(key))
                        .fetch_optional(pool)
                        .await?;
                    Ok(value.map(|v| Scalar::Date(v)))
                }
                DataType::Timestamp => {
                    let value: Option<i64> = sqlx::query_scalar(&sql)
                        .bind(self.format_key(key))
                        .fetch_optional(pool)
                        .await?;
                    Ok(value.map(|v| Scalar::Timestamp(v)))
                }
                _ => Err(ErrorCode::DictionarySourceError(format!(
                    "unsupported value type {data_type}"
                ))),
            },
        }
    }
}

impl TransformAsyncFunction {
    pub(crate) fn init_operators(
        async_func_descs: &[AsyncFunctionDesc],
    ) -> Result<BTreeMap<usize, Arc<DictionaryOperator>>> {
        let mut operators = BTreeMap::new();
        for (i, async_func_desc) in async_func_descs.iter().enumerate() {
            if let AsyncFunctionArgument::DictGetFunction(dict_arg) = &async_func_desc.func_arg {
                match &dict_arg.dict_source {
                    DictionarySource::Redis(redis_source) => {
                        let mut builder = Redis::default().endpoint(&redis_source.connection_url);
                        if let Some(ref username) = redis_source.username {
                            builder = builder.username(username);
                        }
                        if let Some(ref password) = redis_source.password {
                            builder = builder.password(password);
                        }
                        if let Some(db_index) = redis_source.db_index {
                            builder = builder.db(db_index);
                        }
                        let op = build_operator(builder)?;
                        operators.insert(i, Arc::new(DictionaryOperator::Operator(op)));
                    }
                    DictionarySource::Mysql(sql_source) => {
                        let mysql_pool = databend_common_base::runtime::block_on(
                            sqlx::MySqlPool::connect(&sql_source.connection_url),
                        )?;
                        let sql = format!(
                            "SELECT {} FROM {} WHERE {} = ? LIMIT 1",
                            &sql_source.value_field, &sql_source.table, &sql_source.key_field
                        );
                        operators.insert(i, Arc::new(DictionaryOperator::Mysql((mysql_pool, sql))));
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
        let op: &Arc<DictionaryOperator> = self.operators.get(&i).unwrap();
        // only support one key field.
        let arg_index = arg_indices[0];
        let entry = data_block.get_by_offset(arg_index);
        let value = match &entry.value {
            Value::Scalar(scalar) => {
                let value = op
                    .dict_get(scalar.as_ref(), data_type)
                    .await?
                    .unwrap_or(dict_arg.default_value.clone());
                Value::Scalar(value)
            }
            Value::Column(column) => {
                let mut builder = ColumnBuilder::with_capacity(data_type, column.len());
                for scalar_ref in column.iter() {
                    let value = op
                        .dict_get(scalar_ref, data_type)
                        .await?
                        .unwrap_or(dict_arg.default_value.clone());
                    builder.push(value.as_ref());
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
