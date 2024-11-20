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
use std::collections::HashMap;
use std::string::String;
use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use jiff::tz::TimeZone;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use redis::Client;
use sqlx::MySqlPool;

use crate::pipelines::processors::transforms::TransformAsyncFunction;
use crate::sql::executor::physical_plans::AsyncFunctionDesc;
use crate::sql::plans::AsyncFunctionArgument;
use crate::sql::plans::DictGetFunctionArgument;
use crate::sql::plans::DictionarySource;
use crate::sql::IndexType;

pub(crate) enum DictionaryOperator {
    Redis(ConnectionManager),
    Mysql((MySqlPool, String)),
}

impl DictionaryOperator {
    async fn dict_get(
        &self,
        value: &Value<AnyType>,
        data_type: &DataType,
        default_value: &Scalar,
    ) -> Result<Value<AnyType>> {
        match self {
            DictionaryOperator::Redis(connection) => match value {
                Value::Scalar(scalar) => match scalar {
                    Scalar::String(str) => {
                        self.get_scalar_value_from_redis(str, connection, data_type, default_value)
                            .await
                    }
                    Scalar::Null => Ok(Value::Scalar(default_value.clone())),
                    _ => Err(ErrorCode::DictionarySourceError(format!(
                        "Redis dictionary operator currently does not support value type {}",
                        scalar.as_ref().infer_data_type(),
                    ))),
                },
                Value::Column(column) => {
                    let (_, validity) = column.validity();
                    let column =
                        StringType::try_downcast_column(&column.remove_nullable()).unwrap();
                    self.get_column_values_from_redis(
                        &column,
                        validity,
                        data_type,
                        connection,
                        default_value,
                    )
                    .await
                }
            },
            DictionaryOperator::Mysql((pool, sql)) => match value {
                Value::Scalar(scalar) => {
                    let value = self
                        .get_data_from_mysql(scalar.as_ref(), data_type, pool, sql)
                        .await?
                        .unwrap_or(default_value.clone());
                    Ok(Value::Scalar(value))
                }
                Value::Column(column) => {
                    let mut builder = ColumnBuilder::with_capacity(data_type, column.len());
                    for scalar_ref in column.iter() {
                        let value = self
                            .get_data_from_mysql(scalar_ref, data_type, pool, sql)
                            .await?
                            .unwrap_or(default_value.clone());
                        builder.push(value.as_ref());
                    }
                    Ok(Value::Column(builder.build()))
                }
            },
        }
    }

    async fn get_scalar_value_from_redis(
        &self,
        key: &String,
        connection: &ConnectionManager,
        data_type: &DataType,
        default_value: &Scalar,
    ) -> Result<Value<AnyType>> {
        let mut conn = connection.clone();
        let redis_val: redis::Value = conn.get(key).await.unwrap();
        let res = Self::from_redis_value_to_scalar(&redis_val, data_type, default_value)?;
        match res {
            Scalar::String(str) => Ok(Value::Scalar(Scalar::String(str))),
            _ => unreachable!(),
        }
    }

    async fn get_column_values_from_redis(
        &self,
        str_col: &StringColumn,
        validity: Option<&Bitmap>,
        data_type: &DataType,
        connection: &ConnectionManager,
        default_value: &Scalar,
    ) -> Result<Value<AnyType>> {
        // step-1: deduplicate the keys in the column.
        let key_cnt = str_col.len();
        let mut keys: Vec<&str> = vec![];
        let mut key_map = HashMap::new();
        for key in str_col.option_iter(validity).flatten() {
            if !key_map.contains_key(key) {
                keys.push(key);
                let index = key_map.len();
                key_map.insert(key, index);
            }
        }

        // step-2: get the values from redis via mget.
        let mut builder = ColumnBuilder::with_capacity(data_type, key_cnt);
        if keys.is_empty() {
            // keys in the column only have null.
            for _ in 0..key_cnt {
                builder.push(default_value.as_ref());
            }
        } else {
            let mut conn = connection.clone();
            let redis_val: redis::Value = conn.get(keys).await.unwrap();
            let res = Self::from_redis_value_to_scalar(&redis_val, data_type, default_value)?;
            match res {
                Scalar::Array(arr) => {
                    for key in str_col.option_iter(validity) {
                        if let Some(key) = key {
                            let index = key_map[key];
                            let val = unsafe { arr.index_unchecked(index) };
                            builder.push(val);
                        } else {
                            builder.push(default_value.as_ref());
                        }
                    }
                }
                Scalar::String(str) => {
                    let val = Scalar::String(str);
                    for key in str_col.option_iter(validity) {
                        if let Some(_key) = key {
                            builder.push(val.as_ref());
                        } else {
                            builder.push(default_value.as_ref());
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
        Ok(Value::Column(builder.build()))
    }

    #[inline]
    fn from_redis_value_to_scalar(
        rv: &redis::Value,
        data_type: &DataType,
        default_value: &Scalar,
    ) -> Result<Scalar> {
        match rv {
            redis::Value::BulkString(bs) => {
                let str = unsafe { String::from_utf8_unchecked(bs.to_vec()) };
                Ok(Scalar::String(str))
            }
            redis::Value::Array(arr) => {
                let mut builder = ColumnBuilder::with_capacity(data_type, 1);
                for item in arr {
                    let scalar = Self::from_redis_value_to_scalar(item, data_type, default_value)?;
                    builder.push(scalar.as_ref());
                }
                Ok(Scalar::Array(builder.build()))
            }
            redis::Value::Nil => Ok(default_value.clone()),
            _ => unreachable!(),
        }
    }

    async fn get_data_from_mysql(
        &self,
        key: ScalarRef<'_>,
        data_type: &DataType,
        pool: &MySqlPool,
        sql: &String,
    ) -> Result<Option<Scalar>> {
        if key == ScalarRef::Null {
            return Ok(None);
        }
        match data_type.remove_nullable() {
            DataType::Boolean => {
                let value: Option<bool> = sqlx::query_scalar(sql)
                    .bind(self.format_key(key))
                    .fetch_optional(pool)
                    .await?;
                Ok(value.map(Scalar::Boolean))
            }
            DataType::String => {
                let value: Option<String> = sqlx::query_scalar(sql)
                    .bind(self.format_key(key))
                    .fetch_optional(pool)
                    .await?;
                Ok(value.map(Scalar::String))
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
                        let value: Option<f32> = sqlx::query_scalar(sql)
                            .bind(self.format_key(key))
                            .fetch_optional(pool)
                            .await?;
                        Ok(value.map(|v| Scalar::Number(NumberScalar::Float32(v.into()))))
                    }
                    NumberDataType::Float64 => {
                        let value: Option<f64> = sqlx::query_scalar(sql)
                            .bind(self.format_key(key))
                            .fetch_optional(pool)
                            .await?;
                        Ok(value.map(|v| Scalar::Number(NumberScalar::Float64(v.into()))))
                    }
                })
            }
            _ => Err(ErrorCode::DictionarySourceError(format!(
                "MySQL dictionary operator currently does not support value type {data_type}"
            ))),
        }
    }

    fn format_key(&self, key: ScalarRef<'_>) -> String {
        match key {
            ScalarRef::String(s) => s.to_string(),
            ScalarRef::Date(d) => format!("{}", date_to_string(d as i64, &TimeZone::UTC)),
            ScalarRef::Timestamp(t) => {
                format!("{}", timestamp_to_string(t, &TimeZone::UTC))
            }
            _ => format!("{}", key),
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
                        let client = Client::open(redis_source.connection_url.clone())?;
                        let conn = databend_common_base::runtime::block_on(
                            ConnectionManager::new(client),
                        )?;
                        operators.insert(i, Arc::new(DictionaryOperator::Redis(conn)));
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
        let default_value = dict_arg.default_value.clone();
        let value = op.dict_get(&entry.value, data_type, &default_value).await?;
        let entry = BlockEntry {
            data_type: data_type.clone(),
            value,
        };
        data_block.add_column(entry);

        Ok(())
    }
}
