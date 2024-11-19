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
use std::collections::HashSet;
use std::string::String;
use std::sync::Arc;

use chrono_tz::Tz;
use databend_common_arrow::arrow::bitmap::Bitmap;
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
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::DictionaryCache;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use redis::Client;
use regex::Regex;
use sqlx::MySqlPool;

use crate::pipelines::processors::transforms::TransformAsyncFunction;
use crate::sql::executor::physical_plans::AsyncFunctionDesc;
use crate::sql::plans::AsyncFunctionArgument;
use crate::sql::plans::DictGetFunctionArgument;
use crate::sql::plans::DictionarySource;
use crate::sql::IndexType;

pub(crate) enum DictionaryOperator {
    Redis((ConnectionManager, DictionaryCache, String)),
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
            DictionaryOperator::Redis((connection, redis_cache, cache_key_prefix)) => match value {
                Value::Scalar(scalar) => match scalar {
                    Scalar::String(str) => {
                        self.get_scalar_value_from_redis(
                            str,
                            connection,
                            redis_cache,
                            cache_key_prefix,
                            default_value,
                        )
                        .await
                    }
                    Scalar::Null => Ok(Value::Scalar(default_value.clone())),
                    _ => Err(ErrorCode::DictionarySourceError(format!(
                        "Redis dictionary operator currently does not support value type {}",
                        scalar.as_ref().infer_data_type(),
                    ))),
                },
                Value::Column(column) => match column {
                    Column::Nullable(box nullable_col) => match &nullable_col.column {
                        Column::String(str_col) => {
                            self.get_column_values_from_redis(
                                str_col,
                                &Some(nullable_col.validity.clone()),
                                data_type,
                                connection,
                                redis_cache,
                                cache_key_prefix,
                                default_value,
                            )
                            .await
                        }
                        _ => Err(ErrorCode::DictionarySourceError(format!(
                            "Redis dictionary operator currently does not support value type {}",
                            column.data_type()
                        ))),
                    },
                    Column::String(str_col) => {
                        self.get_column_values_from_redis(
                            str_col,
                            &None,
                            data_type,
                            connection,
                            redis_cache,
                            cache_key_prefix,
                            default_value,
                        )
                        .await
                    }
                    _ => Err(ErrorCode::DictionarySourceError(format!(
                        "Redis dictionary operator currently does not support value type {}",
                        column.data_type()
                    ))),
                },
            },
            DictionaryOperator::Mysql((pool, sql)) => match value {
                Value::Scalar(scalar) => {
                    let value = self
                        .get_data_from_mysql(scalar.as_ref(), &data_type, &pool, &sql)
                        .await?
                        .unwrap_or(default_value.clone());
                    Ok(Value::Scalar(value))
                }
                Value::Column(column) => {
                    let mut builder = ColumnBuilder::with_capacity(data_type, column.len());
                    for scalar_ref in column.iter() {
                        let value = self
                            .get_data_from_mysql(scalar_ref, &data_type, &pool, &sql)
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
        redis_cache: &DictionaryCache,
        cache_key_prefix: &String,
        default_value: &Scalar,
    ) -> Result<Value<AnyType>> {
        let cache_key = format!("{}-{}", cache_key_prefix, key);
        if redis_cache.contains_key(&cache_key) {
            let val = redis_cache.get(cache_key.as_str()).unwrap();
            Ok(Value::Scalar((*val).clone()))
        } else {
            let mut conn = connection.clone();
            let redis_val: redis::Value = conn.get(key).await.unwrap();
            let res = self.from_redis_value_to_scalar(&redis_val, &default_value)?;
            match res {
                Scalar::String(str) => {
                    redis_cache.insert(cache_key, Scalar::String(str.clone()));
                    Ok(Value::Scalar(Scalar::String(str)))
                }
                _ => unreachable!(),
            }
        }
    }

    async fn get_column_values_from_redis(
        &self,
        str_col: &StringColumn,
        validity: &Option<Bitmap>,
        data_type: &DataType,
        connection: &ConnectionManager,
        redis_cache: &DictionaryCache,
        cache_key_prefix: &String,
        default_value: &Scalar,
    ) -> Result<Value<AnyType>> {
        // step-1: deduplicate the keys in the column.
        let vali_bitmap = validity.clone().unwrap();
        let vali_none = validity.is_none();
        let key_cnt = str_col.len();
        let mut keys: Vec<&str> = vec![];
        let mut cache_keys: Vec<String> = vec![];
        let mut key_set = HashSet::new();
        let mut kv_map = HashMap::new();
        for i in 0..key_cnt {
            if vali_none == true || vali_bitmap.get_bit(i) == true {
                let key = unsafe { str_col.index_unchecked(i) };
                if key_set.contains(key) == false {
                    keys.push(key);
                    let cache_key = cache_key_prefix.clone() + "-" + key;
                    cache_keys.push(cache_key);
                    key_set.insert(key);
                }
            }
        }

        // step-2: collect keys in the cache, and push the corresponding kv-pairs to `kv_map`.
        let mut key_pos = 0 as usize;
        let values = match redis_cache.len() {
            0 => {
                // TODO: cache is not empty, but the data in cache is all expired.
                key_pos = key_cnt;
                vec![]
            }
            _ => redis_cache.get_batch(cache_keys),
        };
        for i in 0..values.len() {
            match values[i].clone() {
                None => {
                    keys[key_pos] = keys[i];
                    key_pos += 1;
                }
                Some(val) => {
                    kv_map.insert(keys[i], val.clone());
                }
            }
        }

        // step-3: get the values from redis via mget.
        let mut builder = ColumnBuilder::with_capacity(data_type, key_cnt);
        let all_in_cache = values.len() != 0 && key_pos == 0;
        let all_out_of_cache = values.len() == 0;
        if all_in_cache == false {
            keys.truncate(key_pos);
            let mut conn = connection.clone();
            let redis_val: redis::Value = conn.get(keys.clone()).await.unwrap();
            let res = self.from_redis_value_to_scalar(&redis_val, &default_value)?;
            match res {
                Scalar::Array(arr) => {
                    let mut kv_pairs: Vec<(String, Scalar)> = vec![];
                    // if kv-pairs are all out of cache, return answer early to avoid using the hashmap `kv_map`.
                    if all_out_of_cache {
                        for i in 0..arr.len() {
                            if vali_none == true || vali_bitmap.get_bit(i) == true {
                                let val = unsafe { arr.index_unchecked(i) }.to_owned();
                                builder.push(val.as_ref());
                                let key = keys[i];
                                let cache_key = cache_key_prefix.clone() + "-" + key;
                                kv_pairs.push((cache_key, val));
                            } else {
                                builder.push(default_value.as_ref());
                            }
                        }
                        redis_cache.insert_batch(kv_pairs);
                        return Ok(Value::Column(builder.build()));
                    }

                    for i in 0..arr.len() {
                        let key = keys[i];
                        let val = unsafe { arr.index_unchecked(i) }.to_owned();
                        kv_map.insert(key, val.clone().into());
                        let cache_key = cache_key_prefix.clone() + "-" + key;
                        kv_pairs.push((cache_key, val));
                    }
                    redis_cache.insert_batch(kv_pairs);
                }
                Scalar::String(str) => {
                    let key = unsafe { str_col.index_unchecked(0) };
                    let val = Scalar::String(str.clone());
                    kv_map.insert(key, val.clone().into());
                    let cache_key = cache_key_prefix.clone() + "-" + key;
                    redis_cache.insert(cache_key, val);
                }
                _ => unreachable!(),
            }
        }

        // step-5: push results to the column.
        for i in 0..key_cnt {
            if vali_none == true || vali_bitmap.get_bit(i) == true {
                let key = unsafe { str_col.index_unchecked(i) };
                builder.push((*kv_map[key]).as_ref());
            } else {
                builder.push(default_value.as_ref());
            }
        }
        Ok(Value::Column(builder.build()))
    }

    fn from_redis_value_to_scalar(
        &self,
        rv: &redis::Value,
        default_value: &Scalar,
    ) -> Result<Scalar> {
        match rv {
            redis::Value::BulkString(bs) => {
                let str = unsafe { String::from_utf8_unchecked(bs.to_vec()) };
                Ok(Scalar::String(str))
            }
            redis::Value::Array(arr) => {
                let mut builder = ColumnBuilder::with_capacity(&DataType::String, 1);
                for item in arr {
                    let scalar = self.from_redis_value_to_scalar(item, default_value)?;
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
            ScalarRef::Date(d) => format!("{}", date_to_string(d as i64, Tz::UTC)),
            ScalarRef::Timestamp(t) => format!("{}", timestamp_to_string(t, Tz::UTC)),
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
                        let redis_cache = CacheManager::instance().get_dictionary_cache().unwrap();
                        let (db_name, dict_name, field_arg) =
                            parse_display_name(&async_func_desc.display_name)?;
                        let cache_key_prefix = format!("{}-{}-{}", db_name, dict_name, field_arg);
                        operators.insert(
                            i,
                            Arc::new(DictionaryOperator::Redis((
                                conn,
                                redis_cache,
                                cache_key_prefix,
                            ))),
                        );
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

pub fn parse_display_name(display_name: &String) -> Result<(String, String, String)> {
    let re = Regex::new(
        r"(?x)
        ^
        [^()]+
        \(([^.]+)\.([^,]+),
        \s*([^,]+)
        ,\s*[^)]+
        \)$
    ",
    )
    .unwrap();

    if let Some(captures) = re.captures(display_name) {
        let db_name = captures.get(1).map_or("", |m| m.as_str()).to_string();
        let dict_name = captures.get(2).map_or("", |m| m.as_str()).to_string();
        let field_arg = captures.get(3).map_or("", |m| m.as_str()).to_string();

        Ok((db_name, dict_name, field_arg))
    } else {
        Err(ErrorCode::DictionarySourceError(format!(
            "display_name '{}' is not valid",
            display_name,
        )))
    }
}
