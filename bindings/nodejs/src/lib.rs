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

#[macro_use]
extern crate napi_derive;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures::StreamExt;
use napi::{bindgen_prelude::*, tokio};

#[napi]
pub struct Client(Box<dyn databend_driver::Connection>);

#[napi]
pub struct ConnectionInfo(databend_driver::ConnectionInfo);

pub struct Value(databend_driver::Value);

impl ToNapiValue for Value {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        match val.0 {
            databend_driver::Value::Null => Null::to_napi_value(env, Null),
            databend_driver::Value::Boolean(b) => bool::to_napi_value(env, b),
            databend_driver::Value::String(s) => String::to_napi_value(env, s),
            databend_driver::Value::Number(n) => NumberValue::to_napi_value(env, NumberValue(n)),
            databend_driver::Value::Timestamp(_) => {
                let v = NaiveDateTime::try_from(val.0).map_err(format_napi_error)?;
                NaiveDateTime::to_napi_value(env, v)
            }
            databend_driver::Value::Date(_) => {
                let v = NaiveDate::try_from(val.0).map_err(format_napi_error)?;
                NaiveDateTime::to_napi_value(
                    env,
                    NaiveDateTime::new(v, NaiveTime::from_hms_opt(0, 0, 0).unwrap()),
                )
            }
        }
    }
}

pub struct NumberValue(databend_driver::NumberValue);

impl ToNapiValue for NumberValue {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        match val.0 {
            databend_driver::NumberValue::Int8(i) => i8::to_napi_value(env, i),
            databend_driver::NumberValue::Int16(i) => i16::to_napi_value(env, i),
            databend_driver::NumberValue::Int32(i) => i32::to_napi_value(env, i),
            databend_driver::NumberValue::Int64(i) => i64::to_napi_value(env, i),
            databend_driver::NumberValue::UInt8(i) => u8::to_napi_value(env, i),
            databend_driver::NumberValue::UInt16(i) => u16::to_napi_value(env, i),
            databend_driver::NumberValue::UInt32(i) => u32::to_napi_value(env, i),
            databend_driver::NumberValue::UInt64(i) => u64::to_napi_value(env, i),
            databend_driver::NumberValue::Float32(i) => f32::to_napi_value(env, i),
            databend_driver::NumberValue::Float64(i) => f64::to_napi_value(env, i),
            databend_driver::NumberValue::Decimal128(_, _) => {
                String::to_napi_value(env, val.0.to_string())
            }
            databend_driver::NumberValue::Decimal256(_, _) => {
                String::to_napi_value(env, val.0.to_string())
            }
        }
    }
}

#[napi]
pub struct RowIterator(databend_driver::RowIterator);

#[napi]
impl RowIterator {
    #[napi]
    pub async unsafe fn next(&mut self) -> Option<Result<Row>> {
        self.0
            .next()
            .await
            .map(|row| row.map(Row).map_err(format_napi_error))
    }
}

#[napi]
pub struct Row(databend_driver::Row);

#[napi]
impl Row {
    #[napi]
    pub fn values(&self) -> Vec<Value> {
        // FIXME: do not clone
        self.0
            .values()
            .to_owned()
            .into_iter()
            .map(|v| Value(v))
            .collect()
    }
}

#[napi]
pub struct QueryProgress(databend_driver::QueryProgress);

#[napi]
impl Client {
    #[napi(constructor)]
    pub fn new(dsn: String) -> Result<Self> {
        let conn = databend_driver::new_connection(&dsn).map_err(format_napi_error)?;
        Ok(Self(conn))
    }

    #[napi]
    pub async fn info(&self) -> ConnectionInfo {
        ConnectionInfo(self.0.info().await)
    }

    #[napi]
    pub async fn version(&self) -> Result<String> {
        self.0.version().await.map_err(format_napi_error)
    }

    #[napi]
    pub async fn exec(&self, sql: String) -> Result<i64> {
        self.0.exec(&sql).await.map_err(format_napi_error)
    }

    #[napi]
    pub async fn query_row(&self, sql: String) -> Result<Option<Row>> {
        self.0
            .query_row(&sql)
            .await
            .map(|row| row.map(Row))
            .map_err(format_napi_error)
    }

    #[napi]
    pub async fn query_iter(&self, sql: String) -> Result<RowIterator> {
        self.0
            .query_iter(&sql)
            .await
            .map(|iter| RowIterator(iter))
            .map_err(format_napi_error)
    }

    #[napi]
    pub async fn stream_load(
        &self,
        sql: String,
        file: String,
        file_format_options: Option<Vec<(String, String)>>,
        copy_options: Option<Vec<(String, String)>>,
    ) -> Result<QueryProgress> {
        let file_format_options_map = match file_format_options {
            Some(ref o) => {
                let mut map = std::collections::BTreeMap::new();
                for (k, v) in o {
                    map.insert(k.as_str(), v.as_str());
                }
                Some(map)
            }
            None => None,
        };
        let copy_options_map = match copy_options {
            Some(ref o) => {
                let mut map = std::collections::BTreeMap::new();
                for (k, v) in o {
                    map.insert(k.as_str(), v.as_str());
                }
                Some(map)
            }
            None => None,
        };
        let metadata = tokio::fs::metadata(&file).await.map_err(Error::from)?;
        let size = metadata.len();
        let f = tokio::fs::File::open(&file).await.map_err(Error::from)?;
        let reader = tokio::io::BufReader::new(f);
        let progress = self
            .0
            .stream_load(
                &sql,
                Box::new(reader),
                size,
                file_format_options_map,
                copy_options_map,
            )
            .await
            .map_err(format_napi_error)?;
        Ok(QueryProgress(progress))
    }
}

fn format_napi_error(err: databend_driver::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
