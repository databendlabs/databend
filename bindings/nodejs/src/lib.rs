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
use napi::{bindgen_prelude::*, Env};
use once_cell::sync::Lazy;
use tokio_stream::StreamExt;

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    version.to_string()
});

#[napi]
pub struct Client(databend_driver::Client);

#[napi]
pub struct Connection(Box<dyn databend_driver::Connection>);

#[napi]
pub struct ConnectionInfo(databend_driver::ConnectionInfo);

#[napi]
impl ConnectionInfo {
    #[napi(getter)]
    pub fn handler(&self) -> String {
        self.0.handler.to_string()
    }

    #[napi(getter)]
    pub fn host(&self) -> String {
        self.0.host.to_string()
    }

    #[napi(getter)]
    pub fn port(&self) -> u16 {
        self.0.port
    }

    #[napi(getter)]
    pub fn user(&self) -> String {
        self.0.user.to_string()
    }

    #[napi(getter)]
    pub fn database(&self) -> Option<String> {
        self.0.database.clone()
    }

    #[napi(getter)]
    pub fn warehouse(&self) -> Option<String> {
        self.0.warehouse.clone()
    }
}

pub struct Value(databend_driver::Value);

impl ToNapiValue for Value {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        let ctx = Env::from(env);
        match val.0 {
            databend_driver::Value::Null => Null::to_napi_value(env, Null),
            databend_driver::Value::EmptyArray => {
                let arr = ctx.create_array(0)?;
                Array::to_napi_value(env, arr)
            }
            databend_driver::Value::EmptyMap => {
                let obj = ctx.create_object()?;
                Object::to_napi_value(env, obj)
            }
            databend_driver::Value::Boolean(b) => bool::to_napi_value(env, b),
            databend_driver::Value::Binary(b) => Buffer::to_napi_value(env, b.into()),
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
            databend_driver::Value::Array(inner) => {
                let mut arr = ctx.create_array(inner.len() as u32)?;
                for (i, v) in inner.into_iter().enumerate() {
                    arr.set(i as u32, Value(v))?;
                }
                Array::to_napi_value(env, arr)
            }
            databend_driver::Value::Map(inner) => {
                let mut obj = ctx.create_object()?;
                for (k, v) in inner.into_iter() {
                    obj.set(k.to_string(), Value(v))?;
                }
                Object::to_napi_value(env, obj)
            }
            databend_driver::Value::Tuple(inner) => {
                let mut arr = ctx.create_array(inner.len() as u32)?;
                for (i, v) in inner.into_iter().enumerate() {
                    arr.set(i as u32, Value(v))?;
                }
                Array::to_napi_value(env, arr)
            }
            databend_driver::Value::Bitmap(s) => String::to_napi_value(env, s),
            databend_driver::Value::Variant(s) => String::to_napi_value(env, s),
            databend_driver::Value::Geometry(s) => String::to_napi_value(env, s),
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
pub struct Schema(databend_driver::SchemaRef);

#[napi]
impl Schema {
    #[napi]
    pub fn fields(&self) -> Vec<Field> {
        self.0.fields().iter().map(|f| Field(f.clone())).collect()
    }
}

#[napi]
pub struct Field(databend_driver::Field);

#[napi]
impl Field {
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.0.name.to_string()
    }

    #[napi(getter)]
    pub fn data_type(&self) -> String {
        self.0.data_type.to_string()
    }
}

#[napi]
pub struct RowIterator(databend_driver::RowIterator);

#[napi]
impl RowIterator {
    /// Fetch next row.
    /// Returns `None` if there are no more rows.
    #[napi]
    #[allow(clippy::missing_safety_doc)]
    pub async unsafe fn next(&mut self) -> Option<Result<Row>> {
        self.0
            .next()
            .await
            .map(|row| row.map(Row).map_err(format_napi_error))
    }
    /// Get Schema for rows.
    #[napi]
    pub fn schema(&self) -> Schema {
        Schema(self.0.schema().clone())
    }
}

#[napi]
pub struct RowIteratorExt(databend_driver::RowStatsIterator);

#[napi]
impl RowIteratorExt {
    /// Fetch next row or stats.
    /// Returns `None` if there are no more rows.
    #[napi]
    #[allow(clippy::missing_safety_doc)]
    pub async unsafe fn next(&mut self) -> Option<Result<RowOrStats>> {
        match self.0.next().await {
            None => None,
            Some(r0) => match r0 {
                Ok(r1) => match r1 {
                    databend_driver::RowWithStats::Row(row) => Some(Ok(RowOrStats {
                        row: Some(Row(row)),
                        stats: None,
                    })),
                    databend_driver::RowWithStats::Stats(ss) => Some(Ok(RowOrStats {
                        row: None,
                        stats: Some(ServerStats(ss)),
                    })),
                },
                Err(e) => Some(Err(format_napi_error(e))),
            },
        }
    }

    #[napi]
    pub fn schema(&self) -> Schema {
        Schema(self.0.schema().clone())
    }
}

/// Must contain either row or stats.
#[napi]
pub struct RowOrStats {
    row: Option<Row>,
    stats: Option<ServerStats>,
}

#[napi]
impl RowOrStats {
    #[napi(getter)]
    pub fn row(&self) -> Option<Row> {
        self.row.clone()
    }

    #[napi(getter)]
    pub fn stats(&self) -> Option<ServerStats> {
        self.stats.clone()
    }
}

#[napi]
#[derive(Clone)]
pub struct Row(databend_driver::Row);

#[napi]
impl Row {
    #[napi]
    pub fn values(&self) -> Vec<Value> {
        // FIXME: do not clone
        self.0.values().iter().map(|v| Value(v.clone())).collect()
    }
}

#[napi]
#[derive(Clone)]
pub struct ServerStats(databend_driver::ServerStats);

#[napi]
impl ServerStats {
    #[napi(getter)]
    pub fn total_rows(&self) -> usize {
        self.0.total_rows
    }

    #[napi(getter)]
    pub fn total_bytes(&self) -> usize {
        self.0.total_bytes
    }

    #[napi(getter)]
    pub fn read_rows(&self) -> usize {
        self.0.read_rows
    }

    #[napi(getter)]
    pub fn read_bytes(&self) -> usize {
        self.0.read_bytes
    }

    #[napi(getter)]
    pub fn write_rows(&self) -> usize {
        self.0.write_rows
    }

    #[napi(getter)]
    pub fn write_bytes(&self) -> usize {
        self.0.write_bytes
    }

    #[napi(getter)]
    pub fn running_time_ms(&self) -> f64 {
        self.0.running_time_ms
    }
}

#[napi]
impl Client {
    /// Create a new databend client with a given DSN.
    #[napi(constructor)]
    pub fn new(dsn: String) -> Self {
        let name = format!("databend-driver-nodejs/{}", VERSION.as_str());
        let client = databend_driver::Client::new(dsn).with_name(name);
        Self(client)
    }

    /// Get a connection from the client.
    #[napi]
    pub async fn get_conn(&self) -> Result<Connection> {
        self.0
            .get_conn()
            .await
            .map(Connection)
            .map_err(format_napi_error)
    }
}

#[napi]
impl Connection {
    /// Get the connection information.
    #[napi]
    pub async fn info(&self) -> ConnectionInfo {
        ConnectionInfo(self.0.info().await)
    }

    /// Get the databend version.
    #[napi]
    pub async fn version(&self) -> Result<String> {
        self.0.version().await.map_err(format_napi_error)
    }

    /// Execute a SQL query, return the number of affected rows.
    #[napi]
    pub async fn exec(&self, sql: String) -> Result<i64> {
        self.0.exec(&sql).await.map_err(format_napi_error)
    }

    /// Execute a SQL query, and only return the first row.
    #[napi]
    pub async fn query_row(&self, sql: String) -> Result<Option<Row>> {
        self.0
            .query_row(&sql)
            .await
            .map(|row| row.map(Row))
            .map_err(format_napi_error)
    }

    /// Execute a SQL query and fetch all data into the result
    #[napi]
    pub async fn query_all(&self, sql: String) -> Result<Vec<Row>> {
        Ok(self
            .0
            .query_all(&sql)
            .await
            .map_err(format_napi_error)?
            .into_iter()
            .map(Row)
            .collect())
    }

    /// Execute a SQL query, and return all rows.
    #[napi]
    pub async fn query_iter(&self, sql: String) -> Result<RowIterator> {
        self.0
            .query_iter(&sql)
            .await
            .map(RowIterator)
            .map_err(format_napi_error)
    }

    /// Execute a SQL query, and return all rows with schema and stats.
    #[napi]
    pub async fn query_iter_ext(&self, sql: String) -> Result<RowIteratorExt> {
        let iterator = self
            .0
            .query_iter_ext(&sql)
            .await
            .map_err(format_napi_error)?;
        Ok(RowIteratorExt(iterator))
    }

    /// Load data with stage attachment.
    /// The SQL can be `INSERT INTO tbl VALUES` or `REPLACE INTO tbl VALUES`.
    #[napi]
    pub async fn stream_load(&self, sql: String, data: Vec<Vec<&str>>) -> Result<ServerStats> {
        let ss = self
            .0
            .stream_load(&sql, data)
            .await
            .map_err(format_napi_error)?;
        Ok(ServerStats(ss))
    }
}

fn format_napi_error(err: databend_driver::Error) -> Error {
    Error::from_reason(format!("{}", err))
}
