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

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use serde::Deserialize;
use tokio_stream::{Stream, StreamExt};

#[cfg(feature = "flight-sql")]
use arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};
use crate::schema::SchemaRef;
use crate::value::Value;

#[derive(Clone, Debug)]
pub enum RowWithStats {
    Row(Row),
    Stats(ServerStats),
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct ServerStats {
    #[serde(default)]
    pub total_rows: usize,
    #[serde(default)]
    pub total_bytes: usize,

    #[serde(default)]
    pub read_rows: usize,
    #[serde(default)]
    pub read_bytes: usize,

    #[serde(default)]
    pub write_rows: usize,
    #[serde(default)]
    pub write_bytes: usize,

    #[serde(default)]
    pub running_time_ms: f64,
}

impl ServerStats {
    pub fn normalize(&mut self) {
        if self.total_rows == 0 {
            self.total_rows = self.read_rows;
        }
        if self.total_bytes == 0 {
            self.total_bytes = self.read_bytes;
        }
    }
}

impl From<databend_client::response::QueryStats> for ServerStats {
    fn from(stats: databend_client::response::QueryStats) -> Self {
        let mut p = Self {
            total_rows: 0,
            total_bytes: 0,
            read_rows: stats.progresses.scan_progress.rows,
            read_bytes: stats.progresses.scan_progress.bytes,
            write_rows: stats.progresses.write_progress.rows,
            write_bytes: stats.progresses.write_progress.bytes,
            running_time_ms: stats.running_time_ms,
        };
        if let Some(total) = stats.progresses.total_scan {
            p.total_rows = total.rows;
            p.total_bytes = total.bytes;
        }
        p
    }
}

#[derive(Clone, Debug, Default)]
pub struct Row(Vec<Value>);

impl TryFrom<(SchemaRef, Vec<Option<String>>)> for Row {
    type Error = Error;

    fn try_from((schema, data): (SchemaRef, Vec<Option<String>>)) -> Result<Self> {
        let mut values: Vec<Value> = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            let val: Option<&str> = data.get(i).and_then(|v| v.as_deref());
            values.push(Value::try_from((&field.data_type, val))?);
        }
        Ok(Self(values))
    }
}

impl Row {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn values(&self) -> &[Value] {
        &self.0
    }

    pub fn from_vec(values: Vec<Value>) -> Self {
        Self(values)
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Clone, Debug)]
pub struct Rows {
    schema: SchemaRef,
    rows: Vec<Row>,
}

#[cfg(feature = "flight-sql")]
impl TryFrom<RecordBatch> for Rows {
    type Error = Error;
    fn try_from(batch: RecordBatch) -> Result<Self> {
        let schema = batch.schema();
        let mut rows: Vec<Row> = Vec::new();
        for i in 0..batch.num_rows() {
            let mut values: Vec<Value> = Vec::new();
            for j in 0..schema.fields().len() {
                let v = batch.column(j);
                let field = schema.field(j);
                let value = Value::try_from((field, v, i))?;
                values.push(value);
            }
            rows.push(Row(values));
        }
        Ok(Self {
            schema: std::sync::Arc::new(schema.try_into()?),
            rows,
        })
    }
}

impl IntoIterator for Rows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl Rows {
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

macro_rules! replace_expr {
    ($_t:tt $sub:expr) => {
        $sub
    };
}

// This macro implements TryFrom for tuple of types
macro_rules! impl_tuple_from_row {
    ( $($Ti:tt),+ ) => {
        impl<$($Ti),+> TryFrom<Row> for ($($Ti,)+)
        where
            $($Ti: TryFrom<Value>),+
        {
            type Error = String;
            fn try_from(row: Row) -> Result<Self, String> {
                // It is not possible yet to get the number of metavariable repetitions
                // ref: https://github.com/rust-lang/lang-team/issues/28#issue-644523674
                // This is a workaround
                let expected_len = <[()]>::len(&[$(replace_expr!(($Ti) ())),*]);

                if expected_len != row.len() {
                    return Err(format!("row size mismatch: expected {} columns, got {}", expected_len, row.len()));
                }
                let mut vals_iter = row.into_iter().enumerate();

                Ok((
                    $(
                        {
                            let (col_ix, col_value) = vals_iter
                                .next()
                                .unwrap(); // vals_iter size is checked before this code is reached,
                                           // so it is safe to unwrap
                            let t = col_value.get_type();
                            $Ti::try_from(col_value)
                                .map_err(|_| format!("failed converting column {} from type({:?}) to type({})", col_ix, t, std::any::type_name::<$Ti>()))?
                        }
                    ,)+
                ))
            }
        }
    }
}

// Implement FromRow for tuples of size up to 16
impl_tuple_from_row!(T1);
impl_tuple_from_row!(T1, T2);
impl_tuple_from_row!(T1, T2, T3);
impl_tuple_from_row!(T1, T2, T3, T4);
impl_tuple_from_row!(T1, T2, T3, T4, T5);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_tuple_from_row!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

pub struct RowIterator {
    schema: SchemaRef,
    it: Pin<Box<dyn Stream<Item = Result<Row>> + Send>>,
}

impl RowIterator {
    pub fn new(schema: SchemaRef, it: Pin<Box<dyn Stream<Item = Result<Row>> + Send>>) -> Self {
        Self { schema, it }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub async fn try_collect<T>(mut self) -> Result<Vec<T>>
    where
        T: TryFrom<Row>,
        T::Error: std::fmt::Display,
    {
        let mut ret = Vec::new();
        while let Some(row) = self.it.next().await {
            let v = T::try_from(row?).map_err(|e| Error::Parsing(e.to_string()))?;
            ret.push(v)
        }
        Ok(ret)
    }
}

impl Stream for RowIterator {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.it).poll_next(cx)
    }
}

pub struct RowStatsIterator {
    schema: SchemaRef,
    it: Pin<Box<dyn Stream<Item = Result<RowWithStats>> + Send>>,
}

impl RowStatsIterator {
    pub fn new(
        schema: SchemaRef,
        it: Pin<Box<dyn Stream<Item = Result<RowWithStats>> + Send>>,
    ) -> Self {
        Self { schema, it }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub async fn filter_rows(self) -> RowIterator {
        let it = self.it.filter_map(|r| match r {
            Ok(RowWithStats::Row(r)) => Some(Ok(r)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        });
        RowIterator::new(self.schema, Box::pin(it))
    }
}

impl Stream for RowStatsIterator {
    type Item = Result<RowWithStats>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.it).poll_next(cx)
    }
}
