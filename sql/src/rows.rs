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
pub enum RowWithProgress {
    Row(Row),
    Progress(QueryProgress),
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct QueryProgress {
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
}

impl QueryProgress {
    pub fn normalize(&mut self) {
        if self.total_rows == 0 {
            self.total_rows = self.read_rows;
        }
        if self.total_bytes == 0 {
            self.total_bytes = self.read_bytes;
        }
    }
}

impl From<databend_client::response::Progresses> for QueryProgress {
    fn from(progresses: databend_client::response::Progresses) -> Self {
        let mut p = Self {
            total_rows: 0,
            total_bytes: 0,
            read_rows: progresses.scan_progress.rows,
            read_bytes: progresses.scan_progress.bytes,
            write_rows: progresses.write_progress.rows,
            write_bytes: progresses.write_progress.bytes,
        };
        if let Some(total) = progresses.total_scan {
            p.total_rows = total.rows;
            p.total_bytes = total.bytes;
        }
        p
    }
}

#[derive(Clone, Debug, Default)]
pub struct Row(Vec<Value>);

impl TryFrom<(SchemaRef, &Vec<String>)> for Row {
    type Error = Error;

    fn try_from((schema, data): (SchemaRef, &Vec<String>)) -> Result<Self> {
        let mut values: Vec<Value> = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            values.push(Value::try_from((&field.data_type, data[i].as_str()))?);
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
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Rows(Vec<Row>);

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
        Ok(Self(rows))
    }
}

impl IntoIterator for Rows {
    type Item = Row;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

pub struct RowIterator(Pin<Box<dyn Stream<Item = Result<Row>> + Send>>);

impl RowIterator {
    pub fn new(it: Pin<Box<dyn Stream<Item = Result<Row>> + Send>>) -> Self {
        Self(it)
    }

    pub async fn try_collect<T>(mut self) -> Result<Vec<T>>
    where
        T: TryFrom<Row>,
        T::Error: std::fmt::Display,
    {
        let mut ret = Vec::new();
        while let Some(row) = self.0.next().await {
            let v = T::try_from(row?).map_err(|e| Error::Parsing(e.to_string()))?;
            ret.push(v)
        }
        Ok(ret)
    }
}

impl Stream for RowIterator {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

pub struct RowProgressIterator(Pin<Box<dyn Stream<Item = Result<RowWithProgress>> + Send>>);

impl RowProgressIterator {
    pub fn new(it: Pin<Box<dyn Stream<Item = Result<RowWithProgress>> + Send>>) -> Self {
        Self(it)
    }

    pub async fn filter_rows(self) -> RowIterator {
        let rows = self.0.filter_map(|r| match r {
            Ok(RowWithProgress::Row(r)) => Some(Ok(r)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        });
        RowIterator(Box::pin(rows))
    }
}

impl Stream for RowProgressIterator {
    type Item = Result<RowWithProgress>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}
