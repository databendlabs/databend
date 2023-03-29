// Copyright 2023 Datafuse Labs.
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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{anyhow, Error, Result};
use databend_client::response::QueryResponse;
use databend_client::APIClient;
use tokio_stream::Stream;

use crate::schema::{DataType, SchemaFieldList};
use crate::value::Value;

#[derive(Clone, Debug, Default)]
pub struct Row(Vec<Value>);

impl TryFrom<(Vec<DataType>, Vec<String>)> for Row {
    type Error = Error;

    fn try_from((schema, data): (Vec<DataType>, Vec<String>)) -> Result<Self> {
        let mut row: Vec<Value> = Vec::new();
        for (i, value) in data.into_iter().enumerate() {
            row.push(Value::try_from((schema[i].clone(), value))?);
        }
        Ok(Self(row))
    }
}

impl Row {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
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
            type Error = Error;
            fn try_from(row: Row) -> Result<Self> {
                // It is not possible yet to get the number of metavariable repetitions
                // ref: https://github.com/rust-lang/lang-team/issues/28#issue-644523674
                // This is a workaround
                let expected_len = <[()]>::len(&[$(replace_expr!(($Ti) ())),*]);

                if expected_len != row.len() {
                    return Err(anyhow!("row size mismatch: expected {} columns, got {}", expected_len, row.len()));
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
                                .map_err(|_| anyhow!("failed converting column {} from type({:?}) to type({})", col_ix, t, std::any::type_name::<$Ti>()))?
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
    client: Arc<APIClient>,
    next_uri: Option<String>,
    schema: Vec<DataType>,
    data: Vec<Vec<String>>,
}

impl Stream for RowIterator {
    type Item = Result<Row>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.data.is_empty() {
            return Poll::Ready(Some(Ok(Row::try_from((
                self.schema.clone(),
                self.data.remove(0),
            ))?)));
        }
        if self.next_uri.is_none() {
            return Poll::Ready(None);
        }
        let next_uri = self.next_uri.take().unwrap();
        let client = self.client.clone();
        let mut resp = Box::pin(client.query_page(&next_uri));
        match Pin::new(&mut resp).poll(cx) {
            Poll::Ready(Ok(resp)) => {
                self.next_uri = resp.next_uri;
                self.data = resp.data;
                self.poll_next(cx)
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl TryFrom<(Arc<APIClient>, QueryResponse)> for RowIterator {
    type Error = Error;

    fn try_from((client, resp): (Arc<APIClient>, QueryResponse)) -> Result<Self> {
        let schema = SchemaFieldList::new(resp.schema).try_into()?;
        Ok(Self {
            client,
            next_uri: resp.next_uri,
            schema,
            data: resp.data,
        })
    }
}
