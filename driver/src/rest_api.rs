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

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use databend_client::response::QueryResponse;
use databend_client::APIClient;
use tokio_stream::{Stream, StreamExt};

use crate::conn::{Connection, ConnectionInfo};
use crate::error::{Error, Result};
use crate::rows::{Row, RowIterator, RowProgressIterator, RowWithProgress};
use crate::schema::{Schema, SchemaRef};
use crate::ScanProgress;

#[derive(Clone)]
pub struct RestAPIConnection {
    pub(crate) client: APIClient,
}

#[async_trait]
impl Connection for RestAPIConnection {
    fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            handler: "RestAPI".to_string(),
            host: self.client.host.clone(),
            port: self.client.port,
            user: self.client.user.clone(),
            database: self.client.database.clone(),
        }
    }

    async fn exec(&mut self, sql: &str) -> Result<i64> {
        let mut resp = self.client.query(sql).await?;
        while let Some(next_uri) = resp.next_uri {
            resp = self.client.query_page(&next_uri).await?;
        }
        Ok(resp.stats.progresses.write_progress.rows as i64)
    }

    async fn query_iter(&mut self, sql: &str) -> Result<RowIterator> {
        let (_, rows_with_progress) = self.query_iter_ext(sql).await?;
        let rows = rows_with_progress.filter_map(|r| match r {
            Ok(RowWithProgress::Row(r)) => Some(Ok(r)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        });
        Ok(Box::pin(rows))
    }

    async fn query_iter_ext(&mut self, sql: &str) -> Result<(Schema, RowProgressIterator)> {
        let resp = self.client.query(sql).await?;
        let (schema, rows) = RestAPIRows::from_response(self.client.clone(), resp)?;
        Ok((schema, Box::pin(rows)))
    }

    async fn query_row(&mut self, sql: &str) -> Result<Option<Row>> {
        let resp = self.client.query(sql).await?;
        let resp = self.wait_for_data(resp).await?;
        self.finish_query(resp.final_uri).await?;
        let schema = resp.schema.try_into()?;
        if resp.data.is_empty() {
            Ok(None)
        } else {
            let row = Row::try_from((Arc::new(schema), &resp.data[0]))?;
            Ok(Some(row))
        }
    }
}

impl RestAPIConnection {
    pub fn try_create(dsn: &str) -> Result<Self> {
        let client = APIClient::from_dsn(dsn)?;
        Ok(Self { client })
    }

    async fn wait_for_data(&self, pre: QueryResponse) -> Result<QueryResponse> {
        if !pre.data.is_empty() {
            return Ok(pre);
        }
        let mut result = pre;
        // preserve schema since it is no included in the final response
        let schema = result.schema;
        while let Some(next_uri) = result.next_uri {
            result = self.client.query_page(&next_uri).await?;
            if !result.data.is_empty() {
                break;
            }
        }
        result.schema = schema;
        Ok(result)
    }

    async fn finish_query(&self, final_uri: Option<String>) -> Result<QueryResponse> {
        match final_uri {
            Some(uri) => self.client.query_page(&uri).await.map_err(|e| e.into()),
            None => Err(Error::InvalidResponse("final_uri is empty".to_string())),
        }
    }
}

type PageFut = Pin<Box<dyn Future<Output = Result<QueryResponse>> + Send>>;

pub struct RestAPIRows {
    client: APIClient,
    schema: SchemaRef,
    data: VecDeque<Vec<String>>,
    next_uri: Option<String>,
    next_page: Option<PageFut>,
}

impl RestAPIRows {
    fn from_response(client: APIClient, resp: QueryResponse) -> Result<(Schema, Self)> {
        let schema: Schema = resp.schema.try_into()?;
        let rows = Self {
            client,
            next_uri: resp.next_uri,
            schema: Arc::new(schema.clone()),
            data: resp.data.into(),
            next_page: None,
        };
        Ok((schema, rows))
    }
}

impl Stream for RestAPIRows {
    type Item = Result<RowWithProgress>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(row) = self.data.pop_front() {
            let row = Row::try_from((self.schema.clone(), &row))?;
            return Poll::Ready(Some(Ok(RowWithProgress::Row(row))));
        }
        match self.next_page {
            Some(ref mut next_page) => match Pin::new(next_page).poll(cx) {
                Poll::Ready(Ok(resp)) => {
                    self.data = resp.data.into();
                    self.next_uri = resp.next_uri;
                    self.next_page = None;
                    let mut progress = ScanProgress {
                        total_rows: 0,
                        total_bytes: 0,
                        read_rows: resp.stats.progresses.scan_progress.rows,
                        read_bytes: resp.stats.progresses.scan_progress.bytes,
                    };
                    if let Some(total_scan) = resp.stats.progresses.total_scan {
                        progress.total_bytes = total_scan.bytes;
                        progress.total_rows = total_scan.rows;
                    }
                    Poll::Ready(Some(Ok(RowWithProgress::Progress(progress))))
                }
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            },
            None => match self.next_uri {
                Some(ref next_uri) => {
                    let client = self.client.clone();
                    let next_uri = next_uri.clone();
                    self.next_page = Some(Box::pin(async move {
                        client.query_page(&next_uri).await.map_err(|e| e.into())
                    }));
                    self.poll_next(cx)
                }
                None => Poll::Ready(None),
            },
        }
    }
}
