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

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use databend_client::response::QueryResponse;
use databend_client::APIClient;
use databend_sql::value::Value;
use http::StatusCode;
use tokio::io::AsyncWriteExt;
use tokio_stream::{Stream, StreamExt};

use databend_sql::error::{Error, Result};
use databend_sql::rows::{QueryProgress, Row, RowIterator, RowProgressIterator, RowWithProgress};
use databend_sql::schema::{DataType, Field, Schema, SchemaRef};

use crate::conn::{Connection, ConnectionInfo, Reader};

#[derive(Clone)]
pub struct RestAPIConnection {
    client: APIClient,
}

#[async_trait]
impl Connection for RestAPIConnection {
    async fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            handler: "RestAPI".to_string(),
            host: self.client.host.clone(),
            port: self.client.port,
            user: self.client.user.clone(),
            database: self.client.current_database().await,
            warehouse: self.client.current_warehouse().await,
        }
    }

    async fn exec(&self, sql: &str) -> Result<i64> {
        let mut resp = self.client.query(sql).await?;
        while let Some(next_uri) = resp.next_uri {
            resp = self.client.query_page(&next_uri).await?;
        }
        Ok(resp.stats.progresses.write_progress.rows as i64)
    }

    async fn query_iter(&self, sql: &str) -> Result<RowIterator> {
        let (_, rows_with_progress) = self.query_iter_ext(sql).await?;
        let rows = rows_with_progress.filter_map(|r| match r {
            Ok(RowWithProgress::Row(r)) => Some(Ok(r)),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        });
        Ok(RowIterator::new(Box::pin(rows)))
    }

    async fn query_iter_ext(&self, sql: &str) -> Result<(Schema, RowProgressIterator)> {
        let resp = self.client.query(sql).await?;
        let (schema, rows) = RestAPIRows::from_response(self.client.clone(), resp)?;
        Ok((schema, RowProgressIterator::new(Box::pin(rows))))
    }

    async fn query_row(&self, sql: &str) -> Result<Option<Row>> {
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

    async fn stream_load(
        &self,
        sql: &str,
        data: Reader,
        size: u64,
        file_format_options: Option<BTreeMap<&str, &str>>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<QueryProgress> {
        let stage_location = format!("@~/client/load/{}", chrono::Utc::now().timestamp_nanos());
        self.client
            .upload_to_stage(&stage_location, data, size)
            .await?;
        let file_format_options =
            file_format_options.unwrap_or_else(Self::default_file_format_options);
        let copy_options = copy_options.unwrap_or_else(Self::default_copy_options);
        let resp = self
            .client
            .insert_with_stage(sql, &stage_location, file_format_options, copy_options)
            .await?;
        Ok(QueryProgress::from(resp.stats.progresses))
    }

    // PUT file://<path_to_file>/<filename> internalStage|externalStage
    async fn put_files(
        &self,
        local_file: &str,
        stage_path: &str,
    ) -> Result<(Schema, RowProgressIterator)> {
        let dsn = url::Url::parse(local_file)?;
        let schema = dsn.scheme();
        if schema != "file" && schema != "fs" {
            return Err(Error::BadArgument(
                "Only support schema file:// or fs://".to_string(),
            ));
        }

        let mut results = Vec::new();
        let stage_path = stage_path.trim_end_matches(|p| p == '/');

        for entry in glob::glob(dsn.path())? {
            let entry = entry?;
            let stage_location = format!(
                "{stage_path}/{}",
                entry.file_name().unwrap().to_str().unwrap()
            );

            let data = tokio::fs::File::open(&entry).await?;
            let size = data.metadata().await?.len();

            let res = match self
                .client
                .upload_to_stage_with_stream(&stage_location, data, size)
                .await
            {
                Ok(_) => (entry.to_string_lossy().to_string(), "SUCCESS".to_owned()),
                Err(e) => (entry.to_string_lossy().to_string(), e.to_string()),
            };
            results.push(Ok(RowWithProgress::Row(Row::from_vec(vec![
                Value::String(res.0),
                Value::String(res.1),
            ]))));
        }

        Ok((
            put_get_schema(),
            RowProgressIterator::new(Box::pin(futures::stream::iter(results))),
        ))
    }

    async fn get_files(
        &self,
        stage_location: &str,
        local_file: &str,
    ) -> Result<(Schema, RowProgressIterator)> {
        let dsn = url::Url::parse(local_file)?;
        let schema = dsn.scheme();
        if schema != "file" && schema != "fs" {
            return Err(Error::BadArgument(
                "Only support schema file:// or fs://".to_string(),
            ));
        }

        let local_file = dsn.path();

        let mut stage_location = stage_location.to_owned();
        if !stage_location.ends_with('/') {
            stage_location.push('/');
        }

        let list_sql = format!("list {stage_location}");
        let mut response = self.query_iter(&list_sql).await?;

        let path_pos = stage_location.find('/').unwrap_or(stage_location.len());
        let stage_name = &stage_location[..path_pos];

        let mut stage_path = stage_location[path_pos..].to_string();
        stage_path = stage_path.trim_start_matches(|p| p == '/').to_owned();

        let mut results = Vec::new();
        while let Some(row) = response.next().await {
            let (mut name, _, _, _, _): (String, u64, Option<String>, String, Option<String>) =
                row.unwrap().try_into().unwrap();

            let presign_sql = format!("PRESIGN DOWNLOAD {stage_name}/{name}");
            let mut resp = self.query_iter(&presign_sql).await?;
            while let Some(r) = resp.next().await {
                let (_, headers, url): (String, String, String) = r.unwrap().try_into().unwrap();

                if !stage_path.is_empty() && name.starts_with(&stage_path) {
                    name = name[stage_path.len()..].to_string();
                }

                let headers: BTreeMap<String, String> = serde_json::from_str(&headers)?;
                let local_path = Path::new(local_file).join(&name);
                if let Some(p) = local_path.parent() {
                    std::fs::create_dir_all(p)?;
                }

                let mut builder = self.client.cli.get(url);
                for (k, v) in headers {
                    builder = builder.header(k, v);
                }

                let resp = builder.send().await.map_err(|err| Error::Api(err.into()))?;
                let status = resp.status();
                let body = resp.bytes().await.map_err(|err| Error::Api(err.into()))?;
                let status = match status {
                    StatusCode::OK => {
                        let mut file = tokio::fs::File::create(&local_path).await?;
                        file.write_all(&body).await?;
                        file.flush().await?;
                        Ok("SUCCESS".to_string())
                    }
                    _ => Err(format!(
                        "Presigned download Failed: {}",
                        String::from_utf8_lossy(&body)
                    )),
                };
                let status = status.unwrap_or_else(|err| err.to_string());

                results.push(Ok(RowWithProgress::Row(Row::from_vec(vec![
                    Value::String(local_path.to_string_lossy().to_string()),
                    Value::String(status),
                ]))));
            }
        }

        Ok((
            put_get_schema(),
            RowProgressIterator::new(Box::pin(futures::stream::iter(results))),
        ))
    }
}

fn put_get_schema() -> Schema {
    Schema::from_vec(vec![
        Field {
            name: "file".to_string(),
            data_type: DataType::String,
        },
        Field {
            name: "status".to_string(),
            data_type: DataType::String,
        },
    ])
}

impl<'o> RestAPIConnection {
    pub async fn try_create(dsn: &str) -> Result<Self> {
        let client = APIClient::from_dsn(dsn).await?;
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

    fn default_file_format_options() -> BTreeMap<&'o str, &'o str> {
        vec![
            ("type", "CSV"),
            ("field_delimiter", ","),
            ("record_delimiter", "\n"),
            ("skip_header", "0"),
        ]
        .into_iter()
        .collect()
    }

    fn default_copy_options() -> BTreeMap<&'o str, &'o str> {
        vec![("purge", "true")].into_iter().collect()
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
                    let progress = QueryProgress::from(resp.stats.progresses);
                    Poll::Ready(Some(Ok(RowWithProgress::Progress(progress))))
                }
                Poll::Ready(Err(e)) => {
                    self.next_page = None;
                    Poll::Ready(Some(Err(e)))
                }
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
