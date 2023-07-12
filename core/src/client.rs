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

use std::sync::Arc;
use std::{collections::BTreeMap, fmt};

use http::StatusCode;
use percent_encoding::percent_decode_str;
use reqwest::header::HeaderMap;
use reqwest::multipart::{Form, Part};
use reqwest::{Body, Client as HttpClient};
use tokio::io::AsyncRead;
use tokio::sync::Mutex;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use tokio_util::io::ReaderStream;
use url::Url;

use crate::{
    error::{Error, Result},
    request::{PaginationConfig, QueryRequest, SessionConfig, StageAttachmentConfig},
    response::{QueryError, QueryResponse},
};

pub struct PresignedResponse {
    pub method: String,
    pub headers: BTreeMap<String, String>,
    pub url: String,
}

pub struct StageLocation {
    pub name: String,
    pub path: String,
}

impl fmt::Display for StageLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}/{}", self.name, self.path)
    }
}

impl TryFrom<&str> for StageLocation {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self> {
        if !s.starts_with('@') {
            return Err(Error::Parsing(format!("Invalid stage location: {}", s)));
        }
        let mut parts = s.splitn(2, '/');
        let name = parts
            .next()
            .ok_or_else(|| Error::Parsing(format!("Invalid stage location: {}", s)))?
            .trim_start_matches('@');
        let path = parts
            .next()
            .ok_or_else(|| Error::Parsing(format!("Invalid stage location: {}", s)))?;
        Ok(Self {
            name: name.to_string(),
            path: path.to_string(),
        })
    }
}

#[derive(Clone)]
pub struct APIClient {
    cli: HttpClient,
    endpoint: Url,
    pub host: String,
    pub port: u16,
    pub user: String,
    password: Option<String>,

    tenant: Option<String>,
    warehouse: Arc<Mutex<Option<String>>>,
    database: Arc<Mutex<Option<String>>>,
    session_settings: Arc<Mutex<BTreeMap<String, String>>>,

    wait_time_secs: Option<i64>,
    max_rows_in_buffer: Option<i64>,
    max_rows_per_page: Option<i64>,

    presigned_url_disabled: bool,
}

impl APIClient {
    pub fn from_dsn(dsn: &str) -> Result<Self> {
        let u = Url::parse(dsn)?;
        let mut client = Self::default();
        if let Some(host) = u.host_str() {
            client.host = host.to_string();
        }
        client.user = u.username().to_string();
        client.password = u
            .password()
            .map(|s| percent_decode_str(s).decode_utf8_lossy().to_string());
        let database = match u.path().trim_start_matches('/') {
            "" => None,
            s => Some(s.to_string()),
        };
        client.database = Arc::new(Mutex::new(database));
        let mut scheme = "https";
        let mut session_settings = BTreeMap::new();
        for (k, v) in u.query_pairs() {
            match k.as_ref() {
                "wait_time_secs" => {
                    client.wait_time_secs = Some(v.parse()?);
                }
                "max_rows_in_buffer" => {
                    client.max_rows_in_buffer = Some(v.parse()?);
                }
                "max_rows_per_page" => {
                    client.max_rows_per_page = Some(v.parse()?);
                }
                "presigned_url_disabled" => {
                    client.presigned_url_disabled = match v.as_ref() {
                        "true" | "1" => true,
                        "false" | "0" => false,
                        _ => {
                            return Err(Error::BadArgument(format!(
                                "Invalid value for presigned_url_disabled: {}",
                                v
                            )))
                        }
                    }
                }
                "tenant" => {
                    client.tenant = Some(v.to_string());
                }
                "warehouse" => {
                    client.warehouse = Arc::new(Mutex::new(Some(v.to_string())));
                }
                "sslmode" => {
                    if v == "disable" {
                        scheme = "http";
                    }
                }
                _ => {
                    session_settings.insert(k.to_string(), v.to_string());
                }
            }
        }
        client.port = match u.port() {
            Some(p) => p,
            None => match scheme {
                "http" => 80,
                "https" => 443,
                _ => unreachable!(),
            },
        };
        client.endpoint = Url::parse(&format!("{}://{}:{}", scheme, client.host, client.port))?;
        client.session_settings = Arc::new(Mutex::new(session_settings));

        Ok(client)
    }

    pub async fn current_warehouse(&self) -> Option<String> {
        let guard = self.warehouse.lock().await;
        guard.clone()
    }

    pub async fn current_database(&self) -> Option<String> {
        let guard = self.database.lock().await;
        guard.clone()
    }

    pub async fn query(&self, sql: &str) -> Result<QueryResponse> {
        let session_settings = self.make_session().await;
        let req = QueryRequest::new(sql)
            .with_pagination(self.make_pagination())
            .with_session(session_settings);
        let endpoint = self.endpoint.join("v1/query")?;
        let headers = self.make_headers().await?;
        let resp = self
            .cli
            .post(endpoint)
            .json(&req)
            .basic_auth(self.user.clone(), self.password.clone())
            .headers(headers)
            .send()
            .await?;
        if resp.status() != StatusCode::OK {
            let resp_err = QueryError {
                code: resp.status().as_u16(),
                message: resp.text().await?,
            };
            return Err(Error::InvalidResponse(resp_err));
        }
        let resp: QueryResponse = resp.json().await?;
        if let Some(err) = resp.error {
            return Err(Error::InvalidResponse(err));
        }
        let mut session_settings = self.session_settings.lock().await;
        if let Some(session) = &resp.session {
            if session.database.is_some() {
                let mut database = self.database.lock().await;
                *database = session.database.clone();
            }
            if let Some(settings) = &session.settings {
                for (k, v) in settings {
                    match k.as_str() {
                        "warehouse" => {
                            let mut warehouse = self.warehouse.lock().await;
                            *warehouse = Some(v.clone());
                        }
                        _ => {
                            session_settings.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }
        Ok(resp)
    }

    pub async fn query_page(&self, next_uri: &str) -> Result<QueryResponse> {
        let endpoint = self.endpoint.join(next_uri)?;
        let headers = self.make_headers().await?;
        let retry_strategy = ExponentialBackoff::from_millis(10).map(jitter).take(3);
        let req = || async {
            self.cli
                .get(endpoint.clone())
                .basic_auth(self.user.clone(), self.password.clone())
                .headers(headers.clone())
                .send()
                .await
        };
        let resp = Retry::spawn(retry_strategy, req).await?;
        if resp.status() != StatusCode::OK {
            let resp_err = QueryError {
                code: resp.status().as_u16(),
                message: resp.text().await?,
            };
            return Err(Error::InvalidResponse(resp_err));
        }
        let resp: QueryResponse = resp.json().await?;
        match resp.error {
            Some(err) => Err(Error::InvalidPage(err)),
            None => Ok(resp),
        }
    }

    pub async fn wait_for_query(&self, resp: QueryResponse) -> Result<QueryResponse> {
        if let Some(next_uri) = &resp.next_uri {
            let schema = resp.schema;
            let mut data = resp.data;
            let mut resp = self.query_page(next_uri).await?;
            while let Some(next_uri) = &resp.next_uri {
                resp = self.query_page(next_uri).await?;
                data.append(&mut resp.data);
            }
            resp.schema = schema;
            resp.data = data;
            Ok(resp)
        } else {
            Ok(resp)
        }
    }

    pub async fn query_wait(&self, sql: &str) -> Result<QueryResponse> {
        let resp = self.query(sql).await?;
        self.wait_for_query(resp).await
    }

    async fn make_session(&self) -> Option<SessionConfig> {
        let session_settings = self.session_settings.lock().await;
        let database = self.database.lock().await;
        if database.is_none() && session_settings.is_empty() {
            return None;
        }
        let mut session = SessionConfig {
            database: None,
            settings: None,
        };
        if database.is_some() {
            session.database = database.clone();
        }
        if !session_settings.is_empty() {
            session.settings = Some(session_settings.clone());
        }
        Some(session)
    }

    fn make_pagination(&self) -> Option<PaginationConfig> {
        if self.wait_time_secs.is_none()
            && self.max_rows_in_buffer.is_none()
            && self.max_rows_per_page.is_none()
        {
            return None;
        }
        let mut pagination = PaginationConfig {
            wait_time_secs: None,
            max_rows_in_buffer: None,
            max_rows_per_page: None,
        };
        if let Some(wait_time_secs) = self.wait_time_secs {
            pagination.wait_time_secs = Some(wait_time_secs);
        }
        if let Some(max_rows_in_buffer) = self.max_rows_in_buffer {
            pagination.max_rows_in_buffer = Some(max_rows_in_buffer);
        }
        if let Some(max_rows_per_page) = self.max_rows_per_page {
            pagination.max_rows_per_page = Some(max_rows_per_page);
        }
        Some(pagination)
    }

    async fn make_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(tenant) = &self.tenant {
            headers.insert("X-DATABEND-TENANT", tenant.parse()?);
        }
        let warehouse = self.warehouse.lock().await;
        if let Some(warehouse) = &*warehouse {
            headers.insert("X-DATABEND-WAREHOUSE", warehouse.parse()?);
        }
        Ok(headers)
    }

    pub async fn insert_with_stage(
        &self,
        sql: &str,
        stage_location: &str,
        file_format_options: BTreeMap<&str, &str>,
        copy_options: BTreeMap<&str, &str>,
    ) -> Result<QueryResponse> {
        let session_settings = self.make_session().await;
        let stage_attachment = Some(StageAttachmentConfig {
            location: stage_location,
            file_format_options: Some(file_format_options),
            copy_options: Some(copy_options),
        });
        let req = QueryRequest::new(sql)
            .with_pagination(self.make_pagination())
            .with_session(session_settings)
            .with_stage_attachment(stage_attachment);
        let endpoint = self.endpoint.join("v1/query")?;
        let headers = self.make_headers().await?;
        let resp = self
            .cli
            .post(endpoint)
            .json(&req)
            .basic_auth(self.user.clone(), self.password.clone())
            .headers(headers)
            .send()
            .await?;
        if resp.status() != StatusCode::OK {
            let resp_err = QueryError {
                code: resp.status().as_u16(),
                message: resp.text().await?,
            };
            return Err(Error::InvalidResponse(resp_err));
        }
        let resp: QueryResponse = resp.json().await?;
        let resp = self.wait_for_query(resp).await?;
        Ok(resp)
    }

    pub async fn upload_to_stage(
        &self,
        stage_location: &str,
        data: impl AsyncRead + Send + Sync + 'static,
        size: u64,
    ) -> Result<()> {
        if self.presigned_url_disabled {
            self.upload_to_stage_with_stream(stage_location, data, size)
                .await
        } else {
            self.upload_to_stage_with_presigned(stage_location, data, size)
                .await
        }
    }

    async fn upload_to_stage_with_stream(
        &self,
        stage_location: &str,
        data: impl AsyncRead + Send + Sync + 'static,
        size: u64,
    ) -> Result<()> {
        let endpoint = self.endpoint.join("v1/upload_to_stage")?;
        let location = StageLocation::try_from(stage_location)?;
        let mut headers = self.make_headers().await?;
        headers.insert("stage_name", location.name.parse()?);
        let stream = Body::wrap_stream(ReaderStream::new(data));
        let part = Part::stream_with_length(stream, size).file_name(location.path);
        let form = Form::new().part("upload", part);
        let resp = self
            .cli
            .put(endpoint)
            .basic_auth(self.user.clone(), self.password.clone())
            .headers(headers)
            .multipart(form)
            .send()
            .await?;

        let status = resp.status();
        let body = resp.bytes().await?;
        match status {
            StatusCode::OK => Ok(()),
            _ => Err(Error::Request(format!(
                "Stage Upload Failed: {}",
                String::from_utf8_lossy(&body)
            ))),
        }
    }

    async fn upload_to_stage_with_presigned(
        &self,
        stage_location: &str,
        data: impl AsyncRead + Send + Sync + 'static,
        size: u64,
    ) -> Result<()> {
        let presigned = self.get_presigned_url(stage_location).await?;
        let mut builder = self.cli.put(presigned.url);
        for (k, v) in presigned.headers {
            builder = builder.header(k, v);
        }
        builder = builder.header("Content-Length", size.to_string());
        let stream = Body::wrap_stream(ReaderStream::new(data));
        let resp = builder.body(stream).send().await?;
        let status = resp.status();
        let body = resp.bytes().await?;
        match status {
            StatusCode::OK => Ok(()),
            _ => Err(Error::Request(format!(
                "Presigned Upload Failed: {}",
                String::from_utf8_lossy(&body)
            ))),
        }
    }

    async fn get_presigned_url(&self, stage_location: &str) -> Result<PresignedResponse> {
        let sql = format!("PRESIGN UPLOAD {}", stage_location);
        let resp = self.query_wait(&sql).await?;
        if resp.data.len() != 1 {
            return Err(Error::Request(
                "Empty response from server for presigned request".to_string(),
            ));
        }
        if resp.data[0].len() != 3 {
            return Err(Error::Request(
                "Invalid response from server for presigned request".to_string(),
            ));
        }
        // resp.data[0]: [ "PUT", "{\"host\":\"s3.us-east-2.amazonaws.com\"}", "https://s3.us-east-2.amazonaws.com/query-storage-xxxxx/tnxxxxx/stage/user/xxxx/xxx?" ]
        let method = resp.data[0][0].clone();
        let headers: BTreeMap<String, String> =
            serde_json::from_str(resp.data[0][1].clone().as_str())?;
        let url = resp.data[0][2].clone();
        if method != "PUT" {
            return Err(Error::Request(format!(
                "Invalid method {} for presigned request",
                method
            )));
        }
        Ok(PresignedResponse {
            method,
            headers,
            url,
        })
    }
}

impl Default for APIClient {
    fn default() -> Self {
        Self {
            cli: HttpClient::new(),
            endpoint: Url::parse("http://localhost:8080").unwrap(),
            host: "localhost".to_string(),
            port: 8000,
            tenant: None,
            warehouse: Arc::new(Mutex::new(None)),
            database: Arc::new(Mutex::new(None)),
            user: "root".to_string(),
            password: None,
            session_settings: Arc::new(Mutex::new(BTreeMap::new())),
            wait_time_secs: None,
            max_rows_in_buffer: None,
            max_rows_per_page: None,
            presigned_url_disabled: false,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_dsn() -> Result<()> {
        let dsn = "databend://username:password@app.databend.com/test?wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&warehouse=wh&sslmode=disable";
        let client = APIClient::from_dsn(dsn)?;
        assert_eq!(client.host, "app.databend.com");
        assert_eq!(client.endpoint, Url::parse("http://app.databend.com:80")?);
        assert_eq!(client.user, "username");
        assert_eq!(client.password, Some("password".to_string()));
        assert_eq!(
            *client.database.try_lock().unwrap(),
            Some("test".to_string())
        );
        assert_eq!(client.wait_time_secs, Some(10));
        assert_eq!(client.max_rows_in_buffer, Some(5000000));
        assert_eq!(client.max_rows_per_page, Some(10000));
        assert_eq!(client.tenant, None);
        assert_eq!(
            *client.warehouse.try_lock().unwrap(),
            Some("wh".to_string())
        );
        Ok(())
    }

    #[test]
    fn parse_encoded_password() -> Result<()> {
        let dsn = "databend://username:3a%40SC(nYE1k%3D%7B%7BR@localhost";
        let client = APIClient::from_dsn(dsn)?;
        assert_eq!(client.password, Some("3a@SC(nYE1k={{R".to_string()));
        Ok(())
    }

    #[test]
    fn parse_special_chars_password() -> Result<()> {
        let dsn = "databend://username:3a@SC(nYE1k={{R@localhost:8000";
        let client = APIClient::from_dsn(dsn)?;
        assert_eq!(client.password, Some("3a@SC(nYE1k={{R".to_string()));
        Ok(())
    }

    #[test]
    fn parse_stage() -> Result<()> {
        let location = "@stage_name/path/to/file";
        let stage_location = StageLocation::try_from(location)?;
        assert_eq!(stage_location.name, "stage_name");
        assert_eq!(stage_location.path, "path/to/file");
        Ok(())
    }

    #[test]
    fn parse_stage_fail() -> Result<()> {
        let location = "stage_name/path/to/file";
        let stage_location = StageLocation::try_from(location);
        assert!(stage_location.is_err());
        Ok(())
    }
}
