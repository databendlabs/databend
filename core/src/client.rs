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

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use reqwest::header::HeaderMap;
use reqwest::Client as HttpClient;
use url::Url;

use crate::{
    request::{PaginationConfig, QueryRequest, SessionConfig},
    response::QueryResponse,
};

#[derive(Clone)]
pub struct APIClient {
    cli: HttpClient,
    endpoint: Url,
    host: String,

    tenant: Option<String>,
    warehouse: Option<String>,
    database: Option<String>,
    user: String,
    password: Option<String>,
    session_settings: BTreeMap<String, String>,

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
        client.password = u.password().map(|s| s.to_string());
        client.database = match u.path().trim_start_matches('/') {
            "" => None,
            s => Some(s.to_string()),
        };

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
                    client.presigned_url_disabled = v.parse()?;
                }
                "tenant" => {
                    client.tenant = Some(v.to_string());
                }
                "warehouse" => {
                    client.warehouse = Some(v.to_string());
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

        let port = match u.port() {
            Some(p) => p,
            None => match scheme {
                "http" => 80,
                "https" => 443,
                _ => unreachable!(),
            },
        };
        client.endpoint = Url::parse(&format!("{}://{}:{}", scheme, client.host, port))?;
        client.session_settings = session_settings;

        Ok(client)
    }

    pub async fn query(&self, sql: String) -> Result<QueryResponse> {
        let req = QueryRequest::new(sql)
            .with_pagination(self.make_pagination())
            .with_session(self.make_session());
        let endpoint = self.endpoint.join("v1/query")?;

        let resp: QueryResponse = self
            .cli
            .post(endpoint)
            .json(&req)
            .basic_auth(self.user.clone(), self.password.clone())
            .headers(self.make_headers()?)
            .send()
            .await?
            .json()
            .await?;
        match resp.error {
            Some(err) => Err(anyhow!("Query error {}: {}", err.code, err.message)),
            // TODO:(everpcpc) update session configs
            None => Ok(resp),
        }
    }

    pub async fn query_page(&self, next_uri: String) -> Result<QueryResponse> {
        let endpoint = self.endpoint.join(&next_uri)?;
        let resp: QueryResponse = self
            .cli
            .get(endpoint)
            .basic_auth(self.user.clone(), self.password.clone())
            .headers(self.make_headers()?)
            .send()
            .await?
            .json()
            .await?;
        match resp.error {
            Some(err) => Err(anyhow!("Query page error {}: {}", err.code, err.message)),
            None => Ok(resp),
        }
    }

    fn make_session(&self) -> Option<SessionConfig> {
        if self.database.is_none() && self.session_settings.is_empty() {
            return None;
        }
        let mut session = SessionConfig {
            database: None,
            settings: None,
        };
        if self.database.is_some() {
            session.database = self.database.clone();
        }
        if !self.session_settings.is_empty() {
            session.settings = Some(self.session_settings.clone());
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

    fn make_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(tenant) = &self.tenant {
            headers.insert("X-DATABEND-TENANT", tenant.parse()?);
        }
        if let Some(warehouse) = &self.warehouse {
            headers.insert("X-DATABEND-WAREHOUSE", warehouse.parse()?);
        }
        Ok(headers)
    }
}

impl Default for APIClient {
    fn default() -> Self {
        Self {
            cli: HttpClient::new(),
            endpoint: Url::parse("http://localhost:8080").unwrap(),
            host: "localhost".to_string(),
            tenant: None,
            warehouse: None,
            database: None,
            user: "root".to_string(),
            password: None,
            session_settings: BTreeMap::new(),
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
    fn test_parse_dsn() -> Result<()> {
        let dsn = "databend://username:password@app.databend.com/test?wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&warehouse=wh&sslmode=disable";
        let client = APIClient::from_dsn(dsn)?;
        assert_eq!(client.host, "app.databend.com");
        assert_eq!(client.endpoint, Url::parse("http://app.databend.com:80")?);
        assert_eq!(client.user, "username");
        assert_eq!(client.password, Some("password".to_string()));
        assert_eq!(client.database, Some("test".to_string()));
        assert_eq!(client.wait_time_secs, Some(10));
        assert_eq!(client.max_rows_in_buffer, Some(5000000));
        assert_eq!(client.max_rows_per_page, Some(10000));
        assert_eq!(client.tenant, None);
        assert_eq!(client.warehouse, Some("wh".to_string()));
        Ok(())
    }
}
