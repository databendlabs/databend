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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::Client;
use reqwest::ClientBuilder;
use serde::Deserialize;
use sqllogictest::DBOutput;
use sqllogictest::DefaultColumnType;

use crate::client::global_cookie_store::GlobalCookieStore;
use crate::error::Result;
use crate::util::parser_rows;
use crate::util::HttpSessionConf;

pub struct HttpClient {
    pub client: Client,
    pub session_token: String,
    pub debug: bool,
    pub session: Option<HttpSessionConf>,
    pub port: u16,
}

#[derive(serde::Deserialize, Debug)]
struct QueryResponse {
    session: Option<HttpSessionConf>,
    data: Option<serde_json::Value>,
    next_uri: Option<String>,

    error: Option<serde_json::Value>,
}

// make error message the same with ErrorCode::display
fn format_error(value: serde_json::Value) -> String {
    let value = value.as_object().unwrap();
    let detail = value.get("detail").and_then(|v| v.as_str());
    let code = value["code"].as_u64().unwrap();
    let message = value["message"].as_str().unwrap();
    if let Some(detail) = detail {
        format!(
            "http query error: code: {}, Text: {}\n{}",
            code, message, detail
        )
    } else {
        format!("http query error: code: {}, Text: {}", code, message)
    }
}

#[derive(Deserialize)]
struct TokenInfo {
    session_token: String,
}

#[derive(Deserialize)]
struct LoginResponse {
    tokens: Option<TokenInfo>,
}

impl HttpClient {
    pub async fn create(port: u16) -> Result<Self> {
        let mut header = HeaderMap::new();
        header.insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );
        header.insert("Accept", HeaderValue::from_str("application/json").unwrap());
        header.insert(
            "X-DATABEND-CLIENT-CAPS",
            HeaderValue::from_str("session_cookie").unwrap(),
        );
        let cookie_provider = GlobalCookieStore::new();
        let client = ClientBuilder::new()
            .cookie_provider(Arc::new(cookie_provider))
            .default_headers(header)
            // https://github.com/hyperium/hyper/issues/2136#issuecomment-589488526
            .http2_keep_alive_timeout(Duration::from_secs(15))
            .pool_max_idle_per_host(0)
            .build()?;

        let url = format!("http://127.0.0.1:{}/v1/session/login", port);

        let session_token = client
            .post(&url)
            .body("{}")
            .basic_auth("root", Some(""))
            .send()
            .await
            .inspect_err(|e| {
                println!("fail to send to {}: {:?}", url, e);
            })?
            .json::<LoginResponse>()
            .await
            .inspect_err(|e| {
                println!("fail to decode json when call {}: {:?}", url, e);
            })?
            .tokens
            .unwrap()
            .session_token;

        Ok(Self {
            client,
            session_token,
            session: None,
            debug: false,
            port,
        })
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>> {
        let start = Instant::now();

        let url = format!("http://127.0.0.1:{}/v1/query", self.port);
        let mut parsed_rows = vec![];
        let mut response = self.post_query(sql, &url).await?;
        self.handle_response(&response, &mut parsed_rows)?;
        while let Some(next_uri) = &response.next_uri {
            let url = format!("http://127.0.0.1:{}{next_uri}", self.port);
            let new_response = self.poll_query_result(&url).await?;
            if new_response.next_uri.is_some() {
                self.handle_response(&new_response, &mut parsed_rows)?;
                response = new_response;
            } else {
                break;
            }
        }
        if let Some(error) = response.error {
            return Err(format_error(error).into());
        }
        // Todo: add types to compare
        let mut types = vec![];
        if !parsed_rows.is_empty() {
            types = vec![DefaultColumnType::Any; parsed_rows[0].len()];
        }

        if self.debug {
            println!(
                "Running sql with http client: [{sql}] ({:?})",
                start.elapsed()
            );
        }

        Ok(DBOutput::Rows {
            types,
            rows: parsed_rows,
        })
    }

    fn handle_response(
        &mut self,
        response: &QueryResponse,
        parsed_rows: &mut Vec<Vec<String>>,
    ) -> Result<()> {
        if response.session.is_some() {
            self.session = response.session.clone();
        }
        if let Some(data) = &response.data {
            parsed_rows.append(&mut parser_rows(data)?);
        }
        Ok(())
    }

    // Send request and get response by json format
    async fn post_query(&self, sql: &str, url: &str) -> Result<QueryResponse> {
        let mut query = HashMap::new();
        query.insert("sql", serde_json::to_value(sql)?);
        if let Some(session) = &self.session {
            query.insert("session", serde_json::to_value(session)?);
        }
        Ok(self
            .client
            .post(url)
            .json(&query)
            .bearer_auth(&self.session_token)
            .send()
            .await
            .inspect_err(|e| {
                println!("fail to send to {}: {:?}", url, e);
            })?
            .json::<QueryResponse>()
            .await
            .inspect_err(|e| {
                println!("fail to decode json when call {}: {:?}", url, e);
            })?)
    }

    async fn poll_query_result(&self, url: &str) -> Result<QueryResponse> {
        Ok(self
            .client
            .get(url)
            .bearer_auth(&self.session_token)
            .send()
            .await
            .inspect_err(|e| {
                println!("fail to send to {}: {:?}", url, e);
            })?
            .json::<QueryResponse>()
            .await
            .inspect_err(|e| {
                println!("fail to decode json when call {}: {:?}", url, e);
            })?)
    }
}
