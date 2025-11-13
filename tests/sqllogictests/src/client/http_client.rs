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

use regex::Regex;
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
    schema: Vec<SchemaItem>,
    data: Option<serde_json::Value>,
    next_uri: Option<String>,

    error: Option<serde_json::Value>,
}

#[derive(serde::Deserialize, Debug)]
struct SchemaItem {
    #[allow(dead_code)]
    pub name: String,
    pub r#type: String,
}

impl SchemaItem {
    fn parse_type(&self) -> Result<DefaultColumnType> {
        let nullable = Regex::new(r"^Nullable\((.+)\)$").unwrap();
        let value = match nullable.captures(&self.r#type) {
            Some(captures) => {
                let (_, [value]) = captures.extract();
                value
            }
            None => &self.r#type,
        };
        let typ = match value {
            "String" => DefaultColumnType::Text,
            "Int8" | "Int16" | "Int32" | "Int64" | "UInt8" | "UInt16" | "UInt32" | "UInt64" => {
                DefaultColumnType::Integer
            }
            "Float32" | "Float64" => DefaultColumnType::FloatingPoint,
            decimal if decimal.starts_with("Decimal") => DefaultColumnType::FloatingPoint,
            _ => DefaultColumnType::Any,
        };
        Ok(typ)
    }
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
        let port = self.port;
        let mut response = self
            .post_query(sql, &format!("http://127.0.0.1:{port}/v1/query"))
            .await?;

        let mut schema = std::mem::take(&mut response.schema);
        let mut parsed_rows = vec![];

        loop {
            self.handle_response(&response, &mut parsed_rows)?;
            if let Some(error) = &response.error {
                return Err(format_error(error.clone()).into());
            }

            match &response.next_uri {
                Some(next_uri) => {
                    let url = format!("http://127.0.0.1:{port}{next_uri}");
                    let mut new_response = self.poll_query_result(&url).await?;
                    if schema.is_empty() && !new_response.schema.is_empty() {
                        schema = std::mem::take(&mut new_response.schema);
                    }
                    response = new_response;
                }
                None => break,
            }
        }

        if self.debug {
            println!(
                "Running sql with http client: [{sql}] ({:?})",
                start.elapsed()
            );
        }

        let types = schema
            .iter()
            .map(|item| item.parse_type().unwrap_or(DefaultColumnType::Any))
            .collect();

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
