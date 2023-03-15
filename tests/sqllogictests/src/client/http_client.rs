// Copyright 2021 Datafuse Labs.
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
use std::time::Instant;

use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::Client;
use reqwest::ClientBuilder;
use sqllogictest::DBOutput;
use sqllogictest::DefaultColumnType;

use crate::error::Result;
use crate::util::parser_rows;
use crate::util::HttpSessionConf;

pub struct HttpClient {
    pub client: Client,
    pub debug: bool,
    pub session: Option<HttpSessionConf>,
}

#[derive(serde::Deserialize)]
struct QueryResponse {
    session: Option<HttpSessionConf>,
    data: serde_json::Value,
    next_uri: Option<String>,

    error: Option<serde_json::Value>,
}

impl HttpClient {
    pub fn create() -> Result<Self> {
        let mut header = HeaderMap::new();
        header.insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );
        header.insert("Accept", HeaderValue::from_str("application/json").unwrap());
        let client = ClientBuilder::new().default_headers(header).build()?;
        Ok(Self {
            client,
            session: None,
            debug: false,
        })
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>> {
        let start = Instant::now();

        let url = "http://127.0.0.1:8000/v1/query".to_string();
        let mut response = self.response(sql, &url, true).await?;
        // Set session from response to client
        // Then client will same session for different queries.

        if response.session.is_some() {
            self.session = response.session.clone();
        }

        if let Some(error) = response.error {
            return Err(format!("http query error: {error}").into());
        }

        let rows = response.data;
        let mut parsed_rows = parser_rows(&rows)?;
        while let Some(next_uri) = response.next_uri {
            let mut url = "http://127.0.0.1:8000".to_string();
            url.push_str(&next_uri);
            response = self.response(sql, &url, false).await?;
            let rows = response.data;
            parsed_rows.append(&mut parser_rows(&rows)?);
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

    // Send request and get response by json format
    async fn response(&mut self, sql: &str, url: &str, post: bool) -> Result<QueryResponse> {
        let mut query = HashMap::new();
        query.insert("sql", serde_json::to_value(sql)?);
        if let Some(session) = &self.session {
            query.insert("session", serde_json::to_value(session)?);
        }
        if post {
            return Ok(self
                .client
                .post(url)
                .json(&query)
                .basic_auth("root", Some(""))
                .send()
                .await?
                .json::<QueryResponse>()
                .await?);
        }
        Ok(self
            .client
            .get(url)
            .json(&query)
            .basic_auth("root", Some(""))
            .send()
            .await?
            .json::<QueryResponse>()
            .await?)
    }
}
