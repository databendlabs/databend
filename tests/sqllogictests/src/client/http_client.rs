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

use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::Client;
use reqwest::ClientBuilder;
use sqllogictest::ColumnType;
use sqllogictest::DBOutput;

use crate::error::Result;
use crate::util::parser_rows;
use crate::util::HttpSessionConf;

pub struct HttpClient {
    pub client: Client,
    pub session: Option<HttpSessionConf>,
}

impl HttpClient {
    pub fn create() -> Result<HttpClient> {
        let mut header = HeaderMap::new();
        header.insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );
        header.insert("Accept", HeaderValue::from_str("application/json").unwrap());
        let client = ClientBuilder::new().default_headers(header).build()?;
        Ok(HttpClient {
            client,
            session: None,
        })
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput> {
        let url = "http://127.0.0.1:8000/v1/query".to_string();
        let mut query = HashMap::new();
        query.insert("sql", serde_json::to_value(sql).unwrap());
        if let Some(session) = &self.session {
            query.insert("session", serde_json::to_value(session).unwrap());
        }
        let mut response = self.response(sql, &url, true).await?;
        // Set session from response to client
        // Then client will same session for different queries.
        self.session = serde_json::from_value(
            response
                .as_object()
                .unwrap()
                .get("session")
                .unwrap()
                .clone(),
        )?;
        let rows = response.as_object().unwrap().get("data").unwrap();
        let mut parsed_rows = parser_rows(rows)?;
        while let Some(next_uri) = response
            .as_object()
            .unwrap()
            .get("next_uri")
            .unwrap()
            .as_str()
        {
            let mut url = "http://127.0.0.1:8000".to_string();
            url.push_str(next_uri);
            response = self.response(sql, &url, false).await?;
            let rows = response.as_object().unwrap().get("data").unwrap();
            parsed_rows.append(&mut parser_rows(rows)?);
        }
        // Todo: add types to compare
        let mut types = vec![];
        if !parsed_rows.is_empty() {
            types = vec![ColumnType::Any; parsed_rows[0].len()];
        }
        Ok(DBOutput::Rows {
            types,
            rows: parsed_rows,
        })
    }

    // Send request and get response by json format
    async fn response(&mut self, sql: &str, url: &str, post: bool) -> Result<serde_json::Value> {
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
                .json::<serde_json::Value>()
                .await?);
        }
        Ok(self
            .client
            .get(url)
            .json(&query)
            .basic_auth("root", Some(""))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?)
    }
}
