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

use reqwest::Client;
use sqllogictest::ColumnType;
use sqllogictest::DBOutput;

use crate::error::Result;
use crate::util::SET_SQL_RE;
use crate::util::UNSET_SQL_RE;
use crate::util::USE_SQL_RE;

pub struct ClickhouseHttpClient {
    pub client: Client,
    pub database: String,
    pub settings: HashMap<String, String>,
    pub url: String,
}

impl ClickhouseHttpClient {
    pub fn create() -> Result<ClickhouseHttpClient> {
        let client = Client::new();
        let url = "http://127.0.0.1:8124".to_string();
        Ok(ClickhouseHttpClient {
            client,
            database: "default".to_string(),
            settings: HashMap::new(),
            url,
        })
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput> {
        // Client will save the following info: use database, settings (session level info)
        // Then send them to server, so even though the session changes, database and settings context is correct
        if let Some(captures) = USE_SQL_RE.captures(sql) {
            self.database = captures.name("db").unwrap().as_str().to_string();
        }
        if let Some(captures) = SET_SQL_RE.captures(sql) {
            let key = captures.name("key").unwrap().as_str().to_string();
            let value = captures.name("value").unwrap().as_str().to_string();
            self.settings
                .entry(key)
                .and_modify(|v| *v = value.clone())
                .or_insert(value);
        }
        if let Some(captures) = UNSET_SQL_RE.captures(sql) {
            let key = captures.name("key").unwrap().as_str();
            self.settings.remove(key);
        }

        let mut query = HashMap::new();
        query.insert("query", sql);
        query.insert("database", self.database.as_str());
        if !self.settings.is_empty() {
            query.extend(
                self.settings
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            );
        }
        let response = self
            .client
            .post(&self.url)
            .query(&query)
            .basic_auth("root", Some(""))
            .send()
            .await?;
        // `res` is tsv format
        // Todo: find a better way to parse tsv
        let res = response.text().await?;
        let rows: Vec<Vec<String>> = res
            .lines()
            .map(|s| {
                s.split('\t')
                    .map(|s| {
                        if s == "\\N" {
                            "NULL".to_string()
                        } else if s == "inf" {
                            "Infinity".to_string()
                        } else if s == "nan" {
                            "NaN".to_string()
                        } else {
                            if s.is_empty() {
                                return "(empty)".to_string();
                            }
                            // Maybe `s` contains "\\N", such as `[\N,'cc']`
                            // So we need to find it and replace with NULL (a little hack)
                            let mut s = str::replace(s, "\\N", "NULL");
                            // Remove escape characters (a little hack)
                            s = str::replace(&s, r"\n", " ");
                            str::replace(&s, r"\", "")
                        }
                    })
                    .collect()
            })
            .collect();

        // Todo: add types to compare
        let mut types = vec![];
        if !rows.is_empty() {
            types = vec![ColumnType::Any; rows[0].len()];
        }
        Ok(DBOutput::Rows { types, rows })
    }
}
