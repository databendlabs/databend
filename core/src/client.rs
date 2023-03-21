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

use anyhow::Result;
use url::Url;

#[derive(Clone)]
pub struct APIClient {
    // cli: reqwest::Client,
    endpoint: Url,
    host: String,

    tenant: Option<String>,
    warehouse: Option<String>,
    database: String,
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
            "" => "default".to_string(),
            s => s.to_string(),
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
}

impl Default for APIClient {
    fn default() -> Self {
        Self {
            // cli: reqwest::Client::new(),
            endpoint: Url::parse("http://localhost:8080").unwrap(),
            host: "localhost".to_string(),
            tenant: None,
            warehouse: None,
            database: "default".to_string(),
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
        assert_eq!(client.database, "test");
        assert_eq!(client.wait_time_secs, Some(10));
        assert_eq!(client.max_rows_in_buffer, Some(5000000));
        assert_eq!(client.max_rows_per_page, Some(10000));
        assert_eq!(client.tenant, None);
        assert_eq!(client.warehouse, Some("wh".to_string()));
        Ok(())
    }
}
