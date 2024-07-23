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

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use databend_client::auth::SensitiveString;
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConnectionArgs {
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub password: SensitiveString,
    pub database: Option<String>,
    pub flight: bool,
    pub args: BTreeMap<String, String>,
}

impl ConnectionArgs {
    pub fn get_dsn(self) -> Result<String> {
        let mut dsn = url::Url::parse("databend://")?;
        dsn.set_host(Some(&self.host))?;
        _ = dsn.set_port(self.port);
        _ = dsn.set_username(&self.user);
        let password = utf8_percent_encode(self.password.inner(), NON_ALPHANUMERIC).to_string();
        _ = dsn.set_password(Some(&password));
        if let Some(database) = self.database {
            dsn.set_path(&database);
        }
        if self.flight {
            _ = dsn.set_scheme("databend+flight");
        }
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        if !self.args.is_empty() {
            for (k, v) in self.args {
                query.append_pair(&k, &v);
            }
        }
        dsn.set_query(Some(&query.finish()));
        Ok(dsn.to_string())
    }

    pub fn from_dsn(dsn: &str) -> Result<Self> {
        let u = url::Url::parse(dsn)?;
        let mut args = BTreeMap::new();
        if let Some(query) = u.query() {
            for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
                args.insert(k.to_string(), v.to_string());
            }
        }
        let flight = matches!(u.scheme(), "databend+flight");
        let host = u.host_str().ok_or(anyhow!("missing host"))?.to_string();
        let port = u.port();
        let user = u.username().to_string();
        let password = u.password().unwrap_or_default();
        let password = percent_decode_str(password).decode_utf8()?.to_string();
        let password = SensitiveString::from(password);
        let database = u.path().strip_prefix('/').map(|s| s.to_string());
        Ok(Self {
            host,
            port,
            user,
            password,
            database,
            flight,
            args,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_dsn() -> Result<()> {
        let dsn = "databend://username:3a%40SC%28nYE%25a1k%3D%7B%7BR@app.databend.com/test?wait_time_secs=10&max_rows_in_buffer=5000000&max_rows_per_page=10000&warehouse=wh&sslmode=disable";
        let args = ConnectionArgs::from_dsn(dsn)?;
        let password_str = "3a@SC(nYE%a1k={{R";
        assert_eq!(args.password.inner(), password_str);
        let expected = ConnectionArgs {
            host: "app.databend.com".to_string(),
            port: None,
            user: "username".to_string(),
            password: SensitiveString::from(password_str),
            database: Some("test".to_string()),
            flight: false,
            args: {
                let mut args = BTreeMap::new();
                args.insert("wait_time_secs".to_string(), "10".to_string());
                args.insert("max_rows_in_buffer".to_string(), "5000000".to_string());
                args.insert("max_rows_per_page".to_string(), "10000".to_string());
                args.insert("warehouse".to_string(), "wh".to_string());
                args.insert("sslmode".to_string(), "disable".to_string());
                args
            },
        };
        assert_eq!(args, expected);
        Ok(())
    }

    #[test]
    fn format_dsn() -> Result<()> {
        let password_str = "3a@SC(nYE%a1k={{R";
        let args = ConnectionArgs {
            host: "app.databend.com".to_string(),
            port: Some(443),
            user: "username".to_string(),
            password: SensitiveString::from(password_str),
            database: Some("test".to_string()),
            flight: false,
            args: {
                let mut args = BTreeMap::new();
                args.insert("wait_time_secs".to_string(), "10".to_string());
                args.insert("max_rows_in_buffer".to_string(), "5000000".to_string());
                args.insert("max_rows_per_page".to_string(), "10000".to_string());
                args.insert("warehouse".to_string(), "wh".to_string());
                args.insert("sslmode".to_string(), "disable".to_string());
                args
            },
        };
        let dsn = args.get_dsn()?;
        let expected = "databend://username:3a%40SC%28nYE%25a1k%3D%7B%7BR@app.databend.com:443/test?max_rows_in_buffer=5000000&max_rows_per_page=10000&sslmode=disable&wait_time_secs=10&warehouse=wh";
        assert_eq!(dsn, expected);
        Ok(())
    }
}
