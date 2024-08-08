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

mod http_client;
mod mysql_client;

use std::borrow::Cow;
use std::fmt;

pub use http_client::HttpClient;
pub use mysql_client::MySQLClient;
use rand::distributions::Alphanumeric;
use rand::Rng;
use regex::Regex;
use sqllogictest::DBOutput;
use sqllogictest::DefaultColumnType;

use crate::error::Result;

#[derive(Debug, Clone, Copy)]
pub enum ClientType {
    MySQL,
    Http,
}

impl fmt::Display for ClientType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

pub enum Client {
    MySQL(MySQLClient),
    Http(HttpClient),
}

impl Client {
    pub async fn query(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>> {
        let sql = replace_rand_values(sql);
        match self {
            Client::MySQL(client) => client.query(&sql).await,
            Client::Http(client) => client.query(&sql).await,
        }
    }

    pub fn enable_debug(&mut self) {
        match self {
            Client::MySQL(client) => client.debug = true,
            Client::Http(client) => client.debug = true,
        }
    }

    // Create sandbox tenant and create default database for the tenant
    pub async fn create_sandbox(&mut self) -> Result<()> {
        let sandbox_name: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();
        self.query(format!("set sandbox_tenant = \'{sandbox_name}\'").as_str())
            .await?;
        self.query("create database if not exists default").await?;
        Ok(())
    }

    pub fn engine_name(&self) -> &str {
        match self {
            Client::MySQL(_) => "mysql",
            Client::Http(_) => "http",
        }
    }

    pub async fn set_table_lock_expire_secs(&mut self, secs: u64) -> Result<()> {
        let sql = format!("set global table_lock_expire_secs = {}", secs);
        self.query(sql.as_str()).await?;
        Ok(())
    }
}

fn replace_rand_values(input: &str) -> Cow<'_, str> {
    let re = Regex::new(r"\$RAND_(\d+)_(\d+)").unwrap();
    re.replace_all(input, |caps: &regex::Captures| {
        let m: usize = caps[1].parse().unwrap();
        let n: usize = caps[2].parse().unwrap();
        let mut rng = rand::thread_rng();
        let rand_value = rng.gen_range(m..n);
        rand_value.to_string()
    })
}
