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

use anyhow::Result;
use async_trait::async_trait;
use dyn_clone::DynClone;

use crate::rest_api::RestAPIConnection;
use crate::rows::{Row, RowIterator};

#[async_trait]
pub trait Connection: DynClone {
    async fn exec(&self, sql: &str) -> Result<()>;
    async fn query_iter(&self, sql: &str) -> Result<RowIterator>;
    async fn query_row(&self, sql: &str) -> Result<Option<Row>>;
}
dyn_clone::clone_trait_object!(Connection);

pub fn new_connection(dsn: &str) -> Result<Box<dyn Connection>> {
    let uri = dsn.parse::<http::Uri>()?;
    let scheme = uri.scheme_str();
    match scheme {
        None => Err(anyhow::anyhow!("Invalid DSN: {}", dsn)),
        Some("databend") | Some("databend+http") | Some("databend+https") => {
            let conn = RestAPIConnection::try_create(dsn)?;
            Ok(Box::new(conn))
        }
        Some(s) => Err(anyhow::anyhow!("Unsupported scheme: {}", s)),
    }
}

pub enum DatabendConnection {
    RestAPI(RestAPIConnection),
}

impl DatabendConnection {
    pub fn try_create(dsn: &str) -> Result<Self> {
        let uri = dsn.parse::<http::Uri>()?;
        let scheme = uri.scheme_str();
        match scheme {
            None => Err(anyhow::anyhow!("Invalid DSN: {}", dsn)),
            Some("databend") | Some("databend+http") | Some("databend+https") => {
                let conn = RestAPIConnection::try_create(dsn)?;
                Ok(Self::RestAPI(conn))
            }
            Some(s) => Err(anyhow::anyhow!("Unsupported scheme: {}", s)),
        }
    }

    pub async fn exec(&self, sql: &str) -> Result<()> {
        match self {
            Self::RestAPI(conn) => conn.exec(sql).await,
        }
    }

    pub async fn query_iter(&self, sql: &str) -> Result<RowIterator> {
        match self {
            Self::RestAPI(conn) => conn.query_iter(sql).await,
        }
    }

    pub async fn query_row(&self, sql: &str) -> Result<Option<Row>> {
        match self {
            Self::RestAPI(conn) => conn.query_row(sql).await,
        }
    }
}
