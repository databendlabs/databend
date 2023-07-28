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

use async_trait::async_trait;
use dyn_clone::DynClone;
use tokio::io::AsyncRead;
use url::Url;

#[cfg(feature = "flight-sql")]
use crate::flight_sql::FlightSQLConnection;

use crate::error::{Error, Result};
use crate::rest_api::RestAPIConnection;
use crate::rows::{Row, RowIterator, RowProgressIterator};
use crate::schema::Schema;
use crate::QueryProgress;

pub struct Client {
    dsn: String,
}

impl<'c> Client {
    pub fn new(dsn: String) -> Self {
        Self { dsn }
    }

    pub async fn get_conn(&self) -> Result<Box<dyn Connection>> {
        let u = Url::parse(&self.dsn)?;
        match u.scheme() {
            "databend" | "databend+http" | "databend+https" => {
                let conn = RestAPIConnection::try_create(&self.dsn).await?;
                Ok(Box::new(conn))
            }
            #[cfg(feature = "flight-sql")]
            "databend+flight" | "databend+grpc" => {
                let conn = FlightSQLConnection::try_create(&self.dsn).await?;
                Ok(Box::new(conn))
            }
            _ => Err(Error::Parsing(format!(
                "Unsupported scheme: {}",
                u.scheme()
            ))),
        }
    }
}

pub struct ConnectionInfo {
    pub handler: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: Option<String>,
    pub warehouse: Option<String>,
}

pub type Reader = Box<dyn AsyncRead + Send + Sync + Unpin + 'static>;

#[async_trait]
pub trait Connection: DynClone + Send + Sync {
    async fn info(&self) -> ConnectionInfo;

    async fn version(&self) -> Result<String> {
        let row = self.query_row("SELECT version()").await?;
        let version = match row {
            Some(row) => {
                let (version,): (String,) = row.try_into()?;
                version
            }
            None => "".to_string(),
        };
        Ok(version)
    }

    async fn exec(&self, sql: &str) -> Result<i64>;
    async fn query_row(&self, sql: &str) -> Result<Option<Row>>;
    async fn query_iter(&self, sql: &str) -> Result<RowIterator>;
    async fn query_iter_ext(&self, sql: &str) -> Result<(Schema, RowProgressIterator)>;

    async fn stream_load(
        &self,
        sql: &str,
        data: Reader,
        size: u64,
        file_format_options: Option<BTreeMap<&str, &str>>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<QueryProgress>;
}
dyn_clone::clone_trait_object!(Connection);
