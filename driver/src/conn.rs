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

use async_trait::async_trait;
use dyn_clone::DynClone;
use url::Url;

#[cfg(feature = "flight-sql")]
use crate::flight_sql::FlightSQLConnection;

use crate::error::{Error, Result};
use crate::rest_api::RestAPIConnection;
use crate::rows::{Row, RowIterator, RowProgressIterator};
use crate::schema::Schema;

pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: Option<String>,
}

#[async_trait]
pub trait Connection: DynClone + Send + Sync {
    fn info(&self) -> ConnectionInfo;
    async fn exec(&mut self, sql: &str) -> Result<i64>;
    async fn query_iter(&mut self, sql: &str) -> Result<RowIterator>;
    async fn query_iter_ext(&mut self, sql: &str) -> Result<(Schema, RowProgressIterator)>;
    async fn query_row(&mut self, sql: &str) -> Result<Option<Row>>;
}
dyn_clone::clone_trait_object!(Connection);

pub async fn new_connection(dsn: &str) -> Result<Box<dyn Connection>> {
    let u = Url::parse(dsn)?;
    match u.scheme() {
        "databend" | "databend+http" | "databend+https" => {
            let conn = RestAPIConnection::try_create(dsn)?;
            Ok(Box::new(conn))
        }
        #[cfg(feature = "flight-sql")]
        "databend+flight" | "databend+grpc" => {
            let conn = FlightSQLConnection::try_create(dsn).await?;
            Ok(Box::new(conn))
        }
        _ => Err(Error::Parsing(format!(
            "Unsupported scheme: {}",
            u.scheme()
        ))),
    }
}
