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
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use tokio::io::AsyncRead;
use tokio_stream::StreamExt;
use url::Url;

#[cfg(feature = "flight-sql")]
use crate::flight_sql::FlightSQLConnection;

use databend_client::presign::{presign_download_from_stage, PresignedResponse};
use databend_client::stage::StageLocation;
use databend_driver_core::error::{Error, Result};
use databend_driver_core::rows::{Row, RowIterator, RowStatsIterator, RowWithStats, ServerStats};
use databend_driver_core::schema::{DataType, Field, NumberDataType, Schema};
use databend_driver_core::value::{NumberValue, Value};

use crate::rest_api::RestAPIConnection;

static VERSION: Lazy<String> = Lazy::new(|| {
    let version = option_env!("CARGO_PKG_VERSION").unwrap_or("unknown");
    version.to_string()
});

#[derive(Clone)]
pub struct Client {
    dsn: String,
    name: String,
}

impl Client {
    pub fn new(dsn: String) -> Self {
        let name = format!("databend-driver-rust/{}", VERSION.as_str());
        Self { dsn, name }
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    pub async fn get_conn(&self) -> Result<Box<dyn Connection>> {
        let u = Url::parse(&self.dsn)?;
        match u.scheme() {
            "databend" | "databend+http" | "databend+https" => {
                let conn = RestAPIConnection::try_create(&self.dsn, self.name.clone()).await?;
                Ok(Box::new(conn))
            }
            #[cfg(feature = "flight-sql")]
            "databend+flight" | "databend+grpc" => {
                let conn = FlightSQLConnection::try_create(&self.dsn, self.name.clone()).await?;
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
                let (version,): (String,) = row.try_into().map_err(Error::Parsing)?;
                version
            }
            None => "".to_string(),
        };
        Ok(version)
    }

    async fn exec(&self, sql: &str) -> Result<i64>;
    async fn query_iter(&self, sql: &str) -> Result<RowIterator>;
    async fn query_iter_ext(&self, sql: &str) -> Result<RowStatsIterator>;

    async fn query_row(&self, sql: &str) -> Result<Option<Row>> {
        let rows = self.query_all(sql).await?;
        let row = rows.into_iter().next();
        Ok(row)
    }

    async fn query_all(&self, sql: &str) -> Result<Vec<Row>> {
        let rows = self.query_iter(sql).await?;
        rows.collect().await
    }

    /// Get presigned url for a given operation and stage location.
    /// The operation can be "UPLOAD" or "DOWNLOAD".
    async fn get_presigned_url(&self, operation: &str, stage: &str) -> Result<PresignedResponse>;

    async fn upload_to_stage(&self, stage: &str, data: Reader, size: u64) -> Result<()>;

    async fn load_data(
        &self,
        sql: &str,
        data: Reader,
        size: u64,
        file_format_options: Option<BTreeMap<&str, &str>>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<ServerStats>;
    async fn load_file(
        &self,
        sql: &str,
        fp: &Path,
        format_options: BTreeMap<&str, &str>,
        copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<ServerStats>;
    async fn stream_load(&self, sql: &str, data: Vec<Vec<&str>>) -> Result<ServerStats>;

    // PUT file://<path_to_file>/<filename> internalStage|externalStage
    async fn put_files(&self, local_file: &str, stage: &str) -> Result<RowStatsIterator> {
        let mut total_count: usize = 0;
        let mut total_size: usize = 0;
        let local_dsn = url::Url::parse(local_file)?;
        validate_local_scheme(local_dsn.scheme())?;
        let mut results = Vec::new();
        let stage_location = StageLocation::try_from(stage)?;
        for entry in glob::glob(local_dsn.path())? {
            let entry = entry?;
            let filename = entry
                .file_name()
                .ok_or_else(|| Error::BadArgument(format!("Invalid local file path: {:?}", entry)))?
                .to_str()
                .ok_or_else(|| {
                    Error::BadArgument(format!("Invalid local file path: {:?}", entry))
                })?;
            let stage_file = stage_location.file_path(filename);
            let data = tokio::fs::File::open(&entry).await?;
            let size = data.metadata().await?.len();
            let (fname, status) = match self
                .upload_to_stage(&stage_file, Box::new(data), size)
                .await
            {
                Ok(_) => {
                    total_count += 1;
                    total_size += size as usize;
                    (entry.to_string_lossy().to_string(), "SUCCESS".to_owned())
                }
                Err(e) => (entry.to_string_lossy().to_string(), e.to_string()),
            };
            let ss = ServerStats {
                total_rows: 0,
                total_bytes: 0,
                read_rows: 0,
                read_bytes: 0,
                write_rows: total_count,
                write_bytes: total_size,
                running_time_ms: 0.0,
            };
            results.push(Ok(RowWithStats::Stats(ss)));
            results.push(Ok(RowWithStats::Row(Row::from_vec(vec![
                Value::String(fname),
                Value::String(status),
                Value::Number(NumberValue::UInt64(size)),
            ]))));
        }
        let schema = put_get_schema();
        Ok(RowStatsIterator::new(
            Arc::new(schema),
            Box::pin(tokio_stream::iter(results)),
        ))
    }

    async fn get_files(&self, stage: &str, local_file: &str) -> Result<RowStatsIterator> {
        let mut total_count: usize = 0;
        let mut total_size: usize = 0;
        let local_dsn = url::Url::parse(local_file)?;
        validate_local_scheme(local_dsn.scheme())?;
        let mut location = StageLocation::try_from(stage)?;
        if !location.path.ends_with('/') {
            location.path.push('/');
        }
        let list_sql = format!("LIST {}", location);
        let mut response = self.query_iter(&list_sql).await?;
        let mut results = Vec::new();
        while let Some(row) = response.next().await {
            let (mut name, _, _, _, _): (String, u64, Option<String>, String, Option<String>) =
                row?.try_into().map_err(Error::Parsing)?;
            if !location.path.is_empty() && name.starts_with(&location.path) {
                name = name[location.path.len()..].to_string();
            }
            let stage_file = format!("{}/{}", location, name);
            let presign = self.get_presigned_url("DOWNLOAD", &stage_file).await?;
            let local_file = Path::new(local_dsn.path()).join(&name);
            let status = presign_download_from_stage(presign, &local_file).await;
            let (status, size) = match status {
                Ok(size) => {
                    total_count += 1;
                    total_size += size as usize;
                    ("SUCCESS".to_owned(), size)
                }
                Err(e) => (e.to_string(), 0),
            };
            let ss = ServerStats {
                total_rows: 0,
                total_bytes: 0,
                read_rows: total_count,
                read_bytes: total_size,
                write_rows: 0,
                write_bytes: 0,
                running_time_ms: 0.0,
            };
            results.push(Ok(RowWithStats::Stats(ss)));
            results.push(Ok(RowWithStats::Row(Row::from_vec(vec![
                Value::String(local_file.to_string_lossy().to_string()),
                Value::String(status),
                Value::Number(NumberValue::UInt64(size)),
            ]))));
        }
        let schema = put_get_schema();
        Ok(RowStatsIterator::new(
            Arc::new(schema),
            Box::pin(tokio_stream::iter(results)),
        ))
    }
}
dyn_clone::clone_trait_object!(Connection);

fn put_get_schema() -> Schema {
    Schema::from_vec(vec![
        Field {
            name: "file".to_string(),
            data_type: DataType::String,
        },
        Field {
            name: "status".to_string(),
            data_type: DataType::String,
        },
        Field {
            name: "size".to_string(),
            data_type: DataType::Number(NumberDataType::UInt64),
        },
    ])
}

fn validate_local_scheme(scheme: &str) -> Result<()> {
    match scheme {
        "file" | "fs" => Ok(()),
        _ => Err(Error::BadArgument(
            "Supported schemes: file:// or fs://".to_string(),
        )),
    }
}
