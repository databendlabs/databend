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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::ipc::{convert::fb_to_schema, root_as_message};
use arrow_flight::decode::FlightDataDecoder;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use percent_encoding::percent_decode_str;
use tokio::sync::Mutex;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use url::Url;

use databend_client::auth::SensitiveString;
use databend_client::presign::{presign_upload_to_stage, PresignedResponse};
use databend_driver_core::error::{Error, Result};
use databend_driver_core::rows::{
    Row, RowIterator, RowStatsIterator, RowWithStats, Rows, ServerStats,
};
use databend_driver_core::schema::Schema;

use crate::conn::{Connection, ConnectionInfo, Reader};

#[derive(Clone)]
pub struct FlightSQLConnection {
    client: Arc<Mutex<FlightSqlServiceClient<Channel>>>,
    handshaked: Arc<Mutex<bool>>,
    args: Args,
}

#[async_trait]
impl Connection for FlightSQLConnection {
    async fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            handler: "FlightSQL".to_string(),
            host: self.args.host.clone(),
            port: self.args.port,
            user: self.args.user.clone(),
            database: self.args.database.clone(),
            warehouse: self.args.warehouse.clone(),
        }
    }

    async fn exec(&self, sql: &str) -> Result<i64> {
        self.handshake().await?;
        let mut client = self.client.lock().await;
        let affected_rows = client.execute_update(sql.to_string(), None).await?;
        Ok(affected_rows)
    }

    async fn query_iter(&self, sql: &str) -> Result<RowIterator> {
        let rows_with_progress = self.query_iter_ext(sql).await?;
        let rows = rows_with_progress.filter_rows().await;
        Ok(rows)
    }

    async fn query_iter_ext(&self, sql: &str) -> Result<RowStatsIterator> {
        self.handshake().await?;
        let mut client = self.client.lock().await;
        let mut stmt = client.prepare(sql.to_string(), None).await?;
        let flight_info = stmt.execute().await?;
        let ticket = flight_info.endpoint[0]
            .ticket
            .as_ref()
            .ok_or_else(|| Error::Protocol("Ticket is empty".to_string()))?;
        let flight_data = client.do_get(ticket.clone()).await?.into_inner();
        let (schema, rows) = FlightSQLRows::try_from_flight_data(flight_data).await?;
        Ok(RowStatsIterator::new(Arc::new(schema), Box::pin(rows)))
    }

    async fn get_presigned_url(&self, operation: &str, stage: &str) -> Result<PresignedResponse> {
        let sql = format!("PRESIGN {} {}", operation, stage);
        let row = self.query_row(&sql).await?.ok_or_else(|| {
            Error::InvalidResponse("Empty response from server for presigned request".to_string())
        })?;
        let (method, headers, url): (String, String, String) =
            row.try_into().map_err(Error::Parsing)?;
        let headers: BTreeMap<String, String> = serde_json::from_str(&headers)?;
        Ok(PresignedResponse {
            method,
            headers,
            url,
        })
    }

    /// Always use presigned url to upload stage for FlightSQL
    async fn upload_to_stage(&self, stage: &str, data: Reader, size: u64) -> Result<()> {
        let presign = self.get_presigned_url("UPLOAD", stage).await?;
        presign_upload_to_stage(presign, data, size).await?;
        Ok(())
    }

    async fn load_data(
        &self,
        _sql: &str,
        _data: Reader,
        _size: u64,
        _file_format_options: Option<BTreeMap<&str, &str>>,
        _copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<ServerStats> {
        Err(Error::Protocol(
            "LOAD DATA unavailable for FlightSQL".to_string(),
        ))
    }

    async fn load_file(
        &self,
        _sql: &str,
        _fp: &Path,
        _format_options: BTreeMap<&str, &str>,
        _copy_options: Option<BTreeMap<&str, &str>>,
    ) -> Result<ServerStats> {
        Err(Error::Protocol(
            "LOAD FILE unavailable for FlightSQL".to_string(),
        ))
    }

    async fn stream_load(&self, _sql: &str, _data: Vec<Vec<&str>>) -> Result<ServerStats> {
        Err(Error::Protocol(
            "STREAM LOAD unavailable for FlightSQL".to_string(),
        ))
    }
}

impl FlightSQLConnection {
    pub async fn try_create(dsn: &str, name: String) -> Result<Self> {
        let (args, endpoint) = Self::parse_dsn(dsn, name).await?;
        let channel = endpoint.connect_lazy();
        let mut client = FlightSqlServiceClient::new(channel);
        // enable progress
        client.set_header("bendsql", "1");
        if let Some(tenant) = args.tenant.as_ref() {
            client.set_header("x-databend-tenant", tenant);
        }
        if let Some(warehouse) = args.warehouse.as_ref() {
            client.set_header("x-databend-warehouse", warehouse);
        }
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            args,
            handshaked: Arc::new(Mutex::new(false)),
        })
    }

    async fn handshake(&self) -> Result<()> {
        let mut handshaked = self.handshaked.lock().await;
        if *handshaked {
            return Ok(());
        }
        let mut client = self.client.lock().await;
        let _token = client
            .handshake(&self.args.user, self.args.password.inner())
            .await?;
        *handshaked = true;
        Ok(())
    }

    async fn parse_dsn(dsn: &str, name: String) -> Result<(Args, Endpoint)> {
        let u = Url::parse(dsn)?;
        let args = Args::from_url(&u)?;
        let mut endpoint = Endpoint::new(args.uri.clone())?
            .user_agent(name)?
            .connect_timeout(args.connect_timeout)
            .timeout(args.query_timeout)
            .tcp_nodelay(args.tcp_nodelay)
            .tcp_keepalive(args.tcp_keepalive)
            .http2_keep_alive_interval(args.http2_keep_alive_interval)
            .keep_alive_timeout(args.keep_alive_timeout)
            .keep_alive_while_idle(args.keep_alive_while_idle);
        #[cfg(any(feature = "rustls", feature = "native-tls"))]
        if args.tls {
            let tls_config = match args.tls_ca_file {
                None => ClientTlsConfig::new(),
                Some(ref ca_file) => {
                    let pem = tokio::fs::read(ca_file).await?;
                    let cert = tonic::transport::Certificate::from_pem(pem);
                    ClientTlsConfig::new().ca_certificate(cert)
                }
            };
            endpoint = endpoint.tls_config(tls_config)?;
        }
        Ok((args, endpoint))
    }
}

#[derive(Clone, Debug)]
struct Args {
    uri: String,
    host: String,
    port: u16,
    user: String,
    password: SensitiveString,
    database: Option<String>,
    tenant: Option<String>,
    warehouse: Option<String>,
    tls: bool,
    tls_ca_file: Option<String>,
    connect_timeout: Duration,
    query_timeout: Duration,
    tcp_nodelay: bool,
    // Disable Nagle's Algorithm since we don't want packets to wait
    tcp_keepalive: Option<Duration>,
    http2_keep_alive_interval: Duration,
    keep_alive_timeout: Duration,
    keep_alive_while_idle: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            uri: "https://localhost:8900".to_string(),
            host: "localhost".to_string(),
            port: 8900,
            database: None,
            tenant: None,
            warehouse: None,
            tls: true,
            tls_ca_file: None,
            user: "root".to_string(),
            password: SensitiveString::from(""),
            connect_timeout: Duration::from_secs(20),
            query_timeout: Duration::from_secs(60),
            tcp_nodelay: true,
            tcp_keepalive: Some(Duration::from_secs(3600)),
            http2_keep_alive_interval: Duration::from_secs(300),
            keep_alive_timeout: Duration::from_secs(20),
            keep_alive_while_idle: true,
        }
    }
}

impl Args {
    fn from_url(u: &Url) -> Result<Self> {
        let mut args = Self::default();
        let mut scheme = "https";
        for (k, v) in u.query_pairs() {
            match k.as_ref() {
                "tenant" => args.tenant = Some(v.to_string()),
                "warehouse" => args.warehouse = Some(v.to_string()),
                "sslmode" => match v.as_ref() {
                    "disable" => {
                        scheme = "http";
                        args.tls = false;
                    }
                    "require" | "enable" => {
                        scheme = "https";
                        args.tls = true;
                    }
                    _ => {
                        return Err(Error::BadArgument(format!(
                            "Invalid value for sslmode: {}",
                            v.as_ref()
                        )))
                    }
                },
                "tls_ca_file" => args.tls_ca_file = Some(v.to_string()),
                "connect_timeout" => args.connect_timeout = Duration::from_secs(v.parse()?),
                "query_timeout" => args.query_timeout = Duration::from_secs(v.parse()?),
                "tcp_nodelay" => args.tcp_nodelay = v.parse()?,
                "tcp_keepalive" => {
                    args.tcp_keepalive = {
                        match v.as_ref() {
                            "0" | "close" => None,
                            _ => Some(Duration::from_secs(v.parse()?)),
                        }
                    }
                }
                "http2_keep_alive_interval" => {
                    args.http2_keep_alive_interval = Duration::from_secs(v.parse()?)
                }
                "keep_alive_timeout" => args.keep_alive_timeout = Duration::from_secs(v.parse()?),
                "keep_alive_while_idle" => args.keep_alive_while_idle = v.parse()?,
                _ => {}
            }
        }
        u.path().split('/').filter(|s| !s.is_empty()).for_each(|s| {
            if args.database.is_none() {
                args.database = Some(s.to_string());
            }
        });
        let host = u
            .host()
            .ok_or_else(|| Error::BadArgument("Host is empty".to_string()))?;
        args.host = host.to_string();
        let port = u
            .port()
            .ok_or_else(|| Error::BadArgument("Port is empty".to_string()))?;
        args.port = port;
        args.uri = match args.database {
            Some(ref db) => format!("{}://{}:{}/{}", scheme, host, port, db),
            None => format!("{}://{}:{}", scheme, host, port),
        };
        args.user = u.username().to_string();
        let password = u.password().unwrap_or_default();
        let password = percent_decode_str(password).decode_utf8()?;
        args.password = SensitiveString::from(password.to_string());
        Ok(args)
    }
}

pub struct FlightSQLRows {
    schema: ArrowSchemaRef,
    data: FlightDataDecoder,
    rows: VecDeque<Row>,
}

impl FlightSQLRows {
    async fn try_from_flight_data(flight_data: FlightDataDecoder) -> Result<(Schema, Self)> {
        let mut data = flight_data;
        let datum = data
            .try_next()
            .await
            .map_err(|err| Error::Protocol(format!("Read flight data failed: {err:?}")))?
            .ok_or_else(|| Error::Protocol("No flight data in stream".to_string()))?;
        let message = root_as_message(&datum.inner.data_header[..])
            .map_err(|err| Error::Protocol(format!("InvalidFlatbuffer: {}", err)))?;
        let ipc_schema = message.header_as_schema().ok_or_else(|| {
            Error::Protocol("Invalid Message: Cannot get header as Schema".to_string())
        })?;
        let arrow_schema = Arc::new(fb_to_schema(ipc_schema));
        let schema = arrow_schema.clone().try_into()?;
        let rows = Self {
            schema: arrow_schema,
            data,
            rows: VecDeque::new(),
        };
        Ok((schema, rows))
    }
}

impl Stream for FlightSQLRows {
    type Item = Result<RowWithStats>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(row) = self.rows.pop_front() {
            return Poll::Ready(Some(Ok(RowWithStats::Row(row))));
        }
        match Pin::new(&mut self.data).poll_next(cx) {
            Poll::Ready(Some(Ok(datum))) => {
                // magic number 1 is used to indicate progress
                if datum.inner.app_metadata[..] == [0x01] {
                    let ss: ServerStats = serde_json::from_slice(&datum.inner.data_body)?;
                    Poll::Ready(Some(Ok(RowWithStats::Stats(ss))))
                } else {
                    let dicitionaries_by_id = HashMap::new();
                    let batch = flight_data_to_arrow_batch(
                        &datum.inner,
                        self.schema.clone(),
                        &dicitionaries_by_id,
                    )?;
                    let rows = Rows::try_from(batch)?;
                    self.rows.extend(rows);
                    self.poll_next(cx)
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(Error::Transport(format!(
                "fetch flight sql rows failed: {err:?}"
            ))))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
