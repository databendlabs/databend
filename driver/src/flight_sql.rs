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

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::ipc::{convert::fb_to_schema, root_as_message};
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{sql::client::FlightSqlServiceClient, FlightData};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::{ClientTlsConfig, Endpoint};
use tonic::Streaming;
use url::Url;

use crate::error::{Error, Result};
use crate::rows::{IterProgress, Row, RowIterator, RowProgressIterator, RowWithProgress, Rows};
use crate::Connection;

#[derive(Clone)]
pub struct FlightSQLConnection {
    pub(crate) client: FlightSqlServiceClient,
}

#[async_trait]
impl Connection for FlightSQLConnection {
    async fn exec(&mut self, sql: &str) -> Result<()> {
        let _info = self.client.execute_update(sql.to_string()).await?;
        Ok(())
    }

    async fn query_iter(&mut self, sql: &str) -> Result<RowIterator> {
        let rows_with_progress = self.query_iter_with_progress(sql).await?;
        let rows = rows_with_progress.filter_map(|r| match r {
            Ok(RowWithProgress::Row(r)) => Some(Ok(r)),
            Ok(RowWithProgress::Progress(_)) => None,
            Err(err) => Some(Err(err)),
        });
        Ok(Box::pin(rows))
    }

    async fn query_iter_with_progress(&mut self, sql: &str) -> Result<RowProgressIterator> {
        let mut stmt = self.client.prepare(sql.to_string()).await?;
        let flight_info = stmt.execute().await?;
        let ticket = flight_info.endpoint[0]
            .ticket
            .as_ref()
            .ok_or(Error::Protocol("Ticket is empty".to_string()))?;
        let flight_data = self.client.do_get(ticket.clone()).await?;
        let rows = FlightSQLRows::from_flight_data(flight_data).await?;
        Ok(Box::pin(rows))
    }

    async fn query_row(&mut self, sql: &str) -> Result<Option<Row>> {
        let mut rows = self.query_iter(sql).await?;
        let row = rows.try_next().await?;
        Ok(row)
    }
}

impl FlightSQLConnection {
    pub async fn try_create(dsn: &str) -> Result<Self> {
        let (args, endpoint) = Self::parse_dsn(dsn)?;
        let channel = endpoint.connect().await?;
        let mut client = FlightSqlServiceClient::new(channel);
        // enable progress
        client.set_header("bendsql", "1");
        let _token = client.handshake(&args.user, &args.password).await?;
        Ok(Self { client })
    }

    fn parse_dsn(dsn: &str) -> Result<(Args, Endpoint)> {
        let u = Url::parse(dsn)?;
        let args = Args::from_url(&u)?;
        let mut endpoint = Endpoint::new(args.uri.clone())?
            .connect_timeout(args.connect_timeout)
            .timeout(args.query_timeout)
            .tcp_nodelay(args.tcp_nodelay)
            .tcp_keepalive(args.tcp_keepalive)
            .http2_keep_alive_interval(args.http2_keep_alive_interval)
            .keep_alive_timeout(args.keep_alive_timeout)
            .keep_alive_while_idle(args.keep_alive_while_idle);
        if args.tls {
            let tls_config = ClientTlsConfig::new();
            endpoint = endpoint.tls_config(tls_config)?;
        }
        Ok((args, endpoint))
    }
}

struct Args {
    uri: String,
    user: String,
    password: String,
    tls: bool,
    connect_timeout: Duration,
    query_timeout: Duration,
    tcp_nodelay: bool, // Disable Nagle's Algorithm since we don't want packets to wait
    tcp_keepalive: Option<Duration>,
    http2_keep_alive_interval: Duration,
    keep_alive_timeout: Duration,
    keep_alive_while_idle: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            uri: "https://localhost:8900".to_string(),
            tls: true,
            user: "root".to_string(),
            password: "".to_string(),
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
                "sslmode" => {
                    if v == "disable" {
                        scheme = "http";
                        args.tls = false;
                    }
                }
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
        let host = u
            .host()
            .ok_or(Error::BadArgument("Host is empty".to_string()))?;
        let port = u
            .port()
            .ok_or(Error::BadArgument("Port is empty".to_string()))?;
        args.uri = format!("{}://{}:{}", scheme, host, port);
        args.user = u.username().to_string();
        args.password = u.password().unwrap_or_default().to_string();
        Ok(args)
    }
}

pub struct FlightSQLRows {
    schema: SchemaRef,
    data: Streaming<FlightData>,
    rows: VecDeque<Row>,
}

impl FlightSQLRows {
    async fn from_flight_data(flight_data: Streaming<FlightData>) -> Result<Self> {
        let mut data = flight_data;
        let datum = data
            .try_next()
            .await?
            .ok_or(Error::Protocol("No flight data in stream".to_string()))?;
        let message = root_as_message(&datum.data_header[..])
            .map_err(|err| Error::Protocol(format!("InvalidFlatbuffer: {}", err)))?;
        let ipc_schema = message.header_as_schema().ok_or(Error::Protocol(
            "Invalid Message: Cannot get header as Schema".to_string(),
        ))?;
        let schema = fb_to_schema(ipc_schema);
        Ok(Self {
            schema: Arc::new(schema),
            data: data,
            rows: VecDeque::new(),
        })
    }
}

impl Stream for FlightSQLRows {
    type Item = Result<RowWithProgress>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(row) = self.rows.pop_front() {
            return Poll::Ready(Some(Ok(RowWithProgress::Row(row))));
        }
        match Pin::new(&mut self.data).poll_next(cx) {
            Poll::Ready(Some(Ok(datum))) => {
                // magic number 1 is used to indicate progress
                if datum.app_metadata[..] == [0x01] {
                    let progress: IterProgress = serde_json::from_slice(&datum.data_body)?;
                    Poll::Ready(Some(Ok(RowWithProgress::Progress(progress))))
                } else {
                    let dicitionaries_by_id = HashMap::new();
                    let batch = flight_data_to_arrow_batch(
                        &datum,
                        self.schema.clone(),
                        &dicitionaries_by_id,
                    )?;
                    let rows = Rows::try_from(batch)?;
                    self.rows.extend(rows);
                    self.poll_next(cx)
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
