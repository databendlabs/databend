# clickhouse-srv

[![](http://meritbadge.herokuapp.com/clickhouse-srv)](https://crates.io/crates/clickhouse-srv)
[![](https://img.shields.io/crates/d/clickhouse-srv.svg)](https://crates.io/crates/clickhouse-srv)
[![](https://img.shields.io/crates/dv/clickhouse-srv.svg)](https://crates.io/crates/clickhouse-srv)
[![](https://docs.rs/clickhouse-srv/badge.svg)](https://docs.rs/clickhouse-srv/)

Bindings for emulating a ClickHouse server.

```rust

use std::env;
use std::error::Error;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use common_clickhouse_srv::connection::Connection;
use common_clickhouse_srv::errors::Result;
use common_clickhouse_srv::types::Block;
use common_clickhouse_srv::types::Progress;
use common_clickhouse_srv::CHContext;
use common_clickhouse_srv::ClickHouseServer;
use futures::task::Context;
use futures::task::Poll;
use futures::Stream;
use futures::StreamExt;
use log::debug;
use log::info;
use tokio::net::TcpListener;

extern crate common_clickhouse_srv;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn Error>> {
    env::set_var("RUST_LOG", "clickhouse_srv=debug");
    env_logger::init();
    let host_port = "127.0.0.1:9000";

    // Note that this is the Tokio TcpListener, which is fully async.
    let listener = TcpListener::bind(host_port).await?;

    info!("Server start at {}", host_port);

    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, _) = listener.accept().await?;

        // Spawn our handler to be run asynchronously.
        tokio::spawn(async move {
            if let Err(e) = ClickHouseServer::run_on_stream(
                Arc::new(Session {
                    last_progress_send: Instant::now()
                }),
                stream
            )
            .await
            {
                println!("Error: {:?}", e);
            }
        });
    }
}

struct Session {
    last_progress_send: Instant
}

#[async_trait::async_trait]
impl common_clickhouse_srv::ClickHouseSession for Session {
    async fn execute_query(&self, ctx: &mut CHContext, connection: &mut Connection) -> Result<()> {
        let query = ctx.state.query.clone();
        info!("Receive query {}", query);

        let start = Instant::now();
        let mut clickhouse_stream = SimpleBlockStream {
            idx: 0,
            start: 10,
            end: 24,
            blocks: 10
        };

        while let Some(block) = clickhouse_stream.next().await {
            let block = block?;
            connection.write_block(&block).await?;

            if self.last_progress_send.elapsed() >= Duration::from_millis(10) {
                let progress = self.get_progress();
                connection
                    .write_progress(progress, ctx.client_revision)
                    .await?;
            }
        }

        let duration = start.elapsed();
        debug!(
            "ClickHouseHandler executor cost:{:?}, statistics:{:?}",
            duration, "xxx",
        );
        Ok(())
    }

    fn dbms_name(&self) -> &str {
        "ClickHouse-X"
    }

    fn dbms_version_major(&self) -> u64 {
        2021
    }

    fn dbms_version_minor(&self) -> u64 {
        5
    }

    // the MIN_SERVER_REVISION for suggestions is 54406
    fn dbms_tcp_protocol_version(&self) -> u64 {
        54405
    }

    fn timezone(&self) -> &str {
        "UTC"
    }

    fn server_display_name(&self) -> &str {
        "ClickHouse-X"
    }

    fn dbms_version_patch(&self) -> u64 {
        0
    }

    fn get_progress(&self) -> Progress {
        Progress {
            rows: 100,
            bytes: 1000,
            total_rows: 1000
        }
    }
}

struct SimpleBlockStream {
    idx: u32,
    start: u32,
    end: u32,
    blocks: u32
}

impl Stream for SimpleBlockStream {
    type Item = Result<Block>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>
    ) -> Poll<Option<Self::Item>> {
        self.idx += 1;
        if self.idx > self.blocks {
            return Poll::Ready(None);
        }
        let block = Some(Block::new().column("abc", (self.start..self.end).collect::<Vec<u32>>()));

        thread::sleep(Duration::from_millis(100));
        Poll::Ready(block.map(Ok))
    }
}


```