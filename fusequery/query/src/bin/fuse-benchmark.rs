// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use clickhouse_rs::row;
use clickhouse_rs::types::Block;
use clickhouse_rs::ClientHandle;
use clickhouse_rs::Pool;
use common_exception::ErrorCodes;
use common_exception::Result;
use crossbeam_queue::ArrayQueue;
use futures::future::try_join_all;
use futures::join;
use futures::StreamExt;
use log::info;
use structopt::StructOpt;
use structopt_toml::StructOptToml;

#[derive(Clone, Debug, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
#[serde(default)]
pub struct Config {
    #[structopt(short, long, default_value = "")]
    pub query: String,

    #[structopt(long, short = "i", default_value = "0")]
    pub itertaions: usize,
    #[structopt(long, short = "c", default_value = "1")]
    pub concurrency: usize
}

impl Config {
    /// Load configs from args.
    pub fn load_from_args() -> Self {
        let cfg = Config::from_args();
        cfg
    }
}

struct Benchmark {
    config: Config,
    queue: ArrayQueue<String>,
    shutdown: AtomicBool,
    executed: AtomicUsize
}

#[tokio::main]
async fn main() -> Result<()> {
    // First load configs from args.
    let conf = Config::load_from_args();
    let benchmark = Benchmark::new(conf);

    benchmark.run().await?;
    Ok(())
}

impl Benchmark {
    pub fn new(config: Config) -> Self {
        let queue = ArrayQueue::new(config.concurrency);
        Self {
            config,
            queue,
            shutdown: AtomicBool::new(false),
            executed: AtomicUsize::new(0)
        }
    }
    async fn run(&self) -> Result<()> {
        let database_url = "tcp://localhost:9000?compression=lz4";
        let pool = Pool::new(database_url);

        let mut executors = vec![];
        let query = "select number from numbers(10)";

        for i in 0..self.config.concurrency {
            executors.push(self.execute(&pool))
        }

        let mut i = 0;
        let max_iterations = self.config.itertaions;
        loop {
            if max_iterations > 0 && i >= max_iterations {
                break;
            }

            dbg!("push one query");
            self.queue.push(query.to_string());
            i += 1;
        }

        for i in 0..self.config.itertaions {}

        try_join_all(executors).await?;
        Ok(())
    }

    async fn execute(&self, pool: &Pool) -> Result<()> {
        loop {
            let query = self.queue.pop();
            if let Some(query) = query {
                dbg!("got one query");

                let mut client = pool.get_handle().await.map_err(to_error_codes)?;
                let result = client.query(&query);

                let mut stream = result.stream();
                while let Some(row) = stream.next().await {
                    let row = row.map_err(to_error_codes)?;
                    let number: Option<u64> = row.get("number").map_err(to_error_codes)?;
                    println!("Found payment {:?}", number);
                }

                self.executed.fetch_add(1, Ordering::Relaxed);
            } else {
                if self.shutdown.load(Ordering::Relaxed)
                    || (self.config.itertaions > 0
                        && self.executed.load(Ordering::Relaxed) >= self.config.itertaions)
                {
                    dbg!("got break query");
                    break;
                }
            }
        }
        Ok(())
    }
}

fn to_error_codes(err: clickhouse_rs::errors::Error) -> ErrorCodes {
    anyhow::anyhow!(err.to_string()).into()
}
