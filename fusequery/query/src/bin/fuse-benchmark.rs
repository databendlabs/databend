// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use core::fmt;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufWriter;
use std::io::Read;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use clickhouse_rs::Pool;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use common_runtime::tokio;
use crossbeam_queue::ArrayQueue;
use futures::future::try_join_all;
use futures::StreamExt;
use quantiles::ckms::CKMS;
use rand::Rng;
use structopt::StructOpt;
use structopt_toml::StructOptToml;

/// echo "select avg(number) from numbers(1000000)" |  ./target/debug/fuse-benchmark -c 1  -i 10
#[derive(Clone, Debug, serde::Deserialize, PartialEq, StructOpt, StructOptToml)]
#[serde(default)]
pub struct Config {
    #[structopt(long, default_value = "")]
    pub query: String,

    #[structopt(long, short = "h", default_value = "127.0.0.1")]
    pub host: String,
    #[structopt(long, short = "p", default_value = "9000")]
    pub port: u32,
    #[structopt(long, short = "i", default_value = "0")]
    pub iterations: usize,
    #[structopt(long, short = "c", default_value = "1")]
    pub concurrency: usize,
    #[structopt(long, default_value = "")]
    pub json: String,
}

impl Config {
    /// Load configs from args.
    pub fn load_from_args() -> Self {
        Config::from_args()
    }
}

type BenchmarkRef = Arc<Benchmark>;
struct Benchmark {
    config: Config,
    queue: Arc<ArrayQueue<String>>,
    shutdown: AtomicBool,
    executed: AtomicUsize,
    stats: Arc<RwLock<Stats>>,
    queries: Vec<String>,
    database_url: String,
}

impl Benchmark {
    pub fn new(config: Config, queries: Vec<String>, database_url: String) -> Self {
        let queue = Arc::new(ArrayQueue::new(config.concurrency));
        Self {
            config,
            queue,
            shutdown: AtomicBool::new(false),
            executed: AtomicUsize::new(0),
            stats: Arc::new(RwLock::new(Stats::new())),
            queries,
            database_url,
        }
    }
}

#[derive(Clone, Debug)]
struct Stats {
    queries: usize,
    errors: usize,
    //seconds
    work_time: f64,
    read_rows: usize,
    read_bytes: usize,
    sample: CKMS<f64>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            queries: 0,
            errors: 0,
            work_time: 0f64,
            read_rows: 0,
            read_bytes: 0,
            sample: CKMS::<f64>::new(0.001),
        }
    }
    pub fn update(&mut self, elapsed: f64, read_rows: usize, read_bytes: usize) {
        self.read_rows += read_rows;
        self.read_bytes += read_bytes;
        self.work_time += elapsed;
        self.queries += 1;

        self.sample.insert(elapsed);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // First load configs from args.
    let conf = Config::load_from_args();
    let database_url = format!("tcp://{}:{}?compression=lz4", conf.host, conf.port);
    let queries = read_queries(&conf.query)?;

    let bench = Arc::new(Benchmark::new(conf, queries, database_url));

    {
        let b = bench.clone();
        ctrlc::set_handler(move || {
            println!("ctrl-c received!");
            b.shutdown.store(true, Ordering::Relaxed);
        })
        .expect("Error setting Ctrl-C handler");
    }

    run(bench.clone()).await?;
    report_text(bench.clone()).await?;

    if !bench.config.json.is_empty() {
        report_json(bench.clone(), &bench.config.json)
            .await
            .map_err(to_error_codes)?;
    }
    Ok(())
}

async fn run(bench: BenchmarkRef) -> Result<()> {
    let mut executors = vec![];

    for _i in 0..bench.config.concurrency {
        let b = bench.clone();
        let b2 = bench.clone();
        executors.push(tokio::spawn(async move {
            if let Err(e) = execute(b).await {
                b2.shutdown.store(true, Ordering::Relaxed);
                log::error!("Got error in query {:?}", e);
            }
        }));
    }

    let mut i = 0usize;
    let max_iterations = bench.config.iterations;
    loop {
        if (max_iterations > 0 && i >= max_iterations) || bench.shutdown.load(Ordering::Relaxed) {
            break;
        }
        let mut rng = rand::thread_rng();
        let idx: usize = rng.gen_range(0..bench.queries.len());
        let query = bench.queries[idx].clone();

        if bench.queue.push(query).is_ok() {
            i += 1;
        }
    }

    try_join_all(executors).await.map_err(to_error_codes)?;
    Ok(())
}

fn read_queries(query: &str) -> Result<Vec<String>> {
    if query.is_empty() {
        let mut buffer = String::new();
        io::stdin()
            .read_to_string(&mut buffer)
            .map_err(to_error_codes)?;

        return Ok(buffer
            .split('\n')
            .filter(|f| !f.is_empty())
            .map(|s| s.to_string())
            .collect());
    } else {
        Ok(vec![query.to_owned()])
    }
}

async fn execute(bench: BenchmarkRef) -> Result<()> {
    let pool = Pool::new(bench.database_url.clone());

    loop {
        if bench.shutdown.load(Ordering::Relaxed)
            || (bench.config.iterations > 0
                && bench.executed.load(Ordering::Relaxed) >= bench.config.iterations)
        {
            break;
        } else {
            let query = bench.queue.pop();

            if let Some(query) = query {
                let start = Instant::now();
                let mut client = pool.get_handle().await.map_err(to_error_codes)?;

                {
                    let result = client.query(&query);
                    let mut stream = result.stream();
                    while stream.next().await.is_some() {}
                }

                {
                    let progress = client.progress();
                    let mut stats = bench.stats.write();
                    stats.update(
                        start.elapsed().as_millis() as f64 / 1000f64,
                        progress.rows as usize,
                        progress.bytes as usize,
                    );
                }
                bench.executed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

async fn report_text(bench: BenchmarkRef) -> Result<()> {
    if bench.queries.is_empty() {
        return Ok(());
    }

    let stats = bench.stats.read();
    eprint!("Queries {}, ", stats.queries);
    if stats.errors > 0 {
        eprint!("errors {}, ", stats.errors);
    }

    eprintln!(
        "QPS: {}, RPS: {}, MiB/s: {}.",
        stats.queries as f64 / stats.work_time,
        stats.read_rows as f64 / stats.work_time,
        stats.read_bytes as f64 / stats.work_time / 1048576f64,
    );
    eprintln!();

    fn print_quantile(stats: &Stats, percent: f64) {
        eprint!("{:.3}%\t\t", percent);
        eprintln!(
            "{} sec.\t",
            stats.sample.query(percent as f64 / 100f64).unwrap().1
        );
    }

    let mut percent = 0;
    while percent <= 90 {
        print_quantile(&stats, percent as f64);
        percent += 10;
    }

    print_quantile(&stats, 95f64);
    print_quantile(&stats, 99f64);
    print_quantile(&stats, 99.9);
    print_quantile(&stats, 99.99);
    Ok(())
}

async fn report_json(bench: BenchmarkRef, json_path: &str) -> std::io::Result<()> {
    if bench.queries.is_empty() {
        return Ok(());
    }

    let stats = bench.stats.read();

    let f = File::create(json_path)?;
    let mut writer = BufWriter::new(f);

    fn print_key_value<V>(
        writer: &mut BufWriter<File>,
        key: &str,
        value: V,
        with_comma: bool,
    ) -> std::io::Result<()>
    where
        V: fmt::Debug,
    {
        write!(writer, "{:?}: {:?}", key, value)?;
        if with_comma {
            writeln!(writer, ",")
        } else {
            writeln!(writer)
        }
    }

    fn print_quantile(
        writer: &mut BufWriter<File>,
        stats: &Stats,
        percent: f64,
        with_comma: bool,
    ) -> std::io::Result<()> {
        write!(
            writer,
            "\"{}\": {}",
            percent,
            stats.sample.query(percent as f64 / 100f64).unwrap().1
        )?;

        if with_comma {
            writeln!(writer, ",")
        } else {
            writeln!(writer)
        }
    }

    writer.write_all(b"{\n\"statistics\": {\n")?;

    print_key_value(
        &mut writer,
        "QPS",
        stats.queries as f64 / stats.work_time,
        true,
    )?;
    print_key_value(
        &mut writer,
        "RPS",
        stats.read_rows as f64 / stats.work_time,
        true,
    )?;
    print_key_value(
        &mut writer,
        "MiBPS",
        stats.read_bytes as f64 / stats.work_time / 1048576f64,
        false,
    )?;
    writer.write_all(b"}, \n")?;
    writer.write_all(b"\"query_time_percentiles\": {\n")?;

    let mut percent = 0;
    while percent <= 90 {
        print_quantile(&mut writer, &stats, percent as f64, true)?;
        percent += 10;
    }

    print_quantile(&mut writer, &stats, 95f64, true)?;
    print_quantile(&mut writer, &stats, 99f64, true)?;
    print_quantile(&mut writer, &stats, 99.9, true)?;
    print_quantile(&mut writer, &stats, 99.99, false)?;

    writer.write_all(b"}\n}\n")?;
    writer.flush()
}

fn to_error_codes<T>(err: T) -> ErrorCode
where T: ToString {
    anyhow::anyhow!(err.to_string()).into()
}
