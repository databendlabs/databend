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

use std::fs::ReadDir;
use std::future::Future;
use std::path::Path;
use std::time::Instant;

use bollard::Docker;
use clap::Parser;
use client::TTCClient;
use futures_util::stream;
use futures_util::StreamExt;
use rand::Rng;
use redis::Commands;
use sqllogictest::default_column_validator;
use sqllogictest::default_validator;
use sqllogictest::parse_file;
use sqllogictest::DBOutput;
use sqllogictest::DefaultColumnType;
use sqllogictest::Record;
use sqllogictest::Runner;
use sqllogictest::TestError;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::redis::REDIS_PORT;

use crate::arg::SqlLogicTestArgs;
use crate::client::Client;
use crate::client::ClientType;
use crate::client::HttpClient;
use crate::client::MySQLClient;
use crate::error::DSqlLogicTestError;
use crate::error::Result;
use crate::util::get_files;
use crate::util::lazy_prepare_data;

mod arg;
mod client;
mod error;
mod util;

const HANDLER_MYSQL: &str = "mysql";
const HANDLER_HTTP: &str = "http";
const HANDLER_HYBRID: &str = "hybrid";
const TTC_PORT_START: u16 = 9902;

use std::sync::LazyLock;

static HYBRID_CONFIGS: LazyLock<Vec<(Box<ClientType>, usize)>> = LazyLock::new(|| {
    vec![
        (Box::new(ClientType::MySQL), 3),
        (
            Box::new(ClientType::Ttc(
                "datafuselabs/ttc-rust:latest".to_string(),
                TTC_PORT_START,
            )),
            7,
        ),
    ]
});

pub struct Databend {
    client: Client,
}

impl Databend {
    pub fn create(client: Client) -> Self {
        Databend { client }
    }
    pub fn client_name(&self) -> &str {
        self.client.engine_name()
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for Databend {
    type Error = DSqlLogicTestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        self.client.query(sql).await
    }

    fn engine_name(&self) -> &str {
        self.client.engine_name()
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

    let docker = Docker::connect_with_local_defaults().unwrap();

    // Run mock sources for dictionary test.
    let _mock_redis = run_redis_mock_sources(&docker).await?;
    let _mock_mysql = run_mysql_mock_sources(&docker).await?;
    println!(
        "Run sqllogictests with args: {}",
        std::env::args().skip(1).collect::<Vec<String>>().join(" ")
    );
    let args = SqlLogicTestArgs::parse();
    let handlers = match &args.handlers {
        Some(hs) => hs.iter().map(|s| s.as_str()).collect(),
        None => vec![HANDLER_MYSQL, HANDLER_HTTP],
    };
    let mut containers = vec![];
    for handler in handlers.iter() {
        match *handler {
            HANDLER_MYSQL => {
                run_mysql_client().await?;
            }
            HANDLER_HTTP => {
                run_http_client().await?;
            }
            HANDLER_HYBRID => {
                run_hybrid_client(&docker, &mut containers).await?;
            }
            _ => {
                return Err(format!("Unknown test handler: {handler}").into());
            }
        }
    }

    Ok(())
}

async fn run_redis_mock_sources(docker: &Docker) -> Result<ContainerAsync<Redis>> {
    let container_name = "redis".to_string();

    // Stop the container
    let _ = docker.stop_container(&container_name, None).await;
    let _ = docker.remove_container(&container_name, None).await;

    let mock_redis = Redis::default()
        .with_network("host")
        .with_container_name(container_name)
        .start()
        .await
        .unwrap();

    let host_ip = mock_redis.get_host().await.unwrap();
    let url = format!("redis://{}:{}", host_ip, REDIS_PORT);
    let client = redis::Client::open(url.as_ref()).unwrap();
    let mut con = client.get_connection().unwrap();

    // Add some key values for test.
    let keys = vec!["a", "b", "c", "1", "2"];
    for key in keys {
        let val = format!("{}_value", key);
        con.set::<_, _, ()>(key, val).unwrap();
    }

    Ok(mock_redis)
}

async fn run_mysql_mock_sources(docker: &Docker) -> Result<ContainerAsync<Mysql>> {
    let container_name = "mysqld".to_string();

    // Stop the container
    let _ = docker.stop_container(&container_name, None).await;
    let _ = docker.remove_container(&container_name, None).await;

    // Add a table for test.
    // CREATE TABLE test.user(
    //   id INT,
    //   name VARCHAR(100),
    //   age SMALLINT UNSIGNED,
    //   salary DOUBLE,
    //   active BOOL
    // );
    //
    // +------+-------+------+---------+--------+
    // | id   | name  | age  | salary  | active |
    // +------+-------+------+---------+--------+
    // |    1 | Alice |   24 |     100 |      1 |
    // |    2 | Bob   |   35 |   200.1 |      0 |
    // |    3 | Lily  |   41 |  1000.2 |      1 |
    // |    4 | Tom   |   55 | 3000.55 |      0 |
    // |    5 | NULL  | NULL |    NULL |   NULL |
    // +------+-------+------+---------+--------+
    let mock_mysqld = Mysql::default()
        .with_init_sql(
"CREATE TABLE test.user(id INT, name VARCHAR(100), age SMALLINT UNSIGNED, salary DOUBLE, active BOOL); INSERT INTO test.user VALUES(1, 'Alice', 24, 100, true), (2, 'Bob', 35, 200.1, false), (3, 'Lily', 41, 1000.2, true), (4, 'Tom', 55, 3000.55, false), (5, NULL, NULL, NULL, NULL);"
        .to_string()
        .into_bytes(),
)
        .with_network("host")
        .with_container_name(container_name)
        .start().await.unwrap();

    Ok(mock_mysqld)
}

async fn run_mysql_client() -> Result<()> {
    println!(
        "MySQL client starts to run with: {:?}",
        SqlLogicTestArgs::parse()
    );
    let suits = SqlLogicTestArgs::parse().suites;
    let suits = std::fs::read_dir(suits).unwrap();
    run_suits(suits, ClientType::MySQL).await?;
    Ok(())
}

async fn run_http_client() -> Result<()> {
    println!(
        "Http client starts to run with: {:?}",
        SqlLogicTestArgs::parse()
    );
    let suits = SqlLogicTestArgs::parse().suites;
    let suits = std::fs::read_dir(suits).unwrap();
    run_suits(suits, ClientType::Http).await?;
    Ok(())
}

async fn run_hybrid_client(
    docker: &Docker,
    cs: &mut Vec<ContainerAsync<GenericImage>>,
) -> Result<()> {
    println!(
        "Hybird client starts to run with: {:?}",
        SqlLogicTestArgs::parse()
    );
    let suits = SqlLogicTestArgs::parse().suites;
    let suits = std::fs::read_dir(suits).unwrap();

    // preparse docker envs
    let mut port_start = TTC_PORT_START;

    for (c, _) in HYBRID_CONFIGS.iter() {
        match c.as_ref() {
            ClientType::MySQL | ClientType::Http => {}
            ClientType::Ttc(image, _) => {
                let mut images = image.split(":");
                let image = images.next().unwrap();
                let tag = images.next().unwrap_or("latest");

                let container_name = format!("databend-ttc-{}", port_start);
                println!("Start {container_name}");

                // Stop the container
                let _ = docker.stop_container(&container_name, None).await;
                let _ = docker.remove_container(&container_name, None).await;

                let container: ContainerAsync<GenericImage> = GenericImage::new(image, tag)
                    .with_exposed_port(port_start.tcp())
                    .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
                    .with_network("host")
                    .with_env_var(
                        "DATABEND_DSN",
                        "databend://root:@127.0.0.1:8000?sslmode=disable",
                    )
                    .with_env_var("TTC_PORT", format!("{port_start}"))
                    .with_container_name(container_name)
                    .start()
                    .await
                    .unwrap();
                println!("Started container: {}", container.id());
                cs.push(container);
                port_start += 1;
            }
            ClientType::Hybird => panic!("Can't run hybrid client in hybrid client"),
        }
    }

    run_suits(suits, ClientType::Hybird).await?;
    Ok(())
}

// Create new databend with client type
#[async_recursion::async_recursion(#[recursive::recursive])]
async fn create_databend(client_type: &ClientType, filename: &str) -> Result<Databend> {
    let mut client: Client;
    let args = SqlLogicTestArgs::parse();
    match client_type {
        ClientType::MySQL => {
            let mut mysql_client = MySQLClient::create(&args.database).await?;
            if args.bench {
                mysql_client.enable_bench();
            }
            client = Client::MySQL(mysql_client);
        }
        ClientType::Http => {
            client = Client::Http(HttpClient::create().await?);
        }

        ClientType::Ttc(image, port) => {
            let conn = format!("127.0.0.1:{port}");
            client = Client::Ttc(TTCClient::create(image, &conn).await?);
        }

        ClientType::Hybird => {
            let ts = &HYBRID_CONFIGS;
            let totals: usize = ts.iter().map(|t| t.1).sum();
            let r = rand::thread_rng().gen_range(0..totals);

            let mut acc = 0;
            for (t, s) in ts.iter() {
                acc += s;

                if acc >= r {
                    return create_databend(t.as_ref(), filename).await;
                }
            }
            unreachable!()
        }
    }
    if args.enable_sandbox {
        client.create_sandbox().await?;
    }
    if args.debug {
        client.enable_debug();
    }

    println!("Running {} test for file: {} ...", client_type, filename);
    Ok(Databend::create(client))
}

async fn run_suits(suits: ReadDir, client_type: ClientType) -> Result<()> {
    // Todo: set validator to process regex
    let args = SqlLogicTestArgs::parse();
    let mut tasks = vec![];
    let mut num_of_tests = 0;
    let start = Instant::now();
    // Walk each suit dir and read all files in it
    // After get a slt file, set the file name to databend
    for suit in suits {
        // Get a suit and find all slt files in the suit
        let suit = suit.unwrap().path();
        // Parse the suit and find all slt files
        let files = get_files(suit)?;
        for file in files.into_iter() {
            let file_name = file
                .as_ref()
                .unwrap()
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            if let Some(ref specific_file) = args.file {
                if !specific_file.split(',').any(|f| f.eq(&file_name)) {
                    continue;
                }
            }
            if let Some(ref skip_file) = args.skipped_file {
                if skip_file.split(',').any(|f| f.eq(&file_name)) {
                    continue;
                }
            }
            num_of_tests += parse_file::<DefaultColumnType>(file.as_ref().unwrap().path())
                .unwrap()
                .len();

            lazy_prepare_data(file.as_ref().unwrap().path())?;

            if args.complete {
                let col_separator = " ";
                let validator = default_validator;
                let column_validator = default_column_validator;
                let mut runner =
                    Runner::new(|| async { create_databend(&client_type, &file_name).await });
                runner
                    .update_test_file(
                        file.unwrap().path(),
                        col_separator,
                        validator,
                        column_validator,
                    )
                    .await
                    .unwrap();
            } else {
                let client_type = client_type.clone();
                tasks.push(async move { run_file_async(&client_type, file.unwrap().path()).await });
            }
        }
    }
    if args.complete {
        return Ok(());
    }
    // Run all tasks parallel
    run_parallel_async(tasks, num_of_tests).await?;
    let duration = start.elapsed();
    println!(
        "Run all tests[{}] using {} ms",
        num_of_tests,
        duration.as_millis()
    );

    Ok(())
}

async fn run_parallel_async(
    tasks: Vec<impl Future<Output = std::result::Result<Vec<TestError>, TestError>>>,
    num_of_tests: usize,
) -> Result<()> {
    let args = SqlLogicTestArgs::parse();
    let jobs = tasks.len().clamp(1, args.parallel);
    let tasks = stream::iter(tasks).buffer_unordered(jobs);
    let no_fail_fast = args.no_fail_fast;
    if !no_fail_fast {
        let errors = tasks
            .filter_map(|result| async { result.err() })
            .collect()
            .await;
        handle_error_records(errors, no_fail_fast, num_of_tests)?;
    } else {
        let errors: Vec<Vec<TestError>> = tasks
            .filter_map(|result| async { result.ok() })
            .collect()
            .await;
        handle_error_records(
            errors.into_iter().flatten().collect(),
            no_fail_fast,
            num_of_tests,
        )?;
    }
    Ok(())
}

async fn run_file_async(
    client_type: &ClientType,
    filename: impl AsRef<Path>,
) -> std::result::Result<Vec<TestError>, TestError> {
    let start = Instant::now();

    let mut error_records = vec![];
    let no_fail_fast = SqlLogicTestArgs::parse().no_fail_fast;
    let records = parse_file(&filename).unwrap();
    let filename = filename.as_ref().to_str().unwrap();

    let mut runner = Runner::new(|| async { create_databend(client_type, filename).await });
    for record in records.into_iter() {
        if let Record::Halt { .. } = record {
            break;
        }
        // Capture error record and continue to run next records
        if let Err(e) = runner.run_async(record).await {
            if no_fail_fast {
                error_records.push(e);
            } else {
                return Err(e);
            }
        }
    }
    let run_file_status = match error_records.is_empty() {
        true => "✅",
        false => "❌",
    };

    if !SqlLogicTestArgs::parse().bench {
        println!(
            "Completed {} test for file: {} {} ({:?})",
            client_type,
            filename,
            run_file_status,
            start.elapsed(),
        );
    }
    Ok(error_records)
}

fn handle_error_records(
    error_records: Vec<TestError>,
    no_fail_fast: bool,
    num_of_tests: usize,
) -> Result<()> {
    if error_records.is_empty() {
        return Ok(());
    }

    println!(
        "Test finished, fail fast {}, {} out of {} records failed to run",
        if no_fail_fast { "disabled" } else { "enabled" },
        error_records.len(),
        num_of_tests
    );
    for (idx, error_record) in error_records.iter().enumerate() {
        println!("{idx}: {}", error_record.display(true));
    }

    Err(DSqlLogicTestError::SelfError(
        "sqllogictest failed".to_string(),
    ))
}
