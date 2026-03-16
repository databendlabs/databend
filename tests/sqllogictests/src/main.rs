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

use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::time::Instant;

use clap::Parser;
use client::TTCClient;
use futures_util::StreamExt;
use futures_util::stream;
use rand::Rng;
use sqllogictest::DBOutput;
use sqllogictest::Location;
use sqllogictest::QueryExpect;
use sqllogictest::Record;
use sqllogictest::Runner;
use sqllogictest::default_validator;
use sqllogictest::parse_file;
use testcontainers::ContainerAsync;
use testcontainers::GenericImage;
use testcontainers::Image;

use crate::arg::SqlLogicTestArgs;
use crate::client::Client;
use crate::client::ClientType;
use crate::client::HttpClient;
use crate::client::MySQLClient;
use crate::client::QueryResultFormat;
use crate::diagnostics::capture_failure_diagnostics;
use crate::error::DSqlLogicTestError;
use crate::error::Result;
use crate::report::ErrorRecord;
use crate::report::RunReport;
use crate::util::ColumnType;
use crate::util::collect_files;
use crate::util::collect_lazy_dir;
use crate::util::lazy_prepare_data;
use crate::util::lazy_run_dictionary_containers;
use crate::util::run_ttc_container;

mod arg;
mod client;
mod diagnostics;
mod error;
mod report;
mod util;

const HANDLER_MYSQL: &str = "mysql";
const HANDLER_HTTP: &str = "http";
const HANDLER_HYBRID: &str = "hybrid";
const TTC_PORT_START: u16 = 9902;

use std::sync::LazyLock;

static HYBRID_CONFIGS: LazyLock<Vec<(Box<ClientType>, usize)>> = LazyLock::new(|| {
    vec![
        (Box::new(ClientType::MySQL), 3),
        // disable for now due to client change
        // (
        //     Box::new(ClientType::Ttc {
        //         image: "ghcr.io/databendlabs/ttc-rust:latest".to_string(),
        //         port: TTC_PORT_START,
        //         query_result_format: QueryResultFormat::Arrow,
        //     }),
        //     5,
        // ),
        (
            Box::new(ClientType::Ttc {
                image: "ghcr.io/databendlabs/ttc-rust:latest".to_string(),
                port: TTC_PORT_START + 1,
                query_result_format: QueryResultFormat::Json,
            }),
            5,
        ),
        (
            Box::new(ClientType::Ttc {
                image: "ghcr.io/databendlabs/ttc-go:latest".to_string(),
                port: TTC_PORT_START + 2,
                query_result_format: QueryResultFormat::Json,
            }),
            5,
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
    type ColumnType = ColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        self.client.query(sql).await
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        self.client.engine_name()
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

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
                run_mysql_client(args.clone()).await?;
            }
            HANDLER_HTTP => {
                run_http_client(args.clone()).await?;
            }
            HANDLER_HYBRID => {
                run_hybrid_client(args.clone(), &mut containers).await?;
            }
            handler if handler.starts_with("ttc") => {
                if handler != "ttc_dev" {
                    let image = format!("ghcr.io/databendlabs/{handler}:latest");
                    run_ttc_container(
                        &image,
                        TTC_PORT_START,
                        args.port,
                        &mut containers,
                        QueryResultFormat::Json,
                    )
                    .await?;
                }
                run_ttc_client(args.clone(), ClientType::Ttc {
                    image: handler.to_string(),
                    port: TTC_PORT_START,
                    query_result_format: QueryResultFormat::Json,
                })
                .await?;
            }
            _ => {
                return Err(format!("Unknown test handler: {handler}").into());
            }
        }
    }

    Ok(())
}

async fn run_mysql_client(args: SqlLogicTestArgs) -> Result<()> {
    println!("MySQL client starts to run with: {:?}", args);
    run_suits(args, ClientType::MySQL).await?;
    Ok(())
}

async fn run_http_client(args: SqlLogicTestArgs) -> Result<()> {
    println!("Http client starts to run with: {:?}", args);
    run_suits(args, ClientType::Http).await?;
    Ok(())
}
async fn run_ttc_client(args: SqlLogicTestArgs, client_type: ClientType) -> Result<()> {
    println!("Http client starts to run with: {:?}", args);
    run_suits(args, client_type).await?;
    Ok(())
}

async fn run_hybrid_client(
    args: SqlLogicTestArgs,
    cs: &mut Vec<ContainerAsync<GenericImage>>,
) -> Result<()> {
    println!("Hybird client starts to run with: {:?}", args);

    for (c, _) in HYBRID_CONFIGS.iter() {
        match c.as_ref() {
            ClientType::MySQL | ClientType::Http => {}
            ClientType::Ttc {
                image,
                port,
                query_result_format,
            } => {
                run_ttc_container(image, *port, args.port, cs, *query_result_format).await?;
            }
            ClientType::Hybird => panic!("Can't run hybrid client in hybrid client"),
        }
    }

    if let Err(e) = run_suits(args, ClientType::Hybird).await {
        for c in cs {
            println!("{}", c.id());
            println!("{}", c.image().name());
            if let Ok(log) = c.stderr_to_vec().await {
                println!("stderr: {}", String::from_utf8_lossy(&log));
            }
            if let Ok(log) = c.stdout_to_vec().await {
                println!("stdout: {}", String::from_utf8_lossy(&log));
            }
        }
        Err(e)?
    }
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
            client = Client::Http(HttpClient::create(args.port).await?);
        }

        ClientType::Ttc {
            image,
            port,
            query_result_format: _,
        } => {
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

async fn run_suits(args: SqlLogicTestArgs, client_type: ClientType) -> Result<()> {
    // Todo: set validator to process regex
    let mut num_of_tests = 0;
    let mut lazy_dirs = HashSet::new();
    let mut files = vec![];
    let start = Instant::now();
    for suit_file in collect_files(&args)?.into_iter() {
        let file_name = suit_file.file_name().unwrap().to_str().unwrap().to_string();

        if !file_name.ends_with(".test") {
            continue;
        }
        if let Some(ref specific_file) = args.file
            && !specific_file.split(',').any(|f| f.eq(&file_name))
        {
            continue;
        }
        if let Some(ref skip_file) = args.skipped_file
            && skip_file.split(',').any(|f| f.eq(&file_name))
        {
            continue;
        }
        num_of_tests += parse_file::<ColumnType>(&suit_file).unwrap().len();

        collect_lazy_dir(&suit_file, &mut lazy_dirs)?;
        files.push(suit_file);
    }
    let selected_files = files.len();

    if !args.bench {
        // lazy load test data
        lazy_prepare_data(&lazy_dirs, args.force_load)?;
    }
    // lazy run dictionaries containers
    let _dict_container = lazy_run_dictionary_containers(&lazy_dirs).await?;

    if args.complete {
        for file in files {
            let file_name = file.file_name().unwrap().to_str().unwrap().to_string();

            let col_separator = " ";
            let validator = default_validator;
            let mut runner =
                Runner::new(|| async { create_databend(&client_type, &file_name).await });
            // todo: The behavior of normalizer for multi line string is incorrect
            runner
                .update_test_file(
                    &file,
                    col_separator,
                    validator,
                    sqllogictest::default_normalizer,
                    |actual, expected| actual == expected,
                )
                .await
                .unwrap();
        }
    } else {
        let mut tasks = Vec::with_capacity(files.len());
        for file in files {
            let client_type = client_type.clone();
            tasks.push(async move { run_file_async(&client_type, args.bench, file).await });
        }
        let error_records = run_parallel_async(tasks).await;
        let report = RunReport::new(
            selected_files,
            num_of_tests,
            num_of_tests > 0,
            args.no_fail_fast,
            start.elapsed(),
            error_records,
        );
        println!("{}", report.render());

        if report.has_failures() {
            return Err(DSqlLogicTestError::SelfError(
                "sqllogictest failed".to_string(),
            ));
        }
    }
    let duration = start.elapsed();
    println!(
        "Run all tests[{}] using {} ms",
        num_of_tests,
        duration.as_millis()
    );

    Ok(())
}

fn column_validator(loc: Location, actual: Vec<ColumnType>, expected: Vec<ColumnType>) {
    if actual.is_empty() {
        return;
    }
    let equals = if actual.len() != expected.len() {
        false
    } else {
        actual.iter().zip(expected.iter()).all(|x| {
            use ColumnType::*;
            matches!(
                x,
                (Bool, Bool)
                    | (Text, Text)
                    | (Integer, Integer)
                    | (FloatingPoint, FloatingPoint)
                    | (Any, _)
                    | (_, Any)
            )
        })
    };
    if !equals {
        println!(
            "warn: column type not match, actual: {actual:?}, expected: {expected:?}, loc: {loc}"
        );
    }
}

async fn run_parallel_async(
    tasks: Vec<impl Future<Output = std::result::Result<Vec<ErrorRecord>, ErrorRecord>>>,
) -> Vec<ErrorRecord> {
    let args = SqlLogicTestArgs::parse();
    let jobs = tasks.len().clamp(1, args.parallel);
    let tasks = stream::iter(tasks).buffer_unordered(jobs);
    let no_fail_fast = args.no_fail_fast;
    if !no_fail_fast {
        tasks
            .filter_map(|result| async { result.err() })
            .collect()
            .await
    } else {
        let errors: Vec<Vec<ErrorRecord>> = tasks
            .filter_map(|result| async { result.ok() })
            .collect()
            .await;
        errors.into_iter().flatten().collect()
    }
}

async fn run_file_async(
    client_type: &ClientType,
    bench: bool,
    filename: impl AsRef<Path>,
) -> std::result::Result<Vec<ErrorRecord>, ErrorRecord> {
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
        let expected_types = if let Record::Query {
            loc,
            expected: QueryExpect::Results { types, .. },
            ..
        } = &record
        {
            Some((loc.clone(), types.clone()))
        } else {
            None
        };

        match (runner.run_async(record).await, expected_types) {
            (
                Ok(sqllogictest::RecordOutput::Query { types: actual, .. }),
                Some((loc, expected)),
            ) => column_validator(loc, actual, expected),
            (Err(e), _) => {
                // Skip query result error in bench
                if bench
                    && matches!(
                        e.kind(),
                        sqllogictest::TestErrorKind::QueryResultMismatch { .. }
                    )
                {
                    continue;
                }

                let diagnostics = capture_failure_diagnostics(&mut runner).await;
                let error_record = ErrorRecord::new(
                    filename.to_string(),
                    e,
                    diagnostics.query_id,
                    diagnostics.non_default_settings,
                );

                if no_fail_fast {
                    error_records.push(error_record);
                } else {
                    return Err(error_record);
                }
            }
            _ => {}
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
