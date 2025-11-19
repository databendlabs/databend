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
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use bollard::container::ListContainersOptions;
use bollard::container::RemoveContainerOptions;
use bollard::Docker;
use clap::Parser;
use redis::Commands;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use testcontainers::core::client::docker_client_instance;
use testcontainers::core::logs::consumer::logging_consumer::LoggingConsumer;
use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers::GenericImage;
use testcontainers::ImageExt;
use testcontainers_modules::mysql::Mysql;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::redis::REDIS_PORT;
use walkdir::DirEntry;
use walkdir::WalkDir;

use crate::arg::SqlLogicTestArgs;
use crate::error::DSqlLogicTestError;
use crate::error::Result;

const CONTAINER_RETRY_TIMES: usize = 3;
const CONTAINER_STARTUP_TIMEOUT_SECONDS: u64 = 60;
const CONTAINER_TIMEOUT_SECONDS: u64 = 300;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ServerInfo {
    pub id: String,
    pub start_time: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HttpSessionConf {
    pub catalog: Option<String>,
    pub database: Option<String>,
    pub role: Option<String>,
    pub secondary_roles: Option<Vec<String>>,
    pub settings: Option<BTreeMap<String, String>>,
    pub txn_state: Option<String>,
    pub internal: String,
}

pub fn parser_rows(rows: &Value) -> Result<Vec<Vec<String>>> {
    let mut parsed_rows = Vec::new();
    for row in rows.as_array().unwrap() {
        let mut parsed_row = Vec::new();
        for col in row.as_array().unwrap() {
            match col {
                Value::Null => {
                    parsed_row.push("NULL".to_string());
                }
                Value::String(cell) => {
                    // If the result is empty, we'll use `(empty)` to mark it explicitly to avoid confusion
                    if cell.is_empty() {
                        parsed_row.push("(empty)".to_string());
                    } else {
                        parsed_row.push(cell.to_string());
                    }
                }
                _ => unreachable!(),
            }
        }
        parsed_rows.push(parsed_row);
    }
    Ok(parsed_rows)
}

fn find_specific_dir(dir: &str, suit: PathBuf) -> Result<DirEntry> {
    for entry in WalkDir::new(suit)
        .min_depth(0)
        .max_depth(100)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
    {
        let e = entry.as_ref().unwrap();
        if e.file_type().is_dir() && e.file_name().to_str().unwrap() == dir {
            return Ok(entry?);
        }
    }
    Err(DSqlLogicTestError::SelfError(
        "Didn't find specific dir".to_string(),
    ))
}

pub fn get_files(suit: PathBuf) -> Result<Vec<walkdir::Result<DirEntry>>> {
    let args = SqlLogicTestArgs::parse();
    let mut files = vec![];

    let dirs = match args.dir {
        Some(ref dir) => {
            // Find specific dir
            let dir_entry = find_specific_dir(dir, suit);
            match dir_entry {
                Ok(dir_entry) => Some(dir_entry.into_path()),
                // If didn't find specific dir, return empty vec
                Err(_) => None,
            }
        }
        None => Some(suit),
    };
    let target = match dirs {
        Some(dir) => dir,
        None => return Ok(vec![]),
    };
    for entry in WalkDir::new(target)
        .min_depth(0)
        .max_depth(100)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
        .filter_entry(|e| {
            if let Some(skipped_dir) = &args.skipped_dir {
                let dirs = skipped_dir.split(',').collect::<Vec<&str>>();
                if dirs.contains(&e.file_name().to_str().unwrap()) {
                    return false;
                }
            }
            true
        })
        .filter(|e| !e.as_ref().unwrap().file_type().is_dir())
    {
        files.push(entry);
    }
    Ok(files)
}

static PREPARE_TPCH: std::sync::Once = std::sync::Once::new();
static PREPARE_TPCDS: std::sync::Once = std::sync::Once::new();
static PREPARE_STAGE: std::sync::Once = std::sync::Once::new();
static PREPARE_WASM: std::sync::Once = std::sync::Once::new();

#[derive(Eq, Hash, PartialEq)]
pub enum LazyDir {
    Tpch,
    Tpcds,
    Stage,
    UdfNative,
    Dictionaries,
}

pub fn collect_lazy_dir(file_path: &Path, lazy_dirs: &mut HashSet<LazyDir>) -> Result<()> {
    let file_path = file_path.to_str().unwrap_or_default();
    if file_path.contains("tpch/") || file_path.contains("tpch_spill/") {
        if !lazy_dirs.contains(&LazyDir::Tpch) {
            lazy_dirs.insert(LazyDir::Tpch);
        }
    } else if file_path.contains("tpcds/") {
        if !lazy_dirs.contains(&LazyDir::Tpcds) {
            lazy_dirs.insert(LazyDir::Tpcds);
        }
    } else if file_path.contains("stage/") || file_path.contains("stage_parquet/") {
        if !lazy_dirs.contains(&LazyDir::Stage) {
            lazy_dirs.insert(LazyDir::Stage);
        }
    } else if file_path.contains("udf_native/") {
        if !lazy_dirs.contains(&LazyDir::UdfNative) {
            lazy_dirs.insert(LazyDir::UdfNative);
        }
    } else if file_path.contains("dictionaries/") && !lazy_dirs.contains(&LazyDir::Dictionaries) {
        lazy_dirs.insert(LazyDir::Dictionaries);
    }
    Ok(())
}

pub fn lazy_prepare_data(lazy_dirs: &HashSet<LazyDir>, force_load: bool) -> Result<()> {
    let force_load_flag = if force_load { "1" } else { "0" };
    for lazy_dir in lazy_dirs {
        match lazy_dir {
            LazyDir::Tpch => {
                PREPARE_TPCH.call_once(|| {
                    println!("Calling the script prepare_tpch_data.sh ...");
                    run_script("prepare_tpch_data.sh", &["tpch_test", force_load_flag]).unwrap();
                });
            }
            LazyDir::Tpcds => {
                PREPARE_TPCDS.call_once(|| {
                    println!("Calling the script prepare_tpcds_data.sh ...");
                    run_script("prepare_tpcds_data.sh", &["tpcds", force_load_flag]).unwrap();
                });
            }
            LazyDir::Stage => {
                PREPARE_STAGE.call_once(|| {
                    println!("Calling the script prepare_stage.sh ...");
                    run_script("prepare_stage.sh", &[]).unwrap();
                });
            }
            LazyDir::UdfNative => {
                println!("wasm context Calling the script prepare_stage.sh ...");
                PREPARE_WASM.call_once(|| run_script("prepare_stage.sh", &[]).unwrap())
            }
            _ => {}
        }
    }
    Ok(())
}

fn run_script(name: &str, args: &[&str]) -> Result<()> {
    let path = format!("tests/sqllogictests/scripts/{}", name);
    let mut new_args = vec![path.as_str()];
    new_args.extend_from_slice(args);

    let output = std::process::Command::new("bash")
        .args(new_args)
        .output()
        .expect("failed to execute process");
    if !output.status.success() {
        return Err(DSqlLogicTestError::SelfError(format!(
            "Failed to run {}: {}",
            name,
            String::from_utf8(output.stderr).unwrap()
        )));
    } else {
        println!(
            "script stdout:\n {}",
            String::from_utf8(output.stdout).unwrap()
        );
        if !output.stderr.is_empty() {
            println!(
                "script stderr:\n {}",
                String::from_utf8(output.stderr).unwrap()
            );
        }
    }
    Ok(())
}

pub async fn run_ttc_container(
    image: &str,
    port: u16,
    http_server_port: u16,
    cs: &mut Vec<ContainerAsync<GenericImage>>,
) -> Result<()> {
    let docker = &docker_client_instance().await?;
    let mut images = image.split(":");
    let image = images.next().unwrap();
    let tag = images.next().unwrap_or("latest");

    use rand::distributions::Alphanumeric;
    use rand::Rng;
    let rng = rand::thread_rng();
    let x: String = rng
        .sample_iter(&Alphanumeric)
        .take(5)
        .map(char::from)
        .collect();
    let container_name = format!("databend-ttc-{}-{}", port, x);
    let start = Instant::now();
    println!("Starting container {container_name}");
    let dsn = format!(
        "databend://root:@127.0.0.1:{}?sslmode=disable",
        http_server_port
    );

    let mut i = 1;
    loop {
        let log_consumer = LoggingConsumer::new();

        let container_res = GenericImage::new(image, tag)
            .with_exposed_port(port.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .with_startup_timeout(Duration::from_secs(CONTAINER_STARTUP_TIMEOUT_SECONDS))
            .with_network("host")
            .with_env_var("DATABEND_DSN", &dsn)
            .with_env_var("TTC_PORT", format!("{port}"))
            .with_container_name(&container_name)
            .with_log_consumer(log_consumer)
            .start()
            .await;
        let duration = start.elapsed().as_secs();
        match container_res {
            Ok(container) => {
                println!(
                    "Started container {container_name} {} using {duration} secs",
                    container.id(),
                );
                cs.push(container);
                return Ok(());
            }
            Err(err) => {
                eprintln!(
                    "Failed to start container {container_name} using {duration} secs: {err}"
                );
                stop_container(docker, &container_name).await;
                if i == CONTAINER_RETRY_TIMES || duration >= CONTAINER_TIMEOUT_SECONDS {
                    break;
                } else {
                    println!(
                        "Retrying to start container {container_name} {i} after {duration} secs",
                    );
                    i += 1;
                }
            }
        }
    }
    Err(format!("Start {container_name} failed").into())
}

#[allow(dead_code)]
pub struct DictionaryContainer {
    pub redis: ContainerAsync<Redis>,
    pub mysql: ContainerAsync<Mysql>,
}

pub async fn lazy_run_dictionary_containers(
    lazy_dirs: &HashSet<LazyDir>,
) -> Result<Option<DictionaryContainer>> {
    if !lazy_dirs.contains(&LazyDir::Dictionaries) {
        return Ok(None);
    }
    println!("Start run dictionary source server container");
    let docker = docker_client_instance().await?;
    let redis = run_redis_server(&docker).await?;
    let mysql = run_mysql_server(&docker).await?;
    let dict_container = DictionaryContainer { redis, mysql };

    Ok(Some(dict_container))
}

async fn run_redis_server(docker: &Docker) -> Result<ContainerAsync<Redis>> {
    let start = Instant::now();
    let container_name = "redis".to_string();
    println!("Start container {container_name}");

    stop_container(docker, &container_name).await;

    let mut i = 1;
    loop {
        let redis_res = Redis::default()
            .with_network("host")
            .with_startup_timeout(Duration::from_secs(CONTAINER_STARTUP_TIMEOUT_SECONDS))
            .with_container_name(&container_name)
            .start()
            .await;

        let duration = start.elapsed().as_secs();
        match redis_res {
            Ok(redis) => {
                let host_ip = redis.get_host().await.unwrap();
                let url = format!("redis://{}:{}", host_ip, REDIS_PORT);
                let client = redis::Client::open(url.as_ref()).unwrap();
                let mut con = client.get_connection().unwrap();

                // Add some key values for test.
                let keys = vec!["a", "b", "c", "1", "2"];
                for key in keys {
                    let val = format!("{}_value", key);
                    con.set::<_, _, ()>(key, val).unwrap();
                }
                println!(
                    "Start container {} using {} secs success",
                    container_name, duration
                );
                return Ok(redis);
            }
            Err(err) => {
                eprintln!(
                    "Start container {} using {} secs failed: {}",
                    container_name, duration, err
                );
                stop_container(docker, &container_name).await;
                if i == CONTAINER_RETRY_TIMES || duration >= CONTAINER_TIMEOUT_SECONDS {
                    break;
                } else {
                    i += 1;
                }
            }
        }
    }
    Err(format!("Start {container_name} failed").into())
}

async fn run_mysql_server(docker: &Docker) -> Result<ContainerAsync<Mysql>> {
    let start = Instant::now();
    let container_name = "mysql".to_string();
    println!("Start container {container_name}");

    stop_container(docker, &container_name).await;

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
    let mut i = 1;
    loop {
        let mysql_res = Mysql::default()
            .with_init_sql(
    "CREATE TABLE test.user(id INT, name VARCHAR(100), age SMALLINT UNSIGNED, salary DOUBLE, active BOOL); \
    INSERT INTO test.user VALUES \
    (1, 'Alice', 24, 100, true), \
    (2, 'Bob', 35, 200.1, false), \
    (3, 'Lily', 41, 1000.2, true), \
    (4, 'Tom', 55, 3000.55, false), \
    (5, NULL, NULL, NULL, NULL);"
                .to_string()
                .into_bytes(),
            )
            .with_network("host")
            .with_startup_timeout(Duration::from_secs(CONTAINER_STARTUP_TIMEOUT_SECONDS))
            .with_container_name(&container_name)
            .start().await;

        let duration = start.elapsed().as_secs();
        match mysql_res {
            Ok(mysql) => {
                println!(
                    "Start container {} using {} secs success",
                    container_name, duration
                );
                return Ok(mysql);
            }
            Err(err) => {
                eprintln!(
                    "Start container {} using {} secs failed: {}",
                    container_name, duration, err
                );
                stop_container(docker, &container_name).await;
                if i == CONTAINER_RETRY_TIMES || duration >= CONTAINER_TIMEOUT_SECONDS {
                    break;
                } else {
                    i += 1;
                }
            }
        }
    }
    Err(format!("Start {container_name} failed").into())
}

// Stop the running container to avoid conflict
async fn stop_container(docker: &Docker, container_name: &str) {
    let opts = Some(ListContainersOptions::<String> {
        all: true,
        ..Default::default()
    });
    let containers = docker.list_containers(opts).await;
    if let Ok(containers) = containers {
        if !containers.is_empty() {
            println!("==> list containers");
            for container in containers {
                if let Some(names) = container.names {
                    println!(
                        " -> container name: {:?}, status: {:?}",
                        names, container.state
                    );
                }
            }
        }
    }

    let container = docker.inspect_container(container_name, None).await;
    if let Ok(container) = container {
        println!(
            "Stopping previous container {container_name}: {:?}",
            container.state
        );
        if let Err(err) = docker.stop_container(container_name, None).await {
            eprintln!("Failed to stop container {container_name}: {err}");
        }
        let options = Some(RemoveContainerOptions {
            force: true,
            ..Default::default()
        });
        match docker.remove_container(container_name, options).await {
            Ok(_) => {
                println!("Removed container {container_name}");
            }
            Err(err) => {
                eprintln!("Failed to remove container {container_name}: {err}");
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ColumnType {
    Bool,
    Text,
    Integer,
    FloatingPoint,
    Any,
}

impl sqllogictest::ColumnType for ColumnType {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'B' => Some(Self::Bool),
            'T' => Some(Self::Text),
            'I' => Some(Self::Integer),
            'R' => Some(Self::FloatingPoint),
            _ => Some(Self::Any),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Bool => 'B',
            Self::Text => 'T',
            Self::Integer => 'I',
            Self::FloatingPoint => 'R',
            Self::Any => '?',
        }
    }
}
