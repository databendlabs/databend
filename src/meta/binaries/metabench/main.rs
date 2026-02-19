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

#![allow(
    clippy::collapsible_if,
    clippy::manual_is_multiple_of,
    clippy::uninlined_format_args
)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use chrono::Utc;
use clap::Parser;
use databend_common_meta_api::DatabaseApi;
use databend_common_meta_api::TableApi;
use databend_common_meta_api::txn_put_pb_with_ttl;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_common_tracing::FileConfig;
use databend_common_tracing::LogFormat;
use databend_common_tracing::StderrConfig;
use databend_common_tracing::init_logging;
use databend_common_version::METASRV_COMMIT_VERSION;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_kvapi::kvapi::KVApi;
use databend_meta_plugin_semaphore::Semaphore;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_types::MatchSeq;
use databend_meta_types::Operation;
use databend_meta_types::TxnRequest;
use databend_meta_types::UpsertKV;
use futures::TryStreamExt;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::sleep;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version = METASRV_COMMIT_VERSION.as_str(), author)]
struct Config {
    /// The prefix of keys to write.
    #[clap(long, default_value = "0")]
    pub prefix: u64,

    #[clap(long, default_value = "10")]
    pub client: u64,

    #[clap(long, default_value = "10000")]
    pub number: u64,

    #[clap(long, default_value = "warn,databend=info")]
    pub log_level: String,

    #[clap(long, env = "METASRV_GRPC_API_ADDRESS", default_value = "")]
    pub grpc_api_address: String,

    /// The RPC to benchmark:
    /// "upsert_kv": send kv-api upsert_kv,
    /// "table": create db, table and upsert_table_option;
    /// "get_table": single get_table() rpc;
    /// "table_copy_file": upsert table with copy file.
    /// "table_copy_file:{"file_cnt":100}": upsert table with 100 copy files. After ":" is a json config string
    /// "list": list all keys with default prefix pattern;
    /// "list:{"limit":50}": list up to 50 keys with default prefix pattern;
    /// "list:{"prefix":"custom_prefix"}": list all keys with custom prefix;
    /// "list:{"prefix":"custom_prefix","limit":50}": list up to 50 keys with custom prefix;
    /// "list:{"interval_ms":100}": add 100ms delay between reading each item (slow client simulation);
    /// "list:{"prefix":"custom_prefix","limit":50,"interval_ms":100}": combine all options;
    #[clap(long, default_value = "upsert_kv")]
    pub rpc: String,
}

#[tokio::main]
async fn main() {
    let config = Config::parse();

    let log_config = databend_common_tracing::Config {
        file: FileConfig {
            on: true,
            level: config.log_level.clone(),
            dir: "./.databend/logs".to_string(),
            format: LogFormat::Text,
            limit: 48,
            max_size: 4294967296,
        },
        stderr: StderrConfig {
            on: true,
            level: "WARN".to_string(),
            format: LogFormat::Text,
        },
        ..Default::default()
    };

    let _guards = init_logging("databend-metabench", &log_config, BTreeMap::new());

    println!("config: {:?}", config);
    if config.grpc_api_address.is_empty() {
        println!("grpc_api_address MUST not be empty!");
        return;
    }

    let client_handle = MetaGrpcClient::try_create_with_features(
        vec![config.grpc_api_address.clone()],
        "root",
        "xxx",
        None,
        None,
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )
    .unwrap();
    let client = MetaStore::R(client_handle);

    let start = Instant::now();
    let mut client_num = 0;
    let mut handles = Vec::new();
    while client_num < config.client {
        client_num += 1;
        let rpc = config.rpc.clone();
        let prefix = config.prefix;

        let cmd_and_param = rpc.splitn(2, ':').collect::<Vec<_>>();
        let cmd = cmd_and_param[0].to_string();
        let param = cmd_and_param.get(1).unwrap_or(&"").to_string();

        let client = client.clone();

        let handle = DatabendRuntime::spawn(
            async move {
                for i in 0..config.number {
                    if cmd == "upsert_kv" {
                        benchmark_upsert(&client, prefix, client_num, i).await;
                    } else if cmd == "table" {
                        benchmark_table(&client, prefix, client_num, i).await;
                    } else if cmd == "get_table" {
                        benchmark_get_table(&client, prefix, client_num, i).await;
                    } else if cmd == "table_copy_file" {
                        benchmark_table_copy_file(&client, prefix, client_num, i, &param).await;
                    } else if cmd == "semaphore" {
                        benchmark_semaphore(&client, prefix, client_num, i, &param).await;
                    } else if cmd == "list" {
                        benchmark_list(&client, prefix, client_num, i, &param).await;
                    } else {
                        unreachable!("Invalid config.rpc: {}", rpc);
                    }
                }
            },
            None,
        );
        handles.push(handle)
    }

    for handle in handles {
        handle.await.unwrap();
    }
    let end = Instant::now();
    println!(
        "benchmark client({}) * number({}) in {} milliseconds",
        config.client,
        config.number,
        end.duration_since(start).as_millis()
    );
}

async fn benchmark_upsert(client: &MetaStore, prefix: u64, client_num: u64, i: u64) {
    let node_key = || format!("{}-{}-{}", prefix, client_num, i);

    let seq = MatchSeq::Any;
    let value = Operation::Update(node_key().as_bytes().to_vec());

    let res = client
        .upsert_kv(UpsertKV::new(node_key(), seq, value, None))
        .await;

    print_res(i, "upsert_kv", &res);
}

async fn benchmark_table(client: &MetaStore, prefix: u64, client_num: u64, i: u64) {
    let tenant = || Tenant::new_literal(&format!("tenant-{}-{}", prefix, client_num));
    let db_name = || format!("db-{}-{}", prefix, client_num);
    let table_name = || format!("table-{}-{}", prefix, client_num);

    let tb_name_ident = || TableNameIdent {
        tenant: tenant(),
        db_name: db_name(),
        table_name: table_name(),
    };

    let res = client
        .create_database(CreateDatabaseReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident: DatabaseNameIdent::new(tenant(), db_name()),
            meta: Default::default(),
        })
        .await;

    print_res(i, "create_db", &res);
    let db_id = match res {
        Ok(res) => *res.db_id,
        Err(_) => 0,
    };

    let res = client
        .create_table(CreateTableReq {
            create_option: CreateOption::CreateIfNotExists,
            catalog_name: None,
            name_ident: tb_name_ident(),
            table_meta: Default::default(),
            as_dropped: false,
            table_properties: None,
            table_partition: None,
        })
        .await;

    print_res(i, "create_table", &res);

    let res = client
        .get_table(GetTableReq::new(&tenant(), db_name(), table_name()))
        .await;

    print_res(i, "get_table", &res);

    let t = res.unwrap();

    let res = client
        .upsert_table_option(UpsertTableOptionReq {
            table_id: t.ident.table_id,
            seq: MatchSeq::GE(t.ident.seq),
            options: Default::default(),
        })
        .await;

    print_res(i, "upsert_table_option", &res);

    let res = client
        .drop_table_by_id(DropTableByIdReq {
            if_exists: false,
            tenant: tenant(),
            db_id,
            db_name: db_name().to_string(),
            table_name: table_name(),
            tb_id: t.ident.table_id,
            engine: "FUSE".to_string(),
            temp_prefix: "".to_string(),
        })
        .await;

    print_res(i, "drop_table", &res);

    let res = client
        .create_table(CreateTableReq {
            create_option: CreateOption::CreateIfNotExists,
            catalog_name: None,
            name_ident: tb_name_ident(),
            table_meta: Default::default(),
            as_dropped: false,
            table_properties: None,
            table_partition: None,
        })
        .await;

    print_res(i, "create_table again", &res);
}

async fn benchmark_get_table(client: &MetaStore, prefix: u64, client_num: u64, i: u64) {
    let tenant = || Tenant::new_literal(&format!("tenant-{}-{}", prefix, client_num));
    let db_name = || format!("db-{}-{}", prefix, client_num);
    let table_name = || format!("table-{}-{}", prefix, client_num);

    let res = client
        .get_table(GetTableReq::new(&tenant(), db_name(), table_name()))
        .await;

    print_res(i, "get_table", &res);
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TableCopyFileConfig {
    file_cnt: u64,
    ttl_ms: Option<u64>,
}

impl Default for TableCopyFileConfig {
    fn default() -> Self {
        Self {
            file_cnt: 100,
            ttl_ms: None,
        }
    }
}

/// Benchmark upsert table with copy file.
async fn benchmark_table_copy_file(
    client: &MetaStore,
    prefix: u64,
    client_num: u64,
    i: u64,
    param: &str,
) {
    let param = if param.is_empty() {
        TableCopyFileConfig::default()
    } else {
        serde_json::from_str(param).unwrap()
    };

    let mut txn = TxnRequest::default();

    for file_index in 0..param.file_cnt {
        let copied_file_ident = TableCopiedFileNameIdent {
            table_id: prefix * 1_000_000 + client_num * 1_000 + i,
            file: format!("{}-{}-{}-{}", prefix, client_num, i, file_index),
        };
        let copied_file_value = TableCopiedFileInfo {
            etag: Some(format!("{}-{}-{}-{}", prefix, client_num, i, file_index)),
            content_length: 5,
            last_modified: Some(Utc::now()),
        };

        let put_op = txn_put_pb_with_ttl(
            &copied_file_ident,
            &copied_file_value,
            param.ttl_ms.map(Duration::from_millis),
        )
        .unwrap();

        txn.if_then.push(put_op);
    }

    let res = client.transaction(txn).await;

    print_res(i, "table_copy_file", &res);
    res.unwrap();
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct SemaphoreConfig {
    semaphores: u64,

    /// The capacity of resource in the semaphore.
    capacity: u64,

    /// Whether to generate a sem seq with the current timestamp,
    /// which reduce the conflict when enqueueing the permits.
    time_based: bool,

    /// The ttl if lease is not extended.
    ttl_ms: Option<u64>,

    /// The time a permit is held by the application for simulation
    hold_ms: Option<u64>,
}

impl Default for SemaphoreConfig {
    fn default() -> Self {
        Self {
            semaphores: 1,
            capacity: 100,
            time_based: false,
            ttl_ms: None,
            hold_ms: None,
        }
    }
}

impl SemaphoreConfig {
    pub fn ttl(&self) -> Duration {
        Duration::from_millis(self.ttl_ms.unwrap_or(3_000))
    }

    pub fn hold(&self) -> Duration {
        Duration::from_millis(self.hold_ms.unwrap_or(100))
    }
}

/// Benchmark semaphore acquire.
///
/// - `key_prefix` is used to distribut the load to separate key spaces.
/// - `client_num` is number of concurrent clients.
/// - `i` is the index of the current client.
/// - `param` is a json string of bench specific config.
async fn benchmark_semaphore(
    client: &MetaStore,
    key_prefix: u64,
    client_num: u64,
    i: u64,
    param: &str,
) {
    let param = if param.is_empty() {
        SemaphoreConfig::default()
    } else {
        serde_json::from_str(param).unwrap()
    };

    let sem_key = format!("sem-{}-{}", key_prefix, client_num % param.semaphores);
    let id = format!("cli-{client_num}-{i}th");

    let permit_str = format!("({sem_key}, id={id})");

    let mut sem = Semaphore::new(
        client.inner().clone(),
        &sem_key,
        param.capacity,
        param.ttl(),
    )
    .await;
    if param.time_based {
        sem.set_time_based_seq(None);
    } else {
        sem.set_storage_based_seq();
    }

    let permit_res = sem.acquire(&id).await;

    print_sem_res(i, format!("sem-acquired: {permit_str}",), &permit_res);

    let permit = match permit_res {
        Ok(permit) => permit,
        Err(e) => {
            println!("ERROR: Failed to acquire semaphore: {permit_str}: {}", e);
            return;
        }
    };

    sleep(param.hold()).await;

    print_sem_res(
        i,
        format!("sem-released: {permit_str}, {}", permit.stat()),
        &permit,
    );

    fn print_sem_res<D: Debug>(i: u64, typ: impl Display, res: &D) {
        println!("{:>10}-th {} result: {:?}", i, typ, res);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
struct ListConfig {
    /// Maximum number of keys to return in the list operation.
    /// If None, all matching keys are returned.
    limit: Option<usize>,
    /// The prefix to search for. If None, uses the default pattern "{prefix}-{client_num}".
    prefix: Option<String>,
    /// Interval in milliseconds to wait between reading each item from the stream.
    /// This simulates a slow client. If None or 0, no delay is added.
    interval_ms: Option<u64>,
}

/// Benchmark listing keys with a prefix.
async fn benchmark_list(client: &MetaStore, prefix: u64, client_num: u64, i: u64, param: &str) {
    let name = format!("client[{:>05}]-{}th", client_num, i);

    let config = if param.is_empty() {
        ListConfig::default()
    } else {
        serde_json::from_str(param).unwrap()
    };

    let key_prefix = config
        .prefix
        .clone()
        .unwrap_or_else(|| format!("{}-{}", prefix, client_num));

    if i % 100 == 0 {
        println!("{:>10} list using prefix: '{}'", name, key_prefix);
    }

    let start_time = Instant::now();
    let stream_res = client.inner().list(&key_prefix).await;
    let stream_returned_time = Instant::now();

    static TOTAL: AtomicU64 = AtomicU64::new(0);
    static ERROR: AtomicU64 = AtomicU64::new(0);

    TOTAL.fetch_add(1, Ordering::Relaxed);

    println!(
        "{:>10} list stream returned in {:?}, err: {:?}, total streams: {}, errors: {}",
        name,
        stream_returned_time.duration_since(start_time),
        stream_res.as_ref().err(),
        TOTAL.load(Ordering::Relaxed),
        ERROR.load(Ordering::Relaxed)
    );

    let mut strm = match stream_res {
        Ok(stream) => stream,
        Err(e) => {
            println!("{:>10} list error: {:?}", name, e);
            ERROR.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    let mut count = 0;

    while let Ok(Some(_item)) = strm.try_next().await {
        count += 1;

        // Apply interval delay if specified (simulate slow client)
        if let Some(interval_ms) = config.interval_ms {
            if interval_ms > 0 {
                sleep(Duration::from_millis(interval_ms)).await;
            }
        }

        // Apply limit if specified
        if let Some(limit) = config.limit {
            if count >= limit {
                break;
            }
        }

        if count % 10 == 9 {
            println!("{:>10} list found {} keys", name, count);
        }
    }

    println!(
        "{:>10} list found {} keys; total list: {}, error: {}",
        name,
        count,
        TOTAL.load(Ordering::Relaxed),
        ERROR.load(Ordering::Relaxed)
    );
}

fn print_res<D: Debug>(i: u64, typ: impl Display, res: &D) {
    if i % 100 == 0 {
        println!("{:>10}-th {} result: {:?}", i, typ, res);
    }
}
