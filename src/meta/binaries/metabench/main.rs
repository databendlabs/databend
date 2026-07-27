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
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use base2histogram::Histogram;
use chrono::Utc;
use clap::Parser;
use databend_common_meta_api::DatabaseApi;
use databend_common_meta_api::TableApi;
use databend_common_meta_api::fetch_id;
use databend_common_meta_api::txn_put_pb;
use databend_common_meta_api::txn_put_pb_with_ttl;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_meta_app::schema::TableIdList;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableMeta;
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
use databend_meta::runtime_api::SpawnApi;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_client::kvapi::KVApi;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::Operation;
use databend_meta_client::types::TxnRequest;
use databend_meta_client::types::UpsertKV;
use databend_meta_plugin_semaphore::Semaphore;
use databend_meta_runtime::DatabendRuntime;
use futures::TryStreamExt;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::sleep;

struct BenchStats {
    total: AtomicU64,
    success: AtomicU64,
    error: AtomicU64,
    latency_total_us: AtomicU64,
    latency_max_us: AtomicU64,
    latency_histogram: Mutex<Histogram>,
}

#[derive(Debug)]
struct BenchStatsSnapshot {
    total: u64,
    success: u64,
    error: u64,
    latency_total_us: u64,
    latency_max_us: u64,
    latency_histogram: Histogram,
}

impl BenchStats {
    fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            success: AtomicU64::new(0),
            error: AtomicU64::new(0),
            latency_total_us: AtomicU64::new(0),
            latency_max_us: AtomicU64::new(0),
            latency_histogram: Mutex::new(Histogram::new()),
        }
    }

    fn record(&self, elapsed: Duration, success: bool) {
        let latency_us = elapsed.as_micros().min(u64::MAX as u128) as u64;

        self.total.fetch_add(1, Ordering::Relaxed);
        if success {
            self.success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.error.fetch_add(1, Ordering::Relaxed);
        }
        self.latency_total_us
            .fetch_add(latency_us, Ordering::Relaxed);
        update_max(&self.latency_max_us, latency_us);

        self.latency_histogram.lock().unwrap().record(latency_us);
    }

    fn snapshot(&self) -> BenchStatsSnapshot {
        let latency_histogram = self.latency_histogram.lock().unwrap().clone();
        BenchStatsSnapshot {
            total: self.total.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            error: self.error.load(Ordering::Relaxed),
            latency_total_us: self.latency_total_us.load(Ordering::Relaxed),
            latency_max_us: self.latency_max_us.load(Ordering::Relaxed),
            latency_histogram,
        }
    }
}

impl BenchStatsSnapshot {
    fn avg_us(&self) -> u64 {
        if self.total == 0 {
            return 0;
        }
        self.latency_total_us / self.total
    }

    fn percentile_us(&self, percentile: f64) -> u64 {
        self.latency_histogram.percentile(percentile)
    }

    fn histogram_line(&self) -> String {
        let parts = self
            .latency_histogram
            .bucket_data()
            .filter(|bucket| bucket.count() > 0)
            .map(|bucket| {
                if bucket.is_last() {
                    format!(
                        "b{}[{}us,{}us]={}",
                        bucket.index(),
                        bucket.left(),
                        bucket.right(),
                        bucket.count()
                    )
                } else {
                    format!(
                        "b{}[{}us,{}us)={}",
                        bucket.index(),
                        bucket.left(),
                        bucket.right(),
                        bucket.count()
                    )
                }
            })
            .collect::<Vec<_>>();

        if parts.is_empty() {
            "empty".to_string()
        } else {
            parts.join(" ")
        }
    }
}

fn update_max(current: &AtomicU64, value: u64) {
    let mut old = current.load(Ordering::Relaxed);
    while value > old {
        match current.compare_exchange_weak(old, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => old = next,
        }
    }
}

fn rate_per_sec(count: u64, elapsed_ms: u128) -> f64 {
    if elapsed_ms == 0 {
        0.0
    } else {
        count as f64 * 1000.0 / elapsed_ms as f64
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version = METASRV_COMMIT_VERSION.as_str(), author)]
struct Config {
    /// The prefix of keys to write.
    #[clap(long, default_value = "0")]
    pub prefix: u64,

    #[clap(long, default_value = "10")]
    pub client: u64,

    /// The number of independent MetaGrpcClient handles to spread benchmark clients across.
    #[clap(long, default_value = "1")]
    pub client_pool_size: u64,

    #[clap(long, default_value = "10000")]
    pub number: u64,

    #[clap(long, default_value = "warn,databend=info")]
    pub log_level: String,

    #[clap(long, env = "METASRV_GRPC_API_ADDRESS", default_value = "")]
    pub grpc_api_address: String,

    /// The RPC to benchmark:
    /// "upsert_kv": send kv-api upsert_kv,
    /// "table": create db, table and upsert_table_option;
    /// "create_tables": accumulate distinct tables, a new db every `db_size` tables;
    /// "create_tables:{"db_size":1000,"meta_size":600}": after ":" is a json config string;
    /// "batch_create_tables": bulk-load, one txn writes the records of `batch` tables;
    /// "batch_create_tables:{"batch":1000,"db_size":1000,"meta_size":600}";
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

    /// Stop issuing new operations after this many seconds; 0 means no time limit.
    /// Each client still runs at most `--number` operations.
    #[clap(long, default_value = "0")]
    pub run_secs: u64,
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

    let client_pool_size = config.client_pool_size.max(1).min(config.client.max(1));
    let clients = (0..client_pool_size)
        .map(|_| create_remote_meta_store(&config.grpc_api_address))
        .collect::<Vec<_>>();
    println!("effective client_pool_size: {}", client_pool_size);

    let start = Instant::now();
    let deadline = if config.run_secs > 0 {
        Some(start + Duration::from_secs(config.run_secs))
    } else {
        None
    };
    let mut client_num = 0;
    let mut handles = Vec::new();
    let stats = Arc::new(BenchStats::new());
    while client_num < config.client {
        client_num += 1;
        let rpc = config.rpc.clone();
        let prefix = config.prefix;

        let cmd_and_param = rpc.splitn(2, ':').collect::<Vec<_>>();
        let cmd = cmd_and_param[0].to_string();
        let param = cmd_and_param.get(1).unwrap_or(&"").to_string();

        let client = clients[((client_num - 1) % client_pool_size) as usize].clone();
        let stats = stats.clone();
        let number = config.number;

        let handle = DatabendRuntime::spawn(
            async move {
                for i in 0..number {
                    if let Some(deadline) = deadline {
                        if Instant::now() >= deadline {
                            break;
                        }
                    }
                    let op_start = Instant::now();
                    let success =
                        run_benchmark_once(&client, &cmd, &rpc, prefix, client_num, i, &param)
                            .await;
                    stats.record(op_start.elapsed(), success);
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

    let elapsed_ms = end.duration_since(start).as_millis();
    let snapshot = stats.snapshot();
    let qps = rate_per_sec(snapshot.total, elapsed_ms);
    let success_qps = rate_per_sec(snapshot.success, elapsed_ms);
    let error_qps = rate_per_sec(snapshot.error, elapsed_ms);

    println!(
        "benchmark summary: total={} success={} error={} elapsed_ms={} qps={:.1} success_qps={:.1} error_qps={:.1} avg_us={} max_us={} p50_us={} p90_us={} p95_us={} p99_us={} client_pool_size={}",
        snapshot.total,
        snapshot.success,
        snapshot.error,
        elapsed_ms,
        qps,
        success_qps,
        error_qps,
        snapshot.avg_us(),
        snapshot.latency_max_us,
        snapshot.percentile_us(0.50),
        snapshot.percentile_us(0.90),
        snapshot.percentile_us(0.95),
        snapshot.percentile_us(0.99),
        client_pool_size,
    );
    println!("benchmark latency histogram: {}", snapshot.histogram_line());
}

/// `grpc_api_address` is a comma-separated endpoint list; give the client every
/// node so it can follow leader changes instead of forwarding via one node.
fn create_remote_meta_store(grpc_api_address: &str) -> MetaStore {
    let client_handle = MetaGrpcClient::try_create_with_features(
        grpc_api_address.split(',').map(str::to_string).collect(),
        "root",
        "xxx",
        None,
        None,
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )
    .unwrap();

    MetaStore::R(client_handle)
}

async fn run_benchmark_once(
    client: &MetaStore,
    cmd: &str,
    rpc: &str,
    prefix: u64,
    client_num: u64,
    i: u64,
    param: &str,
) -> bool {
    if cmd == "upsert_kv" {
        benchmark_upsert(client, prefix, client_num, i).await
    } else if cmd == "table" {
        benchmark_table(client, prefix, client_num, i).await
    } else if cmd == "create_tables" {
        benchmark_create_tables(client, prefix, client_num, i, param).await
    } else if cmd == "batch_create_tables" {
        benchmark_batch_create_tables(client, prefix, client_num, i, param).await
    } else if cmd == "get_table" {
        benchmark_get_table(client, prefix, client_num, i).await
    } else if cmd == "get_table_rand" {
        benchmark_get_table_rand(client, client_num, i, param).await
    } else if cmd == "table_copy_file" {
        benchmark_table_copy_file(client, prefix, client_num, i, param).await
    } else if cmd == "semaphore" {
        benchmark_semaphore(client, prefix, client_num, i, param).await
    } else if cmd == "list" {
        benchmark_list(client, prefix, client_num, i, param).await
    } else {
        unreachable!("Invalid config.rpc: {}", rpc);
    }
}

async fn benchmark_upsert(client: &MetaStore, prefix: u64, client_num: u64, i: u64) -> bool {
    let node_key = || format!("{}-{}-{}", prefix, client_num, i);

    let seq = MatchSeq::Any;
    let value = Operation::Update(node_key().as_bytes().to_vec());

    let res = client
        .upsert_kv(UpsertKV::new(node_key(), seq, value, None))
        .await;

    print_res(i, "upsert_kv", &res);
    res.is_ok()
}

async fn benchmark_table(client: &MetaStore, prefix: u64, client_num: u64, i: u64) -> bool {
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
            override_existing: false,
            catalog_name: None,
            name_ident: DatabaseNameIdent::new(tenant(), db_name()),
            meta: Default::default(),
        })
        .await;

    print_res(i, "create_db", &res);
    let mut success = res.is_ok();
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
            materialized_view: None,
            table_properties: None,
            table_partition: None,
        })
        .await;

    print_res(i, "create_table", &res);
    success &= res.is_ok();

    let res = client
        .get_table(GetTableReq::new(&tenant(), db_name(), table_name()))
        .await;

    print_res(i, "get_table", &res);
    success &= res.is_ok();

    let t = match res {
        Ok(t) => t,
        Err(_) => return false,
    };

    let res = client
        .upsert_table_option(UpsertTableOptionReq {
            table_id: t.ident.table_id,
            seq: MatchSeq::GE(t.ident.seq),
            options: Default::default(),
        })
        .await;

    print_res(i, "upsert_table_option", &res);
    success &= res.is_ok();

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
    success &= res.is_ok();

    let res = client
        .create_table(CreateTableReq {
            create_option: CreateOption::CreateIfNotExists,
            catalog_name: None,
            name_ident: tb_name_ident(),
            table_meta: Default::default(),
            as_dropped: false,
            materialized_view: None,
            table_properties: None,
            table_partition: None,
        })
        .await;

    print_res(i, "create_table again", &res);
    success && res.is_ok()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CreateTablesConfig {
    /// Number of tables per database: client `c` puts table `t-{i}` into db `db-{prefix}-{c}-{i/db_size}`.
    db_size: u64,
    /// Bytes of padding in `TableMeta.options`, to control the serialized TableMeta size.
    meta_size: usize,
}

impl Default for CreateTablesConfig {
    fn default() -> Self {
        Self {
            db_size: 1000,
            meta_size: 600,
        }
    }
}

/// Create distinct tables that accumulate, unlike `table` which churns a single table.
///
/// Each client works in its own databases so that concurrent creates do not
/// conflict on the same db_meta record.
async fn benchmark_create_tables(
    client: &MetaStore,
    prefix: u64,
    client_num: u64,
    i: u64,
    param: &str,
) -> bool {
    let param: CreateTablesConfig = if param.is_empty() {
        Default::default()
    } else {
        serde_json::from_str(param).unwrap()
    };

    let tenant = Tenant::new_literal(&format!("bench-{}", prefix));
    let db_name = format!("db-{}-{}-{}", prefix, client_num, i / param.db_size);

    if i % param.db_size == 0 {
        let res = client
            .create_database(CreateDatabaseReq {
                override_existing: false,
                catalog_name: None,
                name_ident: DatabaseNameIdent::new(&tenant, &db_name),
                meta: Default::default(),
            })
            .await;
        print_res(i, "create_db", &res);
    }

    let mut table_meta = TableMeta {
        engine: "FUSE".to_string(),
        ..Default::default()
    };
    table_meta
        .options
        .insert("bench_pad".to_string(), "x".repeat(param.meta_size));

    let res = client
        .create_table(CreateTableReq {
            create_option: CreateOption::CreateIfNotExists,
            catalog_name: None,
            name_ident: TableNameIdent {
                tenant,
                db_name,
                table_name: format!("t-{}", i),
            },
            table_meta,
            as_dropped: false,
            materialized_view: None,
            table_properties: None,
            table_partition: None,
        })
        .await;

    print_res(i, "create_tables", &res);
    res.is_ok()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct BatchCreateTablesConfig {
    /// Number of tables written in one transaction.
    batch: u64,
    /// Number of tables per database.
    db_size: u64,
    /// Bytes of padding in `TableMeta.options`, to control the serialized TableMeta size.
    meta_size: usize,
}

impl Default for BatchCreateTablesConfig {
    fn default() -> Self {
        Self {
            batch: 1000,
            db_size: 1000,
            meta_size: 600,
        }
    }
}

/// Resolve a database id, creating the database if it does not exist yet.
async fn ensure_db(client: &MetaStore, tenant: &Tenant, db_name: &str) -> Option<u64> {
    let res = client
        .create_database(CreateDatabaseReq {
            override_existing: false,
            catalog_name: None,
            name_ident: DatabaseNameIdent::new(tenant, db_name),
            meta: Default::default(),
        })
        .await;

    match res {
        Ok(reply) => Some(*reply.db_id),
        // Likely DatabaseAlreadyExists, e.g. filled by a previous batch.
        Err(_) => {
            let res = client
                .get_database(GetDatabaseReq::new(tenant.clone(), db_name))
                .await;
            match res {
                Ok(info) => Some(*info.database_id),
                Err(e) => {
                    println!("ERROR: get_database({}) failed: {:?}", db_name, e);
                    None
                }
            }
        }
    }
}

/// Bulk-load tables: one transaction writes the records of `batch` tables,
/// instead of one id-allocation plus one conditional transaction per table.
/// Use it to fill a cluster to a target data volume fast; use `create_tables`
/// to measure the real per-table DDL path.
///
/// Table ids are `fetch_id() * 1_000_000 + offset`: one generator bump per
/// batch keeps ids unique across clients, runs and restarts on the same
/// cluster. Do not mix with `create_table`-based load on a cluster that has
/// to stay consistent: normal ids could collide with this id space once the
/// generator itself exceeds 1_000_000.
///
/// Per-table records match `create_table` exactly (name->id, id->meta, name
/// history, id->name); the db_meta seq bump of the normal path is skipped.
async fn benchmark_batch_create_tables(
    client: &MetaStore,
    prefix: u64,
    client_num: u64,
    i: u64,
    param: &str,
) -> bool {
    let param: BatchCreateTablesConfig = if param.is_empty() {
        Default::default()
    } else {
        serde_json::from_str(param).unwrap()
    };
    assert!(
        param.batch <= 1_000_000,
        "batch must be <= 1_000_000, the id space of one generator bump"
    );

    let tenant = Tenant::new_literal(&format!("bench-{}", prefix));

    let gen_id = match fetch_id(client, IdGenerator::table_id()).await {
        Ok(id) => id,
        Err(e) => {
            println!("ERROR: fetch_id failed: {:?}", e);
            return false;
        }
    };

    let mut txn = TxnRequest::default();
    let mut current_db: Option<(u64, u64)> = None; // (db_index, db_id)

    for k in 0..param.batch {
        let table_index = i * param.batch + k;
        let db_index = table_index / param.db_size;

        let db_id = match current_db {
            Some((index, id)) if index == db_index => id,
            _ => {
                let db_name = format!("db-{}-{}-{}", prefix, client_num, db_index);
                let Some(id) = ensure_db(client, &tenant, &db_name).await else {
                    return false;
                };
                current_db = Some((db_index, id));
                id
            }
        };

        let table_id = gen_id * 1_000_000 + k;
        let table_name = format!("t-{}", table_index);

        let mut table_meta = TableMeta {
            engine: "FUSE".to_string(),
            ..Default::default()
        };
        table_meta
            .options
            .insert("bench_pad".to_string(), "x".repeat(param.meta_size));

        let mut id_list = TableIdList::new();
        id_list.append(table_id);

        let name_key = DBIdTableName {
            db_id,
            table_name: table_name.clone(),
        };

        txn.if_then.extend([
            // name -> id
            txn_put_pb(&name_key, &TableId::new(table_id)),
            // id -> meta
            txn_put_pb(&TableId::new(table_id), &table_meta),
            // name history
            txn_put_pb(
                &TableIdHistoryIdent {
                    database_id: db_id,
                    table_name,
                },
                &id_list,
            ),
            // id -> name
            txn_put_pb(&TableIdToName { table_id }, &name_key),
        ]);
    }

    let res = client.transaction(txn).await;

    print_res(i, "batch_create_tables", &res);
    res.is_ok()
}

async fn benchmark_get_table(client: &MetaStore, prefix: u64, client_num: u64, i: u64) -> bool {
    let tenant = || Tenant::new_literal(&format!("tenant-{}-{}", prefix, client_num));
    let db_name = || format!("db-{}-{}", prefix, client_num);
    let table_name = || format!("table-{}-{}", prefix, client_num);

    let res = client
        .get_table(GetTableReq::new(&tenant(), db_name(), table_name()))
        .await;

    print_res(i, "get_table", &res);
    res.is_ok()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct GetTableRandConfig {
    /// `--prefix` of the `batch_create_tables` fill round to read from.
    fill_prefix: u64,
    /// `--client` of the fill round.
    fill_clients: u64,
    /// Tables each fill client created (`--number` x `batch`).
    tables_per_client: u64,
    /// `db_size` of the fill round.
    db_size: u64,
}

/// Read one uniformly pseudo-random table created by a previous
/// `batch_create_tables` round: tenant/db -> db_id -> table, the same
/// lookup path a real by-name access takes.
async fn benchmark_get_table_rand(
    client: &MetaStore,
    client_num: u64,
    i: u64,
    param: &str,
) -> bool {
    let param: GetTableRandConfig = serde_json::from_str(param).unwrap();

    // splitmix64 on (client_num, i): deterministic uniform spread.
    let mut x = (client_num << 32) ^ i;
    let mut next = || {
        x = x.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    };
    // client_num in the fill run is 1-based.
    let c = next() % param.fill_clients + 1;
    let n = next() % param.tables_per_client;

    let tenant = Tenant::new_literal(&format!("bench-{}", param.fill_prefix));
    let db_name = format!("db-{}-{}-{}", param.fill_prefix, c, n / param.db_size);
    let table_name = format!("t-{}", n);

    let res = client
        .get_table(GetTableReq::new(&tenant, &db_name, &table_name))
        .await;

    if let Err(e) = &res {
        println!(
            "ERROR: get_table_rand({}/{}) failed: {:?}",
            db_name, table_name, e
        );
    } else if i % 100_000 == 0 {
        println!("{:>10}-th get_table_rand ok: {}/{}", i, db_name, table_name);
    }
    res.is_ok()
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
) -> bool {
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
        );

        txn.if_then.push(put_op);
    }

    let res = client.transaction(txn).await;

    print_res(i, "table_copy_file", &res);
    res.is_ok()
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
) -> bool {
    fn print_sem_res<D: Debug>(i: u64, typ: impl Display, res: &D) {
        println!("{:>10}-th {} result: {:?}", i, typ, res);
    }

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
            return false;
        }
    };

    sleep(param.hold()).await;

    print_sem_res(
        i,
        format!("sem-released: {permit_str}, {}", permit.stat()),
        &permit,
    );
    true
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
async fn benchmark_list(
    client: &MetaStore,
    prefix: u64,
    client_num: u64,
    i: u64,
    param: &str,
) -> bool {
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
            return false;
        }
    };

    let mut count = 0;

    let mut success = true;
    loop {
        match strm.try_next().await {
            Ok(Some(_item)) => {
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
            Ok(None) => {
                break;
            }
            Err(e) => {
                println!("{:>10} list stream error: {:?}", name, e);
                ERROR.fetch_add(1, Ordering::Relaxed);
                success = false;
                break;
            }
        }
    }

    println!(
        "{:>10} list found {} keys; total list: {}, error: {}",
        name,
        count,
        TOTAL.load(Ordering::Relaxed),
        ERROR.load(Ordering::Relaxed)
    );
    success
}

fn print_res<D: Debug>(i: u64, typ: impl Display, res: &D) {
    if i % 100 == 0 {
        println!("{:>10}-th {} result: {:?}", i, typ, res);
    }
}
