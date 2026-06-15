use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use chrono::Utc;
use databend_driver::Client;
use databend_driver::Connection;
use databend_driver::RowWithStats;
use databend_driver::ServerStats;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

#[derive(Debug)]
struct BenchmarkConfig {
    benchmark_id: String,
    dataset: String,
    size: String,
    cache_size: String,
    version: String,
    database: String,
    tries: usize,
    user: String,
    password: String,
    gateway: String,
    warehouse: String,
    source: String,
    source_id: String,
    sha: String,
}

#[derive(Serialize)]
struct ResultRecord {
    date: String,
    dataset: String,
    database: String,
    version: String,
    warehouse: String,
    machine: String,
    tags: Vec<String>,
    result: Vec<Vec<f64>>,
    values: BTreeMap<String, Vec<f64>>,
    run_id: String,
    size: String,
    tries: usize,
    storage: String,
    cache_size: String,
    runner: String,
    timing_source: String,
    detail_sources: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    load_time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    query_details: BTreeMap<String, Vec<QueryAttempt>>,
}

#[derive(Clone, Serialize)]
struct ServerStatsRecord {
    total_rows: usize,
    total_bytes: usize,
    read_rows: usize,
    read_bytes: usize,
    write_rows: usize,
    write_bytes: usize,
    running_time_ms: f64,
    spill_file_nums: usize,
    spill_bytes: usize,
}

#[derive(Serialize)]
struct SystemHistoryRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    query_history: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    profile_history: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile_statistics_desc: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct QueryAttempt {
    attempt: usize,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_seconds: Option<f64>,
    client_wall_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    query_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_stats: Option<ServerStatsRecord>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    stats_samples: Vec<ServerStatsRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    system_history: Option<SystemHistoryRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

struct QueryAttemptWithHistory {
    attempt: QueryAttempt,
    history_receiver: Option<oneshot::Receiver<SystemHistoryRecord>>,
}

struct HistoryRequest {
    query_id: String,
    response: oneshot::Sender<SystemHistoryRecord>,
}

struct HistoryCollector {
    sender: mpsc::UnboundedSender<HistoryRequest>,
    worker: JoinHandle<()>,
}

fn env_or_default(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_string())
}

fn load_config() -> Result<BenchmarkConfig> {
    let benchmark_id = env::var("BENCHMARK_ID").unwrap_or_else(|_| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_string()
    });
    let dataset = env_or_default("BENCHMARK_DATASET", "hits");
    let size = env_or_default("BENCHMARK_SIZE", "Small");
    let cache_size = env::var("BENCHMARK_CACHE_SIZE")
        .unwrap_or_default()
        .trim()
        .to_string();
    let cache_size = if cache_size.is_empty() {
        "0".to_string()
    } else {
        cache_size
    };
    let version = env_or_default("BENCHMARK_VERSION", "");
    let database = env_or_default("BENCHMARK_DATABASE", "default");
    let tries_raw = env_or_default("BENCHMARK_TRIES", "3");
    let source = env_or_default("BENCHMARK_SOURCE", "");
    let source_id = env_or_default("BENCHMARK_SOURCE_ID", "");
    let sha = env_or_default("BENCHMARK_SHA", "");

    if version.is_empty() {
        bail!("Please set BENCHMARK_VERSION to run the benchmark.");
    }
    if dataset == "load" {
        bail!("BENCHMARK_DATASET=load is not supported by benchmark-cloud");
    }

    let tries: usize = tries_raw
        .parse()
        .with_context(|| format!("BENCHMARK_TRIES must be an integer, got {tries_raw}"))?;
    if !(1..=3).contains(&tries) {
        bail!("BENCHMARK_TRIES must be between 1 and 3, got {tries}");
    }

    let user = env_or_default("CLOUD_USER", "");
    let password = env_or_default("CLOUD_PASSWORD", "");
    let gateway = env_or_default("CLOUD_GATEWAY", "");
    let warehouse =
        env::var("CLOUD_WAREHOUSE").unwrap_or_else(|_| format!("benchmark-{benchmark_id}"));

    if user.is_empty() || password.is_empty() || gateway.is_empty() {
        bail!("Please set CLOUD_USER, CLOUD_PASSWORD and CLOUD_GATEWAY to run the benchmark.");
    }

    Ok(BenchmarkConfig {
        benchmark_id,
        dataset,
        size,
        cache_size,
        version,
        database,
        tries,
        user,
        password,
        gateway,
        warehouse,
        source,
        source_id,
        sha,
    })
}

fn build_dsn(
    config: &BenchmarkConfig,
    database: Option<&str>,
    warehouse: Option<&str>,
    login_disable: bool,
) -> String {
    let mut params = Vec::new();
    if login_disable {
        params.push("login=disable".to_string());
    }
    if let Some(warehouse) = warehouse {
        params.push(format!("warehouse={warehouse}"));
    }
    let query = if params.is_empty() {
        String::new()
    } else {
        format!("?{}", params.join("&"))
    };
    let db_path = database.map(|db| format!("/{db}")).unwrap_or_default();
    format!(
        "databend://{}:{}@{}:443{}{}",
        config.user, config.password, config.gateway, db_path, query
    )
}

fn quote_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn resolve_sql_dataset(dataset: &str) -> &str {
    let trimmed = dataset.trim_end_matches(|ch: char| ch.is_ascii_digit());
    if trimmed.is_empty() {
        dataset
    } else {
        trimmed
    }
}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn machine_for_size(size: &str) -> Result<&'static str> {
    match size {
        "Small" => Ok("Small"),
        "Large" => Ok("Large"),
        _ => bail!("Unsupported benchmark size: {size}"),
    }
}

fn stats_record(stats: &ServerStats) -> ServerStatsRecord {
    ServerStatsRecord {
        total_rows: stats.total_rows,
        total_bytes: stats.total_bytes,
        read_rows: stats.read_rows,
        read_bytes: stats.read_bytes,
        write_rows: stats.write_rows,
        write_bytes: stats.write_bytes,
        running_time_ms: stats.running_time_ms,
        spill_file_nums: stats.spill_file_nums,
        spill_bytes: stats.spill_bytes,
    }
}

async fn connect(dsn: String) -> Result<Connection> {
    Client::new(dsn)
        .with_name("databend-benchmark-cloud/0.1".to_string())
        .get_conn()
        .await
        .context("failed to connect Databend")
}

async fn execute_sql(conn: &Connection, sql: &str) -> Result<()> {
    for statement in split_sql_statements(sql) {
        let mut rows = conn.query_iter_ext(&statement).await?;
        while let Some(item) = rows.next().await {
            item?;
        }
    }
    Ok(())
}

async fn execute_sql_file(conn: &Connection, path: &Path) -> Result<()> {
    let sql = fs::read_to_string(path)
        .with_context(|| format!("failed to read SQL file {}", path.display()))?;
    if !sql.trim().is_empty() {
        execute_sql(conn, &sql).await?;
    }
    Ok(())
}

async fn wait_for_warehouse(conn: &Connection, warehouse: &str) -> Result<()> {
    println!("Waiting for warehouse {warehouse} to be ready...");
    let sql = format!("SHOW WAREHOUSES LIKE '{}'", quote_literal(warehouse));
    for _ in 0..=20 {
        let rows = conn.query_all(&sql).await?;
        let output = rows
            .iter()
            .map(|row| format!("{:?}", row.values()))
            .collect::<Vec<_>>()
            .join("\n");
        if output.contains("Running") {
            println!("Warehouse {warehouse} is running.");
            return Ok(());
        }
        println!("Warehouse not ready yet. Sleeping 10 seconds...");
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    bail!("Failed to start warehouse {warehouse} in time.");
}

async fn run_query_attempt(conn: &Connection, sql: &str, attempt: usize) -> QueryAttempt {
    let start = Instant::now();
    let mut stats_samples = Vec::new();
    let mut final_stats = None;
    let mut success = true;
    let mut error = None;

    match conn.query_iter_ext(sql).await {
        Ok(mut rows) => {
            while let Some(item) = rows.next().await {
                match item {
                    Ok(RowWithStats::Row(_)) => {}
                    Ok(RowWithStats::Stats(stats)) => {
                        let record = stats_record(&stats);
                        final_stats = Some(record.clone());
                        stats_samples.push(record);
                    }
                    Err(err) => {
                        success = false;
                        error = Some(err.to_string());
                        break;
                    }
                }
            }
        }
        Err(err) => {
            success = false;
            error = Some(err.to_string());
        }
    }

    let client_wall_ms = start.elapsed().as_millis();
    let query_id = conn.last_query_id();
    let elapsed_seconds = final_stats
        .as_ref()
        .map(|stats| round3(stats.running_time_ms / 1000.0))
        .or_else(|| success.then(|| round3(client_wall_ms as f64 / 1000.0)));

    QueryAttempt {
        attempt,
        success,
        elapsed_seconds,
        client_wall_ms,
        query_id,
        server_stats: final_stats,
        stats_samples,
        system_history: None,
        error,
    }
}

async fn collect_system_history(conn: &Connection, query_id: &str) -> SystemHistoryRecord {
    let mut query_history = None;
    let mut profile_history = Vec::new();
    let mut profile_statistics_desc = None;
    let mut error = None;

    for delay in [0_u64, 500, 1000, 2000] {
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }

        match collect_query_history(conn, query_id).await {
            Ok(Some(record)) => query_history = Some(record),
            Ok(None) => {}
            Err(err) => {
                error = Some(err.to_string());
                break;
            }
        }

        match collect_profile_history(conn, query_id).await {
            Ok((records, statistics_desc)) => {
                if !records.is_empty() {
                    profile_history = records;
                    profile_statistics_desc = statistics_desc;
                }
            }
            Err(err) => {
                error = Some(err.to_string());
                break;
            }
        }

        if query_history.is_some() && !profile_history.is_empty() {
            break;
        }
    }

    SystemHistoryRecord {
        query_history,
        profile_history,
        profile_statistics_desc,
        error,
    }
}

impl HistoryCollector {
    fn spawn(history_dsn: String) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<HistoryRequest>();
        // This benchmark crate is intentionally independent from the main Databend workspace.
        #[allow(clippy::disallowed_methods)]
        let worker = tokio::spawn(async move {
            match connect(history_dsn).await {
                Ok(conn) => {
                    while let Some(request) = receiver.recv().await {
                        let history = collect_system_history(&conn, &request.query_id).await;
                        let _ = request.response.send(history);
                    }
                    if let Err(err) = conn.close().await {
                        eprintln!("failed to close system history connection: {err}");
                    }
                }
                Err(err) => {
                    let error = err.to_string();
                    while let Some(request) = receiver.recv().await {
                        let _ = request.response.send(history_error(error.clone()));
                    }
                }
            }
        });

        Self { sender, worker }
    }

    fn queue(&self, query_id: String) -> Result<oneshot::Receiver<SystemHistoryRecord>> {
        let (response, receiver) = oneshot::channel();
        self.sender
            .send(HistoryRequest { query_id, response })
            .map_err(|_| anyhow!("system history collector is not running"))?;
        Ok(receiver)
    }

    async fn shutdown(self) {
        drop(self.sender);
        if let Err(err) = self.worker.await {
            eprintln!("system history collector task failed: {err}");
        }
    }
}

fn history_error(error: String) -> SystemHistoryRecord {
    SystemHistoryRecord {
        query_history: None,
        profile_history: Vec::new(),
        profile_statistics_desc: None,
        error: Some(error),
    }
}

async fn finalize_history(attempts: Vec<QueryAttemptWithHistory>) -> Vec<QueryAttempt> {
    let mut output = Vec::with_capacity(attempts.len());
    for mut item in attempts {
        if let Some(receiver) = item.history_receiver.take() {
            item.attempt.system_history = Some(match receiver.await {
                Ok(history) => history,
                Err(err) => {
                    history_error(format!("system history collector dropped result: {err}"))
                }
            });
        }
        output.push(item.attempt);
    }
    output
}

async fn collect_query_history(
    conn: &Connection,
    query_id: &str,
) -> Result<Option<serde_json::Value>> {
    let query_id_literal = quote_literal(query_id);
    let sql = format!(
        r#"
        SELECT to_string(object_construct(
            'query_id', query_id,
            'log_type', log_type,
            'log_type_name', log_type_name,
            'query_text', query_text,
            'query_kind', query_kind,
            'sql_user', sql_user,
            'handler_type', handler_type,
            'tenant_id', tenant_id,
            'cluster_id', cluster_id,
            'node_id', node_id,
            'event_time', event_time,
            'query_start_time', query_start_time,
            'query_duration_ms', query_duration_ms,
            'query_queued_duration_ms', query_queued_duration_ms,
            'current_database', current_database,
            'scan_rows', scan_rows,
            'scan_bytes', scan_bytes,
            'scan_io_bytes', scan_io_bytes,
            'scan_io_bytes_cost_ms', scan_io_bytes_cost_ms,
            'scan_partitions', scan_partitions,
            'total_partitions', total_partitions,
            'result_rows', result_rows,
            'result_bytes', result_bytes,
            'written_rows', written_rows,
            'written_bytes', written_bytes,
            'written_io_bytes', written_io_bytes,
            'written_io_bytes_cost_ms', written_io_bytes_cost_ms,
            'join_spilled_rows', join_spilled_rows,
            'join_spilled_bytes', join_spilled_bytes,
            'agg_spilled_rows', agg_spilled_rows,
            'agg_spilled_bytes', agg_spilled_bytes,
            'group_by_spilled_rows', group_by_spilled_rows,
            'group_by_spilled_bytes', group_by_spilled_bytes,
            'bytes_from_remote_disk', bytes_from_remote_disk,
            'bytes_from_local_disk', bytes_from_local_disk,
            'bytes_from_memory', bytes_from_memory,
            'exception_code', exception_code,
            'exception_text', exception_text,
            'server_version', server_version,
            'query_tag', query_tag,
            'has_profile', has_profile,
            'peek_memory_usage', peek_memory_usage,
            'session_id', session_id,
            'session_settings', session_settings
        ))
        FROM system_history.query_history
        WHERE query_id = '{query_id_literal}'
          AND log_type != 1
        ORDER BY event_time DESC
        LIMIT 1
        "#
    );

    let Some(row) = conn.query_row(&sql).await? else {
        return Ok(None);
    };
    let (raw,): (Option<String>,) = row.try_into().map_err(|err: String| anyhow!(err))?;
    Ok(raw.map(|raw| json_or_raw(&raw)))
}

async fn collect_profile_history(
    conn: &Connection,
    query_id: &str,
) -> Result<(Vec<serde_json::Value>, Option<serde_json::Value>)> {
    let query_id_literal = quote_literal(query_id);
    let sql = format!(
        r#"
        SELECT to_string(object_construct(
            'timestamp', timestamp,
            'query_id', query_id,
            'profiles', profiles
        )),
        to_string(statistics_desc)
        FROM system_history.profile_history
        WHERE query_id = '{query_id_literal}'
        ORDER BY timestamp
        "#
    );

    let rows = conn.query_all(&sql).await?;
    let mut records = Vec::with_capacity(rows.len());
    let mut statistics_desc = None;
    for row in rows {
        let (raw, statistics_desc_raw): (Option<String>, Option<String>) =
            row.try_into().map_err(|err: String| anyhow!(err))?;
        if let Some(raw) = raw {
            records.push(json_or_raw(&raw));
        }
        if statistics_desc.is_none() {
            statistics_desc = statistics_desc_raw.map(|raw| json_or_raw(&raw));
        }
    }

    Ok((records, statistics_desc))
}

fn json_or_raw(raw: &str) -> serde_json::Value {
    serde_json::from_str(raw).unwrap_or_else(|_| serde_json::json!({ "raw": raw }))
}

fn pad_attempts(values: &mut Vec<f64>, target_size: usize) {
    let Some(last) = values.last().copied() else {
        return;
    };
    while values.len() < target_size {
        values.push(last);
    }
}

fn write_result_files(script_dir: &Path, record: &ResultRecord) -> Result<()> {
    let result_path = script_dir.join("result.json");
    let cache_suffix = if record.cache_size.is_empty() {
        String::new()
    } else {
        format!("-cache-{}", record.cache_size)
    };
    let final_result_path = script_dir.join(format!(
        "result-{}-cloud-{}{}.json",
        record.dataset, record.size, cache_suffix
    ));
    let ndjson_path = script_dir.join(format!(
        "result-{}-cloud-{}{}-{}.ndjson",
        record.dataset, record.size, cache_suffix, record.run_id
    ));

    let pretty = serde_json::to_string_pretty(record)?;
    fs::write(&result_path, format!("{pretty}\n"))?;
    fs::write(&final_result_path, format!("{pretty}\n"))?;
    fs::write(
        &ndjson_path,
        format!("{}\n", serde_json::to_string(record)?),
    )?;

    println!(
        "Wrote JSON results to {} and {}",
        result_path.display(),
        final_result_path.display()
    );
    println!("Wrote NDJSON results to {}", ndjson_path.display());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    println!("#######################################################");
    println!("Running benchmark for Databend Cloud with S3 storage...");

    let script_dir = env::current_dir().context("failed to get current directory")?;
    let sql_dataset = resolve_sql_dataset(&config.dataset);
    let dataset_dir = script_dir.join(sql_dataset);
    if !dataset_dir.exists() {
        bail!("Dataset directory {} does not exist", dataset_dir.display());
    }
    if sql_dataset != config.dataset {
        println!(
            "Dataset {} uses SQL directory {}",
            config.dataset, sql_dataset
        );
    }

    let machine = machine_for_size(&config.size)?.to_string();
    let mut system = None;
    let mut comment = None;
    if !config.source.is_empty() && !config.source_id.is_empty() {
        system = Some(match config.source.as_str() {
            "pr" => format!("Databend(PR#{})", config.source_id),
            "release" => format!("Databend(Release@{})", config.source_id),
            _ => bail!("Unsupported benchmark source: {}", config.source),
        });
    } else if !config.source.is_empty() || !config.source_id.is_empty() {
        bail!("Both BENCHMARK_SOURCE and BENCHMARK_SOURCE_ID must be provided together.");
    }
    if !config.sha.is_empty() {
        comment = Some(format!("commit: {}", config.sha));
    }

    let mut record = ResultRecord {
        date: Utc::now().format("%Y-%m-%d").to_string(),
        dataset: config.dataset.clone(),
        database: config.database.clone(),
        version: config.version.clone(),
        warehouse: config.warehouse.clone(),
        machine,
        tags: vec!["s3".to_string(), format!("cache-{}", config.cache_size)],
        result: Vec::new(),
        values: BTreeMap::new(),
        run_id: config.benchmark_id.clone(),
        size: config.size.clone(),
        tries: config.tries,
        storage: "s3".to_string(),
        cache_size: config.cache_size.clone(),
        runner: "rust/databend-driver".to_string(),
        timing_source: "server_stats.running_time_ms".to_string(),
        detail_sources: vec![
            "databend-driver query_iter_ext".to_string(),
            "system_history.query_history".to_string(),
            "system_history.profile_history".to_string(),
        ],
        load_time: None,
        data_size: None,
        system,
        comment,
        query_details: BTreeMap::new(),
    };

    let admin_conn = connect(build_dsn(&config, None, Some("default"), true)).await?;
    let warehouse_literal = quote_literal(&config.warehouse);
    println!("Creating warehouse {}...", config.warehouse);
    execute_sql(
        &admin_conn,
        &format!("DROP WAREHOUSE IF EXISTS '{warehouse_literal}';"),
    )
    .await?;
    execute_sql(
        &admin_conn,
        &format!(
            "CREATE WAREHOUSE '{warehouse_literal}' WITH version='{}' warehouse_size='{}' cache_size={};",
            quote_literal(&config.version),
            quote_literal(&config.size),
            config.cache_size
        ),
    )
    .await?;
    execute_sql(&admin_conn, "SHOW WAREHOUSES;").await?;
    wait_for_warehouse(&admin_conn, &config.warehouse).await?;

    let query_conn = connect(build_dsn(
        &config,
        Some(&config.database),
        Some(&config.warehouse),
        false,
    ))
    .await?;
    let history_dsn = build_dsn(
        &config,
        Some(&config.database),
        Some(&config.warehouse),
        false,
    );
    let history_collector = HistoryCollector::spawn(history_dsn);

    println!("Checking session settings...");
    execute_sql(
        &query_conn,
        "select * from system.settings where value != default;",
    )
    .await?;

    let analyze_sql = dataset_dir.join("analyze.sql");
    if analyze_sql.exists() {
        println!("Analyze tables...");
        execute_sql_file(&query_conn, &analyze_sql).await?;
    }

    println!("Running queries...");
    let queries = sorted_query_files(&dataset_dir)?;
    if queries.is_empty() {
        bail!("No queries found under {}", dataset_dir.display());
    }
    let mut pending_query_details: BTreeMap<String, Vec<QueryAttemptWithHistory>> = BTreeMap::new();

    for (query_num, query_file) in queries.iter().enumerate() {
        println!("==> Running Q{query_num}: {}", query_file.display());
        let query_sql = fs::read_to_string(query_file)
            .with_context(|| format!("failed to read query {}", query_file.display()))?;
        record.result.push(Vec::new());
        let query_key = format!("Q{query_num}");
        record.values.insert(query_key.clone(), Vec::new());
        let mut attempt_results = Vec::new();

        for attempt in 1..=config.tries {
            let mut detail = run_query_attempt(&query_conn, &query_sql, attempt).await;
            let history_receiver = if let Some(query_id) = detail.query_id.clone() {
                match history_collector.queue(query_id) {
                    Ok(receiver) => Some(receiver),
                    Err(err) => {
                        detail.system_history = Some(history_error(err.to_string()));
                        None
                    }
                }
            } else {
                None
            };

            if detail.success {
                if let Some(elapsed) = detail.elapsed_seconds {
                    println!("Q{query_num}[{attempt}] succeeded in {elapsed:.3} seconds");
                    record.result[query_num].push(elapsed);
                    record
                        .values
                        .get_mut(&query_key)
                        .expect("query value vector exists")
                        .push(elapsed);
                } else {
                    println!("Q{query_num}[{attempt}] succeeded without timing info");
                }
            } else {
                println!(
                    "Q{query_num}[{attempt}] failed: {}",
                    detail.error.as_deref().unwrap_or("unknown error")
                );
            }
            attempt_results.push(QueryAttemptWithHistory {
                attempt: detail,
                history_receiver,
            });
        }

        if config.tries < 3 {
            pad_attempts(&mut record.result[query_num], 3);
            pad_attempts(
                record
                    .values
                    .get_mut(&query_key)
                    .expect("query value vector exists"),
                3,
            );
        }
        pending_query_details.insert(query_key, attempt_results);
    }

    println!("Collecting system history details...");
    for (query_key, attempts) in pending_query_details {
        record
            .query_details
            .insert(query_key, finalize_history(attempts).await);
    }
    history_collector.shutdown().await;

    let cleanup_result = cleanup(&config).await;
    let write_result = write_result_files(&script_dir, &record);
    cleanup_result?;
    write_result?;
    Ok(())
}

async fn cleanup(config: &BenchmarkConfig) -> Result<()> {
    let cleanup_conn = connect(build_dsn(config, None, Some("default"), true)).await?;
    println!("Dropping warehouse {}...", config.warehouse);
    execute_sql(
        &cleanup_conn,
        &format!(
            "DROP WAREHOUSE IF EXISTS '{}';",
            quote_literal(&config.warehouse)
        ),
    )
    .await
}

fn sorted_query_files(dataset_dir: &Path) -> Result<Vec<PathBuf>> {
    let query_dir = dataset_dir.join("queries");
    let mut files = Vec::new();
    for entry in fs::read_dir(&query_dir)
        .with_context(|| format!("failed to read query directory {}", query_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "sql") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_backtick = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while let Some(ch) = chars.next() {
        if in_line_comment {
            current.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            current.push(ch);
            if ch == '*' && chars.peek() == Some(&'/') {
                current.push(chars.next().expect("peeked slash"));
                in_block_comment = false;
            }
            continue;
        }

        if in_single {
            current.push(ch);
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    current.push(chars.next().expect("peeked quote"));
                } else {
                    in_single = false;
                }
            }
            continue;
        }

        if in_double {
            current.push(ch);
            if ch == '"' {
                in_double = false;
            }
            continue;
        }

        if in_backtick {
            current.push(ch);
            if ch == '`' {
                in_backtick = false;
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '"' => {
                in_double = true;
                current.push(ch);
            }
            '`' => {
                in_backtick = true;
                current.push(ch);
            }
            '-' if chars.peek() == Some(&'-') => {
                current.push(ch);
                current.push(chars.next().expect("peeked dash"));
                in_line_comment = true;
            }
            '/' if chars.peek() == Some(&'*') => {
                current.push(ch);
                current.push(chars.next().expect("peeked star"));
                in_block_comment = true;
            }
            ';' => {
                let statement = current.trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let statement = current.trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }
    statements
}
