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

#![feature(try_blocks)]

mod context;
mod datablock;
mod dataframe;
mod schema;
mod utils;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_config::BuiltInConfig;
use databend_common_config::InnerConfig;
use databend_common_config::UserAuthConfig;
use databend_common_config::UserConfig;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_version::BUILD_INFO;
use databend_query::GlobalServices;
use pyo3::prelude::*;
use utils::RUNTIME;

pub static EMBEDDED_INIT_STATE: AtomicBool = AtomicBool::new(false);

#[pyfunction]
#[pyo3(signature = (data_path = ".databend"))]
fn init_embedded(_py: Python, data_path: &str) -> PyResult<()> {
    // Check if already initialized
    if EMBEDDED_INIT_STATE.load(Ordering::Acquire) {
        return Ok(());
    }

    // Create configuration and initialize services
    let conf = create_embedded_config(data_path).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Config creation failed: {}", e))
    })?;

    // Initialize global instance
    databend_common_base::base::GlobalInstance::init_production();

    // Initialize all Databend services
    RUNTIME
        .block_on(async { GlobalServices::init_with(&conf, &BUILD_INFO, false).await })
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Service initialization failed: {}",
                e
            ))
        })?;

    // Python binding telemetry - mandatory reporting
    RUNTIME.block_on(async {
        report_python_binding_telemetry(&conf).await;
    });

    // Initialize license manager
    OssLicenseManager::init(conf.query.tenant_id.tenant_name().to_string()).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "License manager init failed: {}",
            e
        ))
    })?;

    EMBEDDED_INIT_STATE.store(true, Ordering::Release);
    Ok(())
}

fn create_embedded_config(
    data_path: &str,
) -> Result<InnerConfig, Box<dyn std::error::Error + Send + Sync>> {
    let mut conf = InnerConfig::default();

    // Query configuration
    conf.query.tenant_id = Tenant::new_literal("python_binding");
    conf.query.common.embedded_mode = true;
    conf.query.common.cluster_id = "".to_string();
    conf.query.common.warehouse_id = "".to_string();
    conf.query.node_id = "embedded_node".to_string();

    // Logging configuration
    let mut log_config = databend_common_tracing::Config::new_testing();
    log_config.stderr.on = false;
    log_config.file.level = "INFO".to_string();
    let log_dir = std::path::Path::new(data_path).join("logs");
    std::fs::create_dir_all(&log_dir)?;
    log_config.file.dir = log_dir.to_str().unwrap().to_string();
    conf.log = log_config;

    // Builtin users
    let users = vec![UserConfig {
        name: "root".to_string(),
        auth: UserAuthConfig {
            auth_type: "no_password".to_string(),
            auth_string: None,
        },
    }];
    conf.query.builtin = BuiltInConfig {
        users,
        udfs: vec![],
    };

    // Storage configuration
    let storage_path = std::path::Path::new(data_path).join("data");
    std::fs::create_dir_all(&storage_path)?;
    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        root: storage_path.to_str().unwrap().to_string(),
    });
    conf.storage.allow_insecure = true;

    // Meta configuration
    let meta_dir = std::path::Path::new(data_path).join("meta");
    std::fs::create_dir_all(&meta_dir)?;
    conf.meta.endpoints = vec![];
    conf.meta.embedded_dir = meta_dir.to_str().unwrap().to_string();

    // Memory and spill configuration
    let mut system = sysinfo::System::new();
    system.refresh_memory();
    let total_memory = system.total_memory();

    // Set max server memory usage to 80% of system memory
    conf.query.common.max_server_memory_usage = (total_memory as f64 * 0.8) as u64;
    conf.query.common.max_memory_limit_enabled = true;

    // Enable spill when memory usage exceeds 60% of system memory
    conf.spill.global_bytes_limit = (total_memory as f64 * 0.6) as u64;

    Ok(conf)
}

/// A Python module implemented in Rust.
#[pymodule(gil_used = false)]
pub fn databend(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // m.add_function(wrap_pyfunction!(init_service, m)?)?;
    m.add_function(wrap_pyfunction!(init_embedded, m)?)?;
    m.add_class::<context::PySessionContext>()?;
    Ok(())
}

async fn report_python_binding_telemetry(config: &InnerConfig) {
    use databend_common_telemetry::report_node_telemetry;
    use databend_common_version::DATABEND_TELEMETRY_API_KEY;
    use databend_common_version::DATABEND_TELEMETRY_ENDPOINT;

    let payload = create_python_binding_telemetry_payload(config);
    report_node_telemetry(
        payload,
        DATABEND_TELEMETRY_ENDPOINT,
        DATABEND_TELEMETRY_API_KEY,
    )
    .await;
}

fn create_python_binding_telemetry_payload(config: &InnerConfig) -> serde_json::Value {
    let start_time = std::time::Instant::now();

    // Collect system information
    let mut system = sysinfo::System::new();
    system.refresh_memory();
    system.refresh_cpu_all();

    serde_json::json!({
        "timestamp": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        "event_type": "python_binding_init",
        "mode": "python_binding",
        "cluster": {
            "cluster_id": "embedded_cluster",
            "tenant_name": config.query.tenant_id.tenant_name()
        },
        "node": {
            "node_id": config.query.node_id,
            "version": BUILD_INFO.commit_detail.as_str()
        },
        "system": {
            "os_name": sysinfo::System::name().unwrap_or_else(|| "unknown".to_string()),
            "os_version": sysinfo::System::os_version().unwrap_or_else(|| "unknown".to_string()),
            "kernel_version": sysinfo::System::kernel_version().unwrap_or_else(|| "unknown".to_string()),
            "arch": sysinfo::System::cpu_arch(),
            "cpu_cores": system.cpus().len(),
            "total_memory": system.total_memory(),
            "available_memory": system.available_memory(),
            "used_memory": system.used_memory(),
            "uptime_seconds": sysinfo::System::uptime()
        },
        "cache_strategy": {
            "enable_table_meta_cache": config.cache.enable_table_meta_cache,
            "table_meta_snapshot_count": config.cache.table_meta_snapshot_count,
            "table_meta_segment_bytes": config.cache.table_meta_segment_bytes,
            "block_meta_count": config.cache.block_meta_count,
            "data_cache_in_memory_bytes": config.cache.data_cache_in_memory_bytes,
            "disk_cache_max_bytes": config.cache.disk_cache_config.max_bytes
        },
        "memory_management": {
            "max_server_memory_usage": config.query.common.max_server_memory_usage,
            "max_memory_limit_enabled": config.query.common.max_memory_limit_enabled,
            "spill_enabled": config.spill.global_bytes_limit > 0,
            "spill_threshold_bytes": config.spill.global_bytes_limit,
            "spill_threshold_percent": 60.0,
            "memory_limit_percent": 80.0
        },
        "meta_config": {
            "meta_embedded": !config.meta.embedded_dir.is_empty(),
            "meta_client_timeout": config.meta.client_timeout_in_second,
            "endpoints": config.meta.endpoints,
        },
        "embedded_config": {
            "data_path_configured": true,
            "log_level": config.log.file.level,
            "log_to_file": config.log.file.on
        },
        "observability": {
            "collection_duration_ms": start_time.elapsed().as_millis() as u64,
            "report_timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            "source": "databend-python-binding",
            "schema_version": "1.1"
        }
    })
}
