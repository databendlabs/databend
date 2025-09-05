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

use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::sync::Once;

use databend_common_config::Config;
use databend_common_config::InnerConfig;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;
use databend_common_version::BUILD_INFO;
use databend_query::clusters::ClusterDiscovery;
use databend_query::GlobalServices;
use pyo3::prelude::*;
use utils::RUNTIME;

static INIT: Once = Once::new();
static INITIALIZED_MUTEX: Mutex<()> = Mutex::new(());

pub fn ensure_service_initialized(config: &str, local_dir: &str) -> PyResult<()> {
    let _guard = INITIALIZED_MUTEX.lock().unwrap_or_else(|poisoned| {
        // Recover from poisoned mutex by clearing the poison
        poisoned.into_inner()
    });
    if INIT.is_completed() {
        return Ok(());
    }
    init_service_internal(config, local_dir)
}

/// A Python module implemented in Rust.
#[pymodule(gil_used = false)]
pub fn databend(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_service, m)?)?;
    m.add_class::<context::PySessionContext>()?;
    Ok(())
}
#[pyfunction]
#[pyo3(signature = (config = "", local_dir = ""))]
fn init_service(_py: Python, config: &str, local_dir: &str) -> PyResult<()> {
    let _guard = INITIALIZED_MUTEX.lock().unwrap_or_else(|poisoned| {
        poisoned.into_inner()
    });
    if INIT.is_completed() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Service already initialized",
        ));
    }
    init_service_internal(config, local_dir)
}

fn init_service_internal(config: &str, local_dir: &str) -> PyResult<()> {

    // if config is file read it to config_str
    let conf = if std::fs::exists(Path::new(config)).unwrap_or_default() {
        Config::load_with_config_file(config).unwrap()
    } else {
        let temp_dr = tempfile::tempdir().unwrap();
        let mut file = std::fs::File::create(temp_dr.path().join("config.toml")).unwrap();

        let config = if local_dir.is_empty() {
            use std::net::{TcpListener, SocketAddr};
            
            // Find available ports
            let find_free_port = || -> u16 {
                TcpListener::bind("127.0.0.1:0")
                    .and_then(|listener| listener.local_addr())
                    .map(|addr| addr.port())
                    .unwrap_or(0)
            };
            
            let meta_port = find_free_port();
            let flight_port = find_free_port();
            
            format!(
                r#"[meta]
embedded_dir = "{local_dir}"
endpoints = ["127.0.0.1:{}"]

[query]
flight_api_address = "127.0.0.1:{}"
admin_api_address = "127.0.0.1:0"
metric_api_address = "127.0.0.1:0"
mysql_handler_host = "127.0.0.1"
mysql_handler_port = 0
clickhouse_http_handler_host = "127.0.0.1"
clickhouse_http_handler_port = 0
clickhouse_handler_host = "127.0.0.1"
clickhouse_handler_port = 0
http_handler_host = "127.0.0.1"
http_handler_port = 0

[storage]
type = "fs"
allow_insecure = true

[storage.fs]
data_path = "{local_dir}"
"#, meta_port, flight_port
            )
        } else {
            config.to_string()
        };

        file.write_all(config.as_bytes()).unwrap();
        let p = format!("{}", temp_dr.path().join("config.toml").as_path().display());
        Config::load_with_config_file(&p).unwrap()
    };

    let mut conf: InnerConfig = conf.try_into().unwrap();
    conf.query.cluster_id = "bendpy".to_string();

    INIT.call_once(|| {
        RUNTIME
            .block_on(async {
                GlobalServices::init(&conf, &BUILD_INFO, false).await?;
                // init oss license manager
                OssLicenseManager::init("".to_string()).unwrap();
                // Cluster register.
                ClusterDiscovery::instance()
                    .register_to_metastore(&conf)
                    .await
            })
            .unwrap();
    });

    Ok(())
}
