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

use databend_common_config::InnerConfig;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;
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
    let conf = create_local_meta_config(data_path).map_err(|e| {
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

    // Keep global config unchanged for embedded mode

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

fn create_local_meta_config(
    data_path: &str,
) -> Result<InnerConfig, Box<dyn std::error::Error + Send + Sync>> {
    use databend_common_config::BuiltInConfig;
    use databend_common_config::UserAuthConfig;
    use databend_common_config::UserConfig;
    use databend_common_meta_app::storage::StorageFsConfig;
    use databend_common_meta_app::storage::StorageParams;

    let mut conf = InnerConfig::default();
    conf.query.tenant_id = Tenant::new_literal("python_binding");
    conf.log = databend_common_tracing::Config::new_testing();
    conf.query.cluster_id = "".to_string(); // Empty cluster_id for embedded mode
    conf.query.warehouse_id = "".to_string(); // Empty warehouse_id for embedded mode
    conf.query.node_id = "embedded_node".to_string();

    // Add builtin users
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
    let storage_path = std::path::Path::new(data_path)
        .join("data")
        .to_str()
        .unwrap()
        .to_string();

    std::fs::create_dir_all(&storage_path)?;

    conf.storage.params = StorageParams::Fs(StorageFsConfig { root: storage_path });
    conf.storage.allow_insecure = true;

    // Local meta mode: endpoints must be empty for local mode
    conf.meta.endpoints = vec![];
    // embedded_dir can be set to a path for persistent storage, or empty for temp storage
    conf.meta.embedded_dir = std::path::Path::new(data_path)
        .join("meta")
        .to_str()
        .unwrap()
        .to_string();

    std::fs::create_dir_all(&conf.meta.embedded_dir)?;

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
