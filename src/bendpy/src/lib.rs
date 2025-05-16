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
use databend_query::clusters::ClusterDiscovery;
use databend_query::GlobalServices;
use pyo3::prelude::*;
use utils::RUNTIME;

static INIT: Once = Once::new();
static INITIALIZED_MUTEX: Mutex<()> = Mutex::new(());

/// A Python module implemented in Rust.
#[pymodule(gil_used = false)]
pub fn _databend(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_service, m)?)?;
    m.add_class::<context::PySessionContext>()?;
    Ok(())
}
#[pyfunction]
#[pyo3(signature = (config = "", local_dir = ""))]
fn init_service(_py: Python, config: &str, local_dir: &str) -> PyResult<()> {
    let _guard = INITIALIZED_MUTEX.lock().unwrap();
    if INIT.is_completed() {
        return Err(pyo3::exceptions::PyRuntimeError::new_err(
            "Service already initialized",
        ));
    }

    // if config is file read it to config_str
    let conf = if std::fs::exists(Path::new(config)).unwrap() {
        Config::load_with_config_file(config).unwrap()
    } else {
        let temp_dr = tempfile::tempdir().unwrap();
        let mut file = std::fs::File::create(temp_dr.path().join("config.toml")).unwrap();

        let config = if local_dir.is_empty() {
            format!(
                r#"[meta]
embedded_dir = "{local_dir}"
[storage]
type = "fs"
allow_insecure = true
[storage.fs]
data_path = "{local_dir}"#
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
                GlobalServices::init(&conf, false).await?;
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
