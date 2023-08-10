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

use std::env;

use common_config::Config;
use common_config::InnerConfig;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::OssLicenseManager;
use common_meta_app::storage::StorageFsConfig;
use common_meta_app::storage::StorageParams;
use common_meta_embedded::MetaEmbedded;
use databend_query::GlobalServices;
use pyo3::prelude::*;
use utils::RUNTIME;

/// A Python module implemented in Rust.
#[pymodule]
fn databend(_py: Python, m: &PyModule) -> PyResult<()> {
    env::set_var("META_EMBEDDED_DIR", ".databend/_meta");
    let mut conf: InnerConfig = Config::load(false).unwrap().try_into().unwrap();
    conf.storage.allow_insecure = true;
    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        root: ".databend/_data".to_string(),
    });

    RUNTIME.block_on(async {
        MetaEmbedded::init_global_meta_store(".databend/_meta".to_string())
            .await
            .unwrap();
        GlobalServices::init(conf).await.unwrap();
    });

    // init oss license manager
    OssLicenseManager::init().unwrap();

    m.add_class::<context::PySessionContext>()?;
    Ok(())
}
