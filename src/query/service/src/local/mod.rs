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

mod config;
mod display;
mod executor;
pub(crate) mod helper;

use std::env;
use std::io::stdin;
use std::io::IsTerminal;

use common_config::Config;
use common_config::InnerConfig;
use common_exception::Result;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::OssLicenseManager;
use common_meta_app::storage::StorageFsConfig;
use common_meta_app::storage::StorageParams;
use common_meta_embedded::MetaEmbedded;

use crate::clusters::ClusterDiscovery;
use crate::GlobalServices;

pub async fn query_local(query_sql: &String, output_format: &String) -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    env::set_var("META_EMBEDDED_DIR", temp_dir.path().join("_meta"));
    let mut conf: InnerConfig = Config::load(true).unwrap().try_into().unwrap();
    conf.storage.allow_insecure = true;
    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        root: temp_dir.path().join("_data").to_str().unwrap().to_owned(),
    });

    let meta_dir = temp_dir.path().join("_meta");
    MetaEmbedded::init_global_meta_store(meta_dir.to_string_lossy().to_string())
        .await
        .unwrap();

    GlobalServices::init(conf.clone()).await?;
    // init oss license manager
    OssLicenseManager::init().unwrap();

    // Cluster register.
    ClusterDiscovery::instance()
        .register_to_metastore(&conf)
        .await?;

    let is_terminal = stdin().is_terminal();
    let is_repl = is_terminal && query_sql.is_empty();
    let mut executor =
        executor::SessionExecutor::try_new(is_repl, query_sql, output_format).await?;
    executor.handle().await;
    Ok(())
}
