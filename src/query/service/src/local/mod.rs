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

mod executor;
pub(crate) mod helper;

use std::env;
use std::time::Instant;

use common_config::Config;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::OssLicenseManager;
use common_meta_app::storage::StorageFsConfig;
use common_meta_app::storage::StorageParams;
use common_meta_embedded::MetaEmbedded;
use tokio_stream::StreamExt;

use crate::interpreters::InterpreterFactory;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::sql::Planner;
use crate::GlobalServices;

pub async fn query_local() -> Result<()> {
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

    GlobalServices::init(conf).await?;
    // init oss license manager
    OssLicenseManager::init().unwrap();

    let now = Instant::now();

    let mut executor = executor::SessionExecutor::try_new(true).await?;
    executor.handle_repl().await;
    Ok(())
}
