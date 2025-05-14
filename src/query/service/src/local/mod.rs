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

use std::io::stdin;
use std::io::IsTerminal;

use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;

use crate::clusters::ClusterDiscovery;
use crate::GlobalServices;

pub async fn query_local(
    mut conf: InnerConfig,
    query_sql: &str,
    output_format: &str,
) -> Result<()> {
    conf.storage.allow_insecure = true;
    if conf.query.cluster_id.is_empty() {
        conf.query.cluster_id = "local_test".to_string();
    }

    GlobalServices::init(&conf, false).await?;
    // init oss license manager
    OssLicenseManager::init(conf.query.tenant_id.tenant_name().to_string()).unwrap();
    // Cluster register.
    ClusterDiscovery::instance()
        .register_to_metastore(&conf)
        .await?;

    let is_terminal = stdin().is_terminal();
    let is_repl = is_terminal && query_sql.is_empty();
    let mut executor = executor::SessionExecutor::try_new(is_repl, output_format).await?;

    let query_sql = query_sql.replace("$STDIN", "'fs:///dev/fd/0'");
    executor.handle(&query_sql).await;
    Ok(())
}
