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

use std::sync::Arc;
use std::time::Duration;

use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;

#[derive(Clone)]
pub struct ExecutorSettings {
    pub query_id: Arc<String>,
    pub max_threads: u64,
    pub enable_queries_executor: bool,
    pub max_execute_time_in_seconds: Duration,
    pub executor_node_id: String,
}

impl ExecutorSettings {
    pub fn try_create(ctx: Arc<dyn TableContext>) -> Result<ExecutorSettings> {
        let query_id = ctx.get_id();
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()?;
        let max_execute_time_in_seconds = settings.get_max_execute_time_in_seconds()?;

        let config_enable_queries_executor = GlobalConfig::instance().query.enable_queries_executor;
        let setting_use_legacy_query_executor = settings.get_use_legacy_query_executor()?;
        // If `use_legacy_query_executor` is set to 1, we disable the queries executor
        // Otherwise, we all follow configuration
        let enable_queries_executor = if setting_use_legacy_query_executor {
            false
        } else {
            config_enable_queries_executor
        };

        Ok(ExecutorSettings {
            enable_queries_executor,
            query_id: Arc::new(query_id),
            max_execute_time_in_seconds: Duration::from_secs(max_execute_time_in_seconds),
            max_threads,
            executor_node_id: ctx.get_cluster().local_id.clone(),
        })
    }
}
