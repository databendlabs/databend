//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::net::SocketAddr;
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_base::base::Runtime;
use common_config::Config;
use common_contexts::DalContext;
use common_contexts::DalMetrics;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_fuse_meta::caches::CacheManager;
use common_io::prelude::FormatSettings;
use common_meta_types::UserInfo;
use common_planners::Partitions;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_settings::Settings;
use common_users::RoleCacheMgr;
use common_users::UserApiProvider;
use opendal::Operator;
use parking_lot::Mutex;

use crate::catalog::Catalog;
use crate::catalog::CatalogManager;
use crate::cluster_info::Cluster;
use crate::table::Table;

pub struct ProcessInfo {
    pub id: String,
    pub typ: String,
    pub state: String,
    pub database: String,
    pub user: Option<UserInfo>,
    pub settings: Arc<Settings>,
    pub client_address: Option<SocketAddr>,
    pub session_extra_info: Option<String>,
    pub memory_usage: i64,
    pub dal_metrics: Option<DalMetrics>,
    pub scan_progress_value: Option<ProgressValues>,
    pub mysql_connection_id: Option<u32>,
}

#[async_trait::async_trait]
pub trait TableContext: Send + Sync {
    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    fn build_table_from_source_plan(&self, plan: &ReadDataSourcePlan) -> Result<Arc<dyn Table>>;
    fn get_scan_progress(&self) -> Arc<Progress>;
    fn get_scan_progress_value(&self) -> ProgressValues;
    fn get_write_progress(&self) -> Arc<Progress>;
    fn get_write_progress_value(&self) -> ProgressValues;
    fn get_result_progress(&self) -> Arc<Progress>;
    fn get_result_progress_value(&self) -> ProgressValues;
    fn get_error(&self) -> Arc<Mutex<Option<ErrorCode>>>;
    fn get_error_value(&self) -> Option<ErrorCode>;
    fn set_error(&self, err: ErrorCode);
    // Steal n partitions from the partition pool by the pipeline worker.
    // This also can steal the partitions from distributed node.
    fn try_get_partitions(&self, num: u64) -> Result<Partitions>;
    // Update the context partition pool from the pipeline builder.
    fn try_set_partitions(&self, partitions: Partitions) -> Result<()>;
    fn try_get_statistics(&self) -> Result<Statistics>;
    fn try_set_statistics(&self, val: &Statistics) -> Result<()>;
    fn attach_query_str(&self, query: &str);
    fn attach_query_plan(&self, query_plan: &PlanNode);
    fn get_fragment_id(&self) -> usize;
    fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>>;
    fn get_id(&self) -> String;
    fn get_current_catalog(&self) -> String;
    fn get_current_database(&self) -> String;
    fn get_config(&self) -> Config;
    fn get_current_user(&self) -> Result<UserInfo>;
    fn set_current_user(&self, user: UserInfo);
    fn get_fuse_version(&self) -> String;
    fn get_changed_settings(&self) -> Arc<Settings>;
    fn apply_changed_settings(&self, changed_settings: Arc<Settings>) -> Result<()>;
    fn get_format_settings(&self) -> Result<FormatSettings>;
    fn get_tenant(&self) -> String;
    fn set_current_tenant(&self, tenant: String);
    fn get_subquery_name(&self, _query: &PlanNode) -> String;
    /// Get the data accessor metrics.
    fn get_dal_metrics(&self) -> DalMetrics;
    /// Get the session running query.
    fn get_query_str(&self) -> String;
    // Get the storage data accessor operator from the session manager.
    fn get_storage_operator(&self) -> Result<Operator>;
    fn get_dal_context(&self) -> &DalContext;
    fn get_storage_runtime(&self) -> Arc<Runtime>;
    fn push_precommit_block(&self, block: DataBlock);
    fn consume_precommit_blocks(&self) -> Vec<DataBlock>;
    fn try_get_function_context(&self) -> Result<FunctionContext>;
    fn get_connection_id(&self) -> String;
    fn get_settings(&self) -> Arc<Settings>;
    // Get user manager api.
    fn get_user_manager(&self) -> Arc<UserApiProvider>;
    fn get_cluster(&self) -> Arc<Cluster>;
    async fn get_processes_info(&self) -> Vec<ProcessInfo>;
}
