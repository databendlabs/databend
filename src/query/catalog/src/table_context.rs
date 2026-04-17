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

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::SystemTime;

use databend_common_base::base::BuildInfoRef;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::base::WatchNotify;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_expression::FunctionContext;
use databend_common_io::prelude::InputFormatSettings;
use databend_common_io::prelude::OutputFormatSettings;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StorageMetrics;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::Object;
use databend_storages_common_session::SessionState;
use databend_storages_common_session::TxnManagerRef;

use crate::catalog::Catalog;
use crate::cluster_info::Cluster;
use crate::plan::DataSourcePlan;
use crate::plan::PartInfoPtr;
use crate::plan::Partitions;
use crate::query_kind::QueryKind;
use crate::session_type::SessionType;
use crate::statistics::data_cache_statistics::DataCacheMetrics;
use crate::table::Table;

mod broadcast;
mod copy;
mod cte;
mod merge_into;
mod mutation;
mod on_error;
mod partitions;
mod perf;
mod query_identity;
mod query_profile;
mod query_queue;
mod read_block_thresholds;
mod result_cache;
mod runtime_filter;
mod segment_locations;
mod spill;
mod stage;
mod stream;
mod table_access;
mod table_management;
mod variables;

pub use broadcast::TableContextBroadcast;
pub use copy::TableContextCopy;
pub use cte::TableContextCte;
pub use merge_into::TableContextMergeInto;
pub use mutation::TableContextMutationStatus;
pub use on_error::TableContextOnError;
pub use partitions::TableContextPartitionStats;
pub use perf::TableContextPerf;
pub use query_identity::TableContextQueryIdentity;
pub use query_profile::TableContextQueryProfile;
pub use query_queue::TableContextQueryQueue;
pub use read_block_thresholds::TableContextReadBlockThresholds;
pub use result_cache::TableContextResultCache;
pub use runtime_filter::TableContextRuntimeFilter;
pub use segment_locations::TableContextSegmentLocations;
pub use spill::TableContextSpillProgress;
pub use stage::TableContextStage;
pub use stream::TableContextStream;
pub use table_access::TableContextTableAccess;
pub use table_management::TableContextTableManagement;
pub use variables::TableContextVariables;

pub struct ContextError;

#[derive(Debug)]
pub struct ProcessInfo {
    pub id: String,
    pub typ: String,
    pub state: ProcessInfoState,
    pub database: String,
    pub user: Option<UserInfo>,
    pub settings: Arc<Settings>,
    pub client_address: Option<String>,
    pub session_extra_info: Option<String>,
    pub memory_usage: i64,
    /// storage metrics for persisted data reading.
    pub data_metrics: Option<StorageMetrics>,
    pub scan_progress_value: Option<ProgressValues>,
    pub write_progress_value: Option<ProgressValues>,
    pub spill_progress_value: Option<ProgressValues>,
    pub mysql_connection_id: Option<u32>,
    pub created_time: SystemTime,
    pub status_info: Option<String>,
    pub current_query_id: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ProcessInfoState {
    Query,
    Aborting,
    Idle,
}

impl Display for ProcessInfoState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ProcessInfoState::Query => write!(f, "Query"),
            ProcessInfoState::Aborting => write!(f, "Aborting"),
            ProcessInfoState::Idle => write!(f, "Idle"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StageAttachment {
    pub location: String,
    pub file_format_options: Option<BTreeMap<String, String>>,
    pub copy_options: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Default)]
pub struct FilteredCopyFiles {
    pub files_to_copy: Vec<StageFileInfo>,
    pub duplicated_files: Vec<String>,
}

#[async_trait::async_trait]
pub trait TableContext:
    TableContextBroadcast
    + TableContextCopy
    + TableContextCte
    + TableContextMergeInto
    + TableContextMutationStatus
    + TableContextOnError
    + TableContextPartitionStats
    + TableContextPerf
    + TableContextQueryIdentity
    + TableContextQueryProfile
    + TableContextQueryQueue
    + TableContextReadBlockThresholds
    + TableContextResultCache
    + TableContextRuntimeFilter
    + TableContextSegmentLocations
    + TableContextSpillProgress
    + TableContextStage
    + TableContextTableAccess
    + TableContextTableManagement
    + TableContextStream
    + TableContextVariables
    + Send
    + Sync
{
    fn as_any(&self) -> &dyn Any;
    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    fn build_table_from_source_plan(&self, plan: &DataSourcePlan) -> Result<Arc<dyn Table>>;

    fn incr_total_scan_value(&self, value: ProgressValues);
    fn get_total_scan_value(&self) -> ProgressValues;

    fn get_scan_progress(&self) -> Arc<Progress>;
    fn get_scan_progress_value(&self) -> ProgressValues;
    fn get_write_progress(&self) -> Arc<Progress>;
    fn get_write_progress_value(&self) -> ProgressValues;
    fn get_result_progress(&self) -> Arc<Progress>;
    fn get_result_progress_value(&self) -> ProgressValues;
    fn get_status_info(&self) -> String;
    fn set_status_info(&self, info: &str);
    fn get_data_cache_metrics(&self) -> &DataCacheMetrics;
    fn get_partition(&self) -> Option<PartInfoPtr>;
    fn get_partitions(&self, num: usize) -> Vec<PartInfoPtr>;
    fn partition_num(&self) -> usize {
        unimplemented!()
    }
    fn set_partitions(&self, partitions: Partitions) -> Result<()>;
    fn get_can_scan_from_agg_index(&self) -> bool;
    fn set_can_scan_from_agg_index(&self, enable: bool);
    fn get_enable_sort_spill(&self) -> bool;
    fn set_enable_sort_spill(&self, enable: bool);
    fn set_compaction_num_block_hint(&self, _table_name: &str, _hint: u64) {
        unimplemented!()
    }
    fn get_compaction_num_block_hint(&self, _table_name: &str) -> u64 {
        unimplemented!()
    }
    fn get_enable_auto_analyze(&self) -> bool {
        unimplemented!()
    }
    fn set_enable_auto_analyze(&self, _enable: bool) {
        unimplemented!()
    }

    fn get_fragment_id(&self) -> usize;
    async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>>;
    fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>>;
    fn get_id(&self) -> String;
    fn get_current_catalog(&self) -> String;
    fn check_aborting(&self) -> Result<(), ContextError>;
    fn get_abort_notify(&self) -> Arc<WatchNotify>;
    fn get_abort_checker(self: Arc<Self>) -> AbortChecker
    where Self: 'static {
        struct Checker<S> {
            this: S,
        }
        impl<S: TableContext + ?Sized> CheckAbort for Checker<Arc<S>> {
            fn try_check_aborting(&self) -> Result<()> {
                self.this.check_aborting().with_context(|| "query aborted")
            }
        }
        Arc::new(Checker { this: self })
    }
    fn get_error(&self) -> Option<ErrorCode<ContextError>>;
    fn push_warning(&self, warning: String);
    fn get_current_database(&self) -> String;
    fn get_current_user(&self) -> Result<UserInfo>;
    fn get_current_role(&self) -> Option<RoleInfo>;
    fn get_secondary_roles(&self) -> Option<Vec<String>>;
    fn get_current_session_id(&self) -> String {
        unimplemented!()
    }
    fn get_current_client_session_id(&self) -> Option<String> {
        unimplemented!()
    }
    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>>;

    async fn validate_privilege(
        &self,
        object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
    ) -> Result<()>;
    async fn get_all_available_roles(&self) -> Result<Vec<RoleInfo>>;
    async fn get_visibility_checker(
        &self,
        ignore_ownership: bool,
        object: Object,
    ) -> Result<GrantObjectVisibilityChecker>;
    fn get_fuse_version(&self) -> String;
    fn get_version(&self) -> BuildInfoRef;
    fn get_input_format_settings(&self) -> Result<InputFormatSettings>;
    fn get_output_format_settings(&self) -> Result<OutputFormatSettings>;
    fn get_tenant(&self) -> Tenant;
    /// Get the kind of session running query.
    fn get_query_kind(&self) -> QueryKind;
    fn get_function_context(&self) -> Result<FunctionContext>;
    fn get_connection_id(&self) -> String;
    fn get_settings(&self) -> Arc<Settings>;
    fn get_session_settings(&self) -> Arc<Settings>;
    fn get_cluster(&self) -> Arc<Cluster>;
    fn set_cluster(&self, cluster: Arc<Cluster>);
    async fn get_warehouse_cluster(&self) -> Result<Arc<Cluster>>;
    fn get_processes_info(&self) -> Vec<ProcessInfo>;
    fn get_queued_queries(&self) -> Vec<ProcessInfo>;

    async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        self.get_table_with_branch(catalog, database, table, None)
            .await
    }

    async fn get_zero_table(&self) -> Result<Arc<dyn Table>> {
        let catalog = self.get_catalog("default").await?;
        catalog
            .get_table(&self.get_tenant(), "system", "zero")
            .await
    }

    /// Get license key from context, return empty if license is not found or error happened.
    fn get_license_key(&self) -> String;

    fn txn_mgr(&self) -> TxnManagerRef;

    fn session_state(&self) -> Result<SessionState>;

    fn get_shared_settings(&self) -> Arc<Settings>;

    fn get_session_type(&self) -> SessionType {
        unimplemented!()
    }
}

pub type AbortChecker = Arc<dyn CheckAbort + Send + Sync>;

pub trait CheckAbort {
    fn try_check_aborting(&self) -> Result<()>;
}
