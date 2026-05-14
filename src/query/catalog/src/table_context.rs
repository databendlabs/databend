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

use databend_common_base::base::ProgressValues;
pub use databend_common_component::BroadcastRegistry;
pub use databend_common_component::CopyState;
pub use databend_common_component::FragmentId;
pub use databend_common_component::MutationState;
pub use databend_common_component::ReadBlockThresholdsState;
pub use databend_common_component::ResultCacheState;
pub use databend_common_component::SegmentLocationsState;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserInfo;
use databend_common_settings::Settings;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StorageMetrics;

mod authorization;
mod execution;
mod materialization;
mod partitions;
mod query;
mod query_state;
mod runtime_filter;
mod session;
mod table;
mod table_access;
mod table_management;

pub use authorization::TableContextAuthorization;
pub use databend_common_component::BroadcastChannel;
pub use execution::TableContextPerf;
pub use execution::TableContextProgress;
pub use execution::TableContextSpillProgress;
pub use execution::TableContextTelemetry;
pub use materialization::TableContextCte;
pub use materialization::TableContextStream;
pub use partitions::TableContextPartitionStats;
pub use query::TableContextLicense;
pub use query::TableContextMergeInto;
pub use query::TableContextQueryIdentity;
pub use query::TableContextQueryInfo;
pub use query::TableContextQueryProfile;
pub use query::TableContextResultCache;
pub use query::TableContextVariables;
pub use query_state::TableContextQueryState;
pub use runtime_filter::TableContextRuntimeFilter;
pub use session::TableContextCluster;
pub use session::TableContextSession;
pub use session::TableContextSettings;
pub use table::TableContextCopy;
pub use table::TableContextStage;
pub use table::TableContextTableFactory;
pub use table_access::TableContextTableAccess;
pub use table_management::TableContextTableManagement;

pub mod prelude {
    pub use super::BroadcastRegistry;
    pub use super::CopyState;
    pub use super::FragmentId;
    pub use super::MutationState;
    pub use super::ReadBlockThresholdsState;
    pub use super::ResultCacheState;
    pub use super::SegmentLocationsState;
    pub use super::TableContext;
    pub use super::TableContextAuthorization;
    pub use super::TableContextCluster;
    pub use super::TableContextCopy;
    pub use super::TableContextCte;
    pub use super::TableContextLicense;
    pub use super::TableContextMergeInto;
    pub use super::TableContextPartitionStats;
    pub use super::TableContextPerf;
    pub use super::TableContextProgress;
    pub use super::TableContextQueryIdentity;
    pub use super::TableContextQueryInfo;
    pub use super::TableContextQueryProfile;
    pub use super::TableContextQueryState;
    pub use super::TableContextResultCache;
    pub use super::TableContextRuntimeFilter;
    pub use super::TableContextSession;
    pub use super::TableContextSettings;
    pub use super::TableContextSpillProgress;
    pub use super::TableContextStage;
    pub use super::TableContextStream;
    pub use super::TableContextTableAccess;
    pub use super::TableContextTableFactory;
    pub use super::TableContextTableManagement;
    pub use super::TableContextTelemetry;
    pub use super::TableContextVariables;
}

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
    TableContextAuthorization
    + TableContextCluster
    + TableContextCopy
    + TableContextCte
    + TableContextLicense
    + TableContextMergeInto
    + TableContextPartitionStats
    + TableContextPerf
    + TableContextProgress
    + TableContextQueryIdentity
    + TableContextQueryInfo
    + TableContextQueryProfile
    + TableContextQueryState
    + TableContextResultCache
    + TableContextRuntimeFilter
    + TableContextSession
    + TableContextSettings
    + TableContextSpillProgress
    + TableContextStage
    + TableContextTableAccess
    + TableContextTableFactory
    + TableContextTableManagement
    + TableContextTelemetry
    + TableContextStream
    + TableContextVariables
    + Send
    + Sync
{
    fn broadcast_registry(&self) -> &BroadcastRegistry;

    fn copy_state(&self) -> &CopyState;

    fn fragment_id(&self) -> &FragmentId;

    fn mutation_state(&self) -> &MutationState;

    fn read_block_thresholds(&self) -> &ReadBlockThresholdsState;

    fn result_cache_state(&self) -> &ResultCacheState;

    fn written_segment_locations(&self) -> &SegmentLocationsState;

    fn selected_segment_locations(&self) -> &SegmentLocationsState;

    fn as_any(&self) -> &dyn Any;
}

pub type AbortChecker = Arc<dyn CheckAbort + Send + Sync>;

pub trait CheckAbort {
    fn try_check_aborting(&self) -> Result<()>;
}
