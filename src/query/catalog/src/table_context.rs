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
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserInfo;
use databend_common_settings::Settings;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StorageMetrics;

mod authorization;
mod broadcast;
mod cluster;
mod copy;
mod cte;
mod fragment;
mod license;
mod merge_into;
mod mutation;
mod partitions;
mod perf;
mod progress;
mod query_identity;
mod query_info;
mod query_profile;
mod query_state;
mod read_block_thresholds;
mod result_cache;
mod runtime_filter;
mod segment_locations;
mod session;
mod settings;
mod spill;
mod stage;
mod stream;
mod table_access;
mod table_factory;
mod table_management;
mod telemetry;
mod variables;

pub use authorization::TableContextAuthorization;
pub use broadcast::TableContextBroadcast;
pub use cluster::TableContextCluster;
pub use copy::TableContextCopy;
pub use cte::TableContextCte;
pub use fragment::TableContextFragment;
pub use license::TableContextLicense;
pub use merge_into::TableContextMergeInto;
pub use mutation::TableContextMutationStatus;
pub use partitions::TableContextPartitionStats;
pub use perf::TableContextPerf;
pub use progress::TableContextProgress;
pub use query_identity::TableContextQueryIdentity;
pub use query_info::TableContextQueryInfo;
pub use query_profile::TableContextQueryProfile;
pub use query_state::TableContextQueryState;
pub use read_block_thresholds::TableContextReadBlockThresholds;
pub use result_cache::TableContextResultCache;
pub use runtime_filter::TableContextRuntimeFilter;
pub use segment_locations::TableContextSegmentLocations;
pub use session::TableContextSession;
pub use settings::TableContextSettings;
pub use spill::TableContextSpillProgress;
pub use stage::TableContextStage;
pub use stream::TableContextStream;
pub use table_access::TableContextTableAccess;
pub use table_factory::TableContextTableFactory;
pub use table_management::TableContextTableManagement;
pub use telemetry::TableContextTelemetry;
pub use variables::TableContextVariables;

pub mod prelude {
    pub use super::TableContext;
    pub use super::TableContextAuthorization;
    pub use super::TableContextBroadcast;
    pub use super::TableContextCluster;
    pub use super::TableContextCopy;
    pub use super::TableContextCte;
    pub use super::TableContextFragment;
    pub use super::TableContextLicense;
    pub use super::TableContextMergeInto;
    pub use super::TableContextMutationStatus;
    pub use super::TableContextPartitionStats;
    pub use super::TableContextPerf;
    pub use super::TableContextProgress;
    pub use super::TableContextQueryIdentity;
    pub use super::TableContextQueryInfo;
    pub use super::TableContextQueryProfile;
    pub use super::TableContextQueryState;
    pub use super::TableContextReadBlockThresholds;
    pub use super::TableContextResultCache;
    pub use super::TableContextRuntimeFilter;
    pub use super::TableContextSegmentLocations;
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
    + TableContextBroadcast
    + TableContextCluster
    + TableContextCopy
    + TableContextCte
    + TableContextFragment
    + TableContextLicense
    + TableContextMergeInto
    + TableContextMutationStatus
    + TableContextPartitionStats
    + TableContextPerf
    + TableContextProgress
    + TableContextQueryIdentity
    + TableContextQueryInfo
    + TableContextQueryProfile
    + TableContextQueryState
    + TableContextReadBlockThresholds
    + TableContextResultCache
    + TableContextRuntimeFilter
    + TableContextSegmentLocations
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
    fn as_any(&self) -> &dyn Any;
}

pub type AbortChecker = Arc<dyn CheckAbort + Send + Sync>;

pub trait CheckAbort {
    fn try_check_aborting(&self) -> Result<()>;
}
