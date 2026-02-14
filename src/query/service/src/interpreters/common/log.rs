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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use databend_common_base::runtime::profile::ProfileDesc;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_pipeline::core::PlanProfile;
use log::error;
use log::info;

use crate::interpreters::InterpreterMetrics;
use crate::interpreters::InterpreterQueryLog;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;

pub fn log_query_start(ctx: &QueryContext) {
    // Nested execution paths may invoke this multiple times for one query context.
    if !ctx.on_query_execution_start() {
        return;
    }

    InterpreterMetrics::record_query_start(ctx);
    let now = SystemTime::now();
    let session = ctx.get_current_session();
    let typ = session.get_type();
    if typ.is_user_session() {
        SessionManager::instance().status.write().query_start(now);
    }

    if let Err(error) = InterpreterQueryLog::log_start(ctx, now, None) {
        error!("Failed to log query start: {:?}", error)
    }
}

pub fn log_query_finished(ctx: &QueryContext, error: Option<ErrorCode>) {
    // Emit finish immediately on error, otherwise only on terminal leave.
    if !ctx.on_query_execution_finish(error.is_some()) {
        return;
    }

    // metrics
    InterpreterMetrics::record_query_finished(ctx, error.clone());

    let now = SystemTime::now();
    let session = ctx.get_current_session();

    session.get_status().write().query_finish();
    let typ = session.get_type();
    if typ.is_user_session() {
        SessionManager::instance().status.write().query_finish(now);
        SessionManager::instance()
            .metrics_collector
            .track_finished_query(
                ctx.get_scan_progress_value(),
                ctx.get_write_progress_value(),
                ctx.get_join_spill_progress_value(),
                ctx.get_aggregate_spill_progress_value(),
                ctx.get_group_by_spill_progress_value(),
                ctx.get_window_partition_spill_progress_value(),
            );
    }

    info!(memory:? = ctx.get_node_peek_memory_usage(); "total memory usage");

    // databend::log::profile
    let query_profiles = ctx.get_query_profiles();
    let has_profiles = !query_profiles.is_empty();

    if has_profiles {
        #[derive(serde::Serialize)]
        struct QueryProfiles {
            query_id: String,
            profiles: Vec<PlanProfile>,
            statistics_desc: Arc<BTreeMap<ProfileStatisticsName, ProfileDesc>>,
        }

        match serde_json::to_string(&QueryProfiles {
            query_id: ctx.get_id(),
            profiles: query_profiles.clone(),
            statistics_desc: get_statistics_desc(),
        }) {
            Ok(profile_json) => {
                info!(target: "databend::log::profile", "{}", profile_json);
            }
            Err(err) => {
                error!("Failed to serialize query profiles: {:?}", err);
            }
        }
    }

    // databend::log::query
    if let Err(error) = InterpreterQueryLog::log_finish(ctx, now, error, has_profiles) {
        error!("Failed to log query finish: {:?}", error)
    }
}
