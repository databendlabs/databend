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

use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::profile::ProfileDesc;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_pipeline::core::PlanProfile;
use databend_common_tracing::HistoryConfig;
use log::error;
use log::info;

use crate::interpreters::InterpreterMetrics;
use crate::interpreters::InterpreterQueryLog;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;
use crate::sessions::TableContextProgress;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextQueryProfile;
use crate::sessions::TableContextSpillProgress;

const QUERY_HISTORY_TABLE: &str = "query_history";
const PROFILE_HISTORY_TABLE: &str = "profile_history";
const ACCESS_HISTORY_TABLE: &str = "access_history";

fn history_table_log_enabled(history: &HistoryConfig, history_table_name: &str) -> bool {
    history.on
        && history
            .tables
            .iter()
            .any(|table| table.table_name.eq_ignore_ascii_case(history_table_name))
}

fn captured_info_log_enabled() -> bool {
    ThreadTracker::capture_log_settings().is_some_and(|settings| {
        settings.queue.is_some() && settings.level >= log::LevelFilter::Info
    })
}

pub(crate) fn query_log_enabled() -> bool {
    let log = &GlobalConfig::instance().log;
    log.query.on
        || history_table_log_enabled(&log.history, QUERY_HISTORY_TABLE)
        || captured_info_log_enabled()
}

pub(crate) fn profile_log_enabled() -> bool {
    let log = &GlobalConfig::instance().log;
    log.profile.on
        || history_table_log_enabled(&log.history, PROFILE_HISTORY_TABLE)
        || captured_info_log_enabled()
}

pub(crate) fn access_log_enabled() -> bool {
    let log = &GlobalConfig::instance().log;
    history_table_log_enabled(&log.history, ACCESS_HISTORY_TABLE) || captured_info_log_enabled()
}

pub fn log_query_start(ctx: &QueryContext) {
    InterpreterMetrics::record_query_start(ctx);
    let now = SystemTime::now();
    let session = ctx.get_current_session();
    let typ = session.get_type();
    if typ.is_user_session() {
        SessionManager::instance().status.write().query_start(now);
    }

    if query_log_enabled()
        && let Err(error) = InterpreterQueryLog::log_start(ctx, now, None)
    {
        error!("Failed to log query start: {:?}", error)
    }
}

pub fn log_query_finished(ctx: &QueryContext, error: Option<ErrorCode>) {
    // metrics
    InterpreterMetrics::record_query_finished(ctx, error.clone());

    let now = SystemTime::now();
    ctx.set_finish_time(now);
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

    let query_log_is_enabled = query_log_enabled();
    let profile_log_is_enabled = profile_log_enabled();
    let query_profiles = if query_log_is_enabled || profile_log_is_enabled {
        ctx.get_query_profiles()
    } else {
        Vec::new()
    };
    let has_profiles = !query_profiles.is_empty();

    if has_profiles && profile_log_is_enabled {
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

    if query_log_is_enabled
        && let Err(error) =
            InterpreterQueryLog::log_finish(ctx, now, error, has_profiles, &query_profiles)
    {
        error!("Failed to log query finish: {:?}", error)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use concurrent_queue::ConcurrentQueue;
    use databend_common_base::runtime::CaptureLogSettings;
    use databend_common_base::runtime::ThreadTracker;
    use databend_common_tracing::HistoryConfig;
    use databend_common_tracing::HistoryTableConfig;
    use log::LevelFilter;

    use super::captured_info_log_enabled;
    use super::history_table_log_enabled;

    #[test]
    fn test_history_table_log_enabled() {
        let mut history = HistoryConfig::default();
        history.tables.push(HistoryTableConfig {
            table_name: "QUERY_HISTORY".to_string(),
            ..Default::default()
        });

        assert!(!history_table_log_enabled(&history, "query_history"));

        history.on = true;
        assert!(history_table_log_enabled(&history, "query_history"));
        assert!(!history_table_log_enabled(&history, "profile_history"));
    }

    #[test]
    fn test_captured_info_log_enabled() {
        assert!(!captured_info_log_enabled());

        {
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.capture_log_settings = Some(CaptureLogSettings::capture_off());
            let _guard = ThreadTracker::tracking(payload);
            assert!(!captured_info_log_enabled());
        }

        {
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.capture_log_settings = Some(CaptureLogSettings::capture_query(
                LevelFilter::Info,
                Arc::new(ConcurrentQueue::unbounded()),
            ));
            let _guard = ThreadTracker::tracking(payload);
            assert!(captured_info_log_enabled());
        }
    }
}
