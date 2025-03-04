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

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::time::SystemTime;

use databend_common_config::GlobalConfig;
use databend_common_config::DATABEND_COMMIT_VERSION;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_system::LogType;
use databend_common_storages_system::QueryLogElement;
use databend_common_storages_system::QueryLogQueue;
use log::error;
use log::info;
use serde_json;

use crate::sessions::convert_query_log_timestamp;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct InterpreterQueryLog;

fn error_fields<C>(log_type: LogType, err: Option<ErrorCode<C>>) -> (LogType, i32, String, String) {
    match err {
        None => (log_type, 0, "".to_string(), "".to_string()),
        Some(e) => {
            if e.code() == ErrorCode::ABORTED_QUERY {
                (
                    LogType::Aborted,
                    e.code().into(),
                    e.to_string(),
                    e.backtrace_str(),
                )
            } else if e.code() == ErrorCode::ABORTED_QUERY {
                (
                    LogType::Closed,
                    e.code().into(),
                    e.to_string(),
                    e.backtrace_str(),
                )
            } else {
                (
                    LogType::Error,
                    e.code().into(),
                    e.to_string(),
                    e.backtrace_str(),
                )
            }
        }
    }
}

impl InterpreterQueryLog {
    fn write_log(event: QueryLogElement) -> Result<()> {
        let event_str = serde_json::to_string(&event)?;
        // log the query log in JSON format
        info!(target: "databend::log::query", "{}", event_str);
        // log the query event in the system log
        info!("query: {} becomes {:?}", event.query_id, event.log_type);
        QueryLogQueue::instance()?.append_data(event)
    }

    pub fn fail_to_start(ctx: Arc<QueryContext>, err: ErrorCode) {
        InterpreterQueryLog::log_start(&ctx, SystemTime::now(), Some(err))
            .unwrap_or_else(|e| error!("fail to write query_log {:?}", e));
    }

    pub fn log_start(ctx: &QueryContext, now: SystemTime, err: Option<ErrorCode>) -> Result<()> {
        // User.
        let handler_type = ctx.get_current_session().get_type().to_string();
        let tenant_id = ctx.get_tenant();
        let cluster_id = GlobalConfig::instance().query.cluster_id.clone();
        let node_id = ctx.get_cluster().local_id.clone();
        let user = ctx.get_current_user()?;
        let sql_user = user.name;
        let sql_user_quota = format!("{:?}", user.quota);
        let sql_user_privileges = user.grants.to_string();

        // Query.
        let query_id = ctx.get_id();
        let query_kind = ctx.get_query_kind().to_string();
        let query_text = ctx.get_query_str();
        let query_hash = ctx.get_query_text_hash();
        let query_parameterized_hash = ctx.get_query_parameterized_hash();
        // Schema.
        let current_database = ctx.get_current_database();

        // Stats.
        let event_time = convert_query_log_timestamp(now);
        let event_date = (event_time / (24 * 3_600_000_000)) as i32;
        let query_start_time = convert_query_log_timestamp(ctx.get_created_time());
        let query_queued_duration_ms = ctx.get_query_queued_duration().as_millis() as i64;

        let written_rows = 0u64;
        let written_bytes = 0u64;
        let written_io_bytes = 0u64;
        let written_io_bytes_cost_ms = 0u64;
        let scan_rows = 0u64;
        let scan_bytes = 0u64;
        let scan_io_bytes = 0u64;
        let scan_io_bytes_cost_ms = 0u64;
        let scan_partitions = 0u64;
        let total_partitions = 0u64;
        let result_rows = 0u64;
        let result_bytes = 0u64;
        let cpu_usage = ctx.get_settings().get_max_threads()? as u32;
        let memory_usage = ctx.get_current_session().get_memory_usage() as u64;
        let join_spilled_rows = 0u64;
        let join_spilled_bytes = 0u64;
        let agg_spilled_rows = 0u64;
        let agg_spilled_bytes = 0u64;
        let group_by_spilled_rows = 0u64;
        let group_by_spilled_bytes = 0u64;

        let bytes_from_storage = 0;
        let bytes_from_disk_cache = 0;
        let bytes_from_mem_cache = 0;

        // Client.
        let client_address = match ctx.get_client_address() {
            Some(addr) => addr,
            None => "".to_string(),
        };
        let user_agent = ctx.get_ua();
        // Session settings
        let mut session_settings = String::new();
        let current_session = ctx.get_current_session();
        for item in current_session.get_settings().into_iter() {
            write!(session_settings, "{}={:?}, ", item.name, item.user_value)
                .expect("write to string must succeed");
        }

        let query_tag = if let Ok(tag) = current_session.get_settings().get_query_tag() {
            tag
        } else {
            "".to_string()
        };
        session_settings.push_str("scope: SESSION");

        // Error
        let (log_type, exception_code, exception_text, stack_trace) =
            error_fields(LogType::Start, err);
        let log_type_name = log_type.as_string();

        // Transaction.
        let txn_mgr = ctx.txn_mgr();
        let guard = txn_mgr.lock();
        let txn_state = format!("{:?}", guard.state());
        let txn_id = guard.txn_id().to_string();
        drop(guard);
        Self::write_log(QueryLogElement {
            log_type,
            log_type_name,
            handler_type,
            tenant_id: tenant_id.tenant_name().to_string(),
            cluster_id,
            node_id,
            sql_user,
            sql_user_quota,
            sql_user_privileges,
            query_id,
            query_kind,
            query_text,
            query_hash,
            query_parameterized_hash,
            event_date,
            event_time,
            query_start_time,
            query_duration_ms: 0,
            query_queued_duration_ms,
            current_database,
            databases: "".to_string(),
            tables: "".to_string(),
            columns: "".to_string(),
            projections: "".to_string(),
            written_rows,
            written_bytes,
            written_io_bytes,
            written_io_bytes_cost_ms,
            scan_rows,
            scan_bytes,
            scan_io_bytes,
            scan_io_bytes_cost_ms,
            scan_partitions,
            total_partitions,
            result_rows,
            result_bytes,
            cpu_usage,
            memory_usage,
            join_spilled_bytes,
            join_spilled_rows,
            agg_spilled_bytes,
            agg_spilled_rows,
            group_by_spilled_bytes,
            group_by_spilled_rows,
            bytes_from_remote_disk: bytes_from_storage,
            bytes_from_local_disk: bytes_from_disk_cache,
            bytes_from_memory: bytes_from_mem_cache,

            client_info: "".to_string(),
            client_address,
            user_agent,

            exception_code,
            exception_text,
            stack_trace,
            server_version: DATABEND_COMMIT_VERSION.to_string(),
            query_tag,
            session_settings,
            extra: "".to_string(),
            has_profiles: false,
            txn_state,
            txn_id,
            peak_memory_usage: HashMap::new(),
        })
    }

    pub fn log_finish<C>(
        ctx: &QueryContext,
        now: SystemTime,
        err: Option<ErrorCode<C>>,
        has_profiles: bool,
    ) -> Result<()> {
        ctx.set_finish_time(now);
        // User.
        let handler_type = ctx.get_current_session().get_type().to_string();
        let tenant_id = GlobalConfig::instance()
            .query
            .tenant_id
            .tenant_name()
            .to_string();
        let cluster_id = GlobalConfig::instance().query.cluster_id.clone();
        let node_id = ctx.get_cluster().local_id.clone();
        let user = ctx.get_current_user()?;
        let sql_user = user.name;
        let sql_user_quota = format!("{:?}", user.quota);
        let sql_user_privileges = user.grants.to_string();

        // Query.
        let query_id = ctx.get_id();
        let query_kind = ctx.get_query_kind().to_string();
        let query_text = ctx.get_query_str();
        let query_hash = ctx.get_query_text_hash();
        let query_parameterized_hash = ctx.get_query_parameterized_hash();

        // Stats.
        let event_time = convert_query_log_timestamp(now);
        let event_date = (event_time / (24 * 3_600_000_000)) as i32;
        let query_start_time = convert_query_log_timestamp(ctx.get_created_time());
        let query_duration_ms = ctx.get_query_duration_ms();
        let query_queued_duration_ms = ctx.get_query_queued_duration().as_millis() as i64;
        let data_metrics = ctx.get_data_metrics();

        let written_rows = ctx.get_write_progress_value().rows as u64;
        let written_bytes = ctx.get_write_progress_value().bytes as u64;
        let written_io_bytes = data_metrics.get_write_bytes() as u64;
        let written_io_bytes_cost_ms = data_metrics.get_write_bytes_cost();

        let scan_rows = ctx.get_scan_progress_value().rows as u64;
        let scan_bytes = ctx.get_scan_progress_value().bytes as u64;
        let scan_io_bytes = data_metrics.get_read_bytes() as u64;
        let scan_io_bytes_cost_ms = data_metrics.get_read_bytes_cost();

        let scan_partitions = data_metrics.get_partitions_scanned();
        let total_partitions = data_metrics.get_partitions_total();
        let cpu_usage = ctx.get_settings().get_max_threads()? as u32;
        let memory_usage = ctx.get_current_session().get_memory_usage() as u64;

        let join_spilled_rows = ctx.get_join_spill_progress_value().rows as u64;
        let join_spilled_bytes = ctx.get_join_spill_progress_value().bytes as u64;

        let agg_spilled_rows = ctx.get_aggregate_spill_progress_value().rows as u64;
        let agg_spilled_bytes = ctx.get_aggregate_spill_progress_value().bytes as u64;

        let group_by_spilled_rows = ctx.get_group_by_spill_progress_value().rows as u64;
        let group_by_spilled_bytes = ctx.get_group_by_spill_progress_value().bytes as u64;

        // Result.
        let result_rows = ctx.get_result_progress_value().rows as u64;
        let result_bytes = ctx.get_result_progress_value().bytes as u64;

        let data_cache_metrics = ctx.get_data_cache_metrics().as_values();
        let bytes_from_remote_disk = data_cache_metrics.bytes_from_remote_disk as u64;
        let bytes_from_local_disk = data_cache_metrics.bytes_from_local_disk as u64;
        let bytes_from_memory = data_cache_metrics.bytes_from_memory as u64;

        // Client.
        let client_address = match ctx.get_client_address() {
            Some(addr) => addr,
            None => "".to_string(),
        };
        let user_agent = ctx.get_ua();

        // Schema.
        let current_database = ctx.get_current_database();

        // Session settings
        let mut session_settings = String::new();
        let current_session = ctx.get_current_session();

        for item in current_session.get_settings().into_iter() {
            write!(session_settings, "{}={:?}, ", item.name, item.user_value)
                .expect("write to string must succeed");
        }

        // Session
        let query_tag = if let Ok(tag) = current_session.get_settings().get_query_tag() {
            tag
        } else {
            "".to_string()
        };
        session_settings.push_str("scope: SESSION");

        // Error
        let (log_type, exception_code, exception_text, stack_trace) =
            error_fields(LogType::Finish, err);
        let log_type_name = log_type.as_string();

        // Transaction.
        let txn_mgr = ctx.txn_mgr();
        let guard = txn_mgr.lock();
        let txn_state = format!("{:?}", guard.state());
        let txn_id = guard.txn_id().to_string();
        drop(guard);

        let peak_memory_usage = ctx.get_node_peek_memory_usage();

        Self::write_log(QueryLogElement {
            log_type,
            log_type_name,
            handler_type,
            tenant_id,
            cluster_id,
            node_id,
            sql_user,
            sql_user_quota,
            sql_user_privileges,
            query_id,
            query_kind,
            query_text,
            query_hash,
            query_parameterized_hash,
            event_date,
            event_time,
            query_start_time,
            query_duration_ms,
            query_queued_duration_ms,
            databases: "".to_string(),
            tables: "".to_string(),
            columns: "".to_string(),
            projections: "".to_string(),
            written_rows,
            written_bytes,
            written_io_bytes,
            written_io_bytes_cost_ms,
            scan_rows,
            scan_bytes,
            scan_io_bytes,
            scan_io_bytes_cost_ms,
            scan_partitions,
            total_partitions,
            result_rows,
            result_bytes,
            cpu_usage,
            memory_usage,
            join_spilled_bytes,
            join_spilled_rows,
            agg_spilled_bytes,
            agg_spilled_rows,
            group_by_spilled_bytes,
            group_by_spilled_rows,
            bytes_from_remote_disk,
            bytes_from_local_disk,
            bytes_from_memory,

            client_info: "".to_string(),
            client_address,
            user_agent,
            current_database,

            exception_code,
            exception_text,
            stack_trace,
            server_version: DATABEND_COMMIT_VERSION.to_string(),
            query_tag,
            session_settings,
            extra: "".to_string(),
            has_profiles,
            txn_state,
            txn_id,
            peak_memory_usage,
        })
    }
}
