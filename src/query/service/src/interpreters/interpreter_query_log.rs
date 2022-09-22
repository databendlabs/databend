// Copyright 2021 Datafuse Labs.
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

use std::fmt::Write;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_preludes::system::LogType;
use common_storages_preludes::system::QueryLogElement;
use common_storages_preludes::system::QueryLogQueue;
use common_tracing::QueryLogger;
use serde_json;
use tracing::error;
use tracing::info;
use tracing::subscriber;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct InterpreterQueryLog;

fn error_fields(log_type: LogType, err: Option<ErrorCode>) -> (LogType, i32, String, String) {
    match err {
        None => (log_type, 0, "".to_string(), "".to_string()),
        Some(e) => {
            if e.code() == ErrorCode::AbortedQuery("").code() {
                (
                    LogType::Aborted,
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
        info!("{}", serde_json::to_string(&event)?);

        if let Some(logger) = QueryLogger::instance().get_subscriber() {
            let event_str = serde_json::to_string(&event)?;
            subscriber::with_default(logger, || {
                info!("{}", event_str);
            });
        };

        QueryLogQueue::instance()?.append_data(event)
    }

    pub fn fail_to_start(ctx: Arc<QueryContext>, err: ErrorCode) {
        ctx.set_error(err.clone());
        InterpreterQueryLog::log_start(&ctx, SystemTime::now(), Some(err))
            .unwrap_or_else(|e| error!("fail to write query_log {:?}", e));
    }

    pub fn log_start(ctx: &QueryContext, now: SystemTime, err: Option<ErrorCode>) -> Result<()> {
        // User.
        let handler_type = ctx.get_current_session().get_type().to_string();
        let tenant_id = ctx.get_tenant();
        let cluster_id = ctx.get_config().query.cluster_id;
        let user = ctx.get_current_user()?;
        let sql_user = user.name;
        let sql_user_quota = format!("{:?}", user.quota);
        let sql_user_privileges = user.grants.to_string();

        // Query.
        let query_id = ctx.get_id();
        let query_kind = ctx.get_query_kind();
        let query_text = ctx.get_query_str();
        // Schema.
        let current_database = ctx.get_current_database();

        // Stats.
        let event_time = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_micros() as i64;
        let event_date = (event_time / (24 * 3_600_000_000)) as i32;

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

        // Client.
        let client_address = match ctx.get_client_address() {
            Some(addr) => format!("{:?}", addr),
            None => "".to_string(),
        };

        // Session settings
        let mut session_settings = String::new();
        for (key, value) in ctx
            .get_current_session()
            .get_settings()
            .get_setting_values_short()
        {
            write!(session_settings, "{}={:?}, ", key, value)
                .expect("write to string must succeed");
        }
        session_settings.push_str("scope: SESSION");

        // Error
        let (log_type, exception_code, exception_text, stack_trace) =
            error_fields(LogType::Start, err);

        Self::write_log(QueryLogElement {
            log_type,
            handler_type,
            tenant_id,
            cluster_id,
            sql_user,
            sql_user_quota,
            sql_user_privileges,
            query_id,
            query_kind,
            query_text,
            event_date,
            event_time,
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
            client_info: "".to_string(),
            client_address,

            exception_code,
            exception_text,
            stack_trace,
            server_version: "".to_string(),
            session_settings,
            extra: "".to_string(),
        })
    }

    pub fn log_finish(ctx: &QueryContext, now: SystemTime, err: Option<ErrorCode>) -> Result<()> {
        // User.
        let handler_type = ctx.get_current_session().get_type().to_string();
        let tenant_id = ctx.get_config().query.tenant_id;
        let cluster_id = ctx.get_config().query.cluster_id;
        let user = ctx.get_current_user()?;
        let sql_user = user.name;
        let sql_user_quota = format!("{:?}", user.quota);
        let sql_user_privileges = user.grants.to_string();

        // Query.
        let query_id = ctx.get_id();
        let query_kind = ctx.get_query_kind();
        let query_text = ctx.get_query_str();

        // Stats.
        let event_time = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_micros() as i64;
        let event_date = (event_time / (24 * 3_600_000_000)) as i32;
        let dal_metrics = ctx.get_dal_metrics();

        let written_rows = ctx.get_write_progress_value().rows as u64;
        let written_bytes = ctx.get_write_progress_value().bytes as u64;
        let written_io_bytes = dal_metrics.get_write_bytes() as u64;
        let written_io_bytes_cost_ms = dal_metrics.get_write_bytes_cost();

        let scan_rows = ctx.get_scan_progress_value().rows as u64;
        let scan_bytes = ctx.get_scan_progress_value().bytes as u64;
        let scan_io_bytes = dal_metrics.get_read_bytes() as u64;
        let scan_io_bytes_cost_ms = dal_metrics.get_read_bytes_cost();

        let scan_partitions = dal_metrics.get_partitions_scanned();
        let total_partitions = dal_metrics.get_partitions_total();
        let cpu_usage = ctx.get_settings().get_max_threads()? as u32;
        let memory_usage = ctx.get_current_session().get_memory_usage() as u64;

        // Result.
        let result_rows = ctx.get_result_progress_value().rows as u64;
        let result_bytes = ctx.get_result_progress_value().bytes as u64;

        // Client.
        let client_address = match ctx.get_client_address() {
            Some(addr) => format!("{:?}", addr),
            None => "".to_string(),
        };

        // Schema.
        let current_database = ctx.get_current_database();

        // Session settings
        let mut session_settings = String::new();
        for (key, value) in ctx
            .get_current_session()
            .get_settings()
            .get_setting_values_short()
        {
            write!(session_settings, "{}={:?}, ", key, value)
                .expect("write to string must succeed");
        }
        session_settings.push_str("scope: SESSION");

        // Error
        let (log_type, exception_code, exception_text, stack_trace) =
            error_fields(LogType::Finish, err);

        Self::write_log(QueryLogElement {
            log_type,
            handler_type,
            tenant_id,
            cluster_id,
            sql_user,
            sql_user_quota,
            sql_user_privileges,
            query_id,
            query_kind,
            query_text,
            event_date,
            event_time,
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
            client_info: "".to_string(),
            client_address,
            current_database,

            exception_code,
            exception_text,
            stack_trace,
            server_version: "".to_string(),
            session_settings,
            extra: "".to_string(),
        })
    }
}
