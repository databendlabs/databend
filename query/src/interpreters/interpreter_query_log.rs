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

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use common_datablocks::DataBlock;
use common_datavalues::prelude::Series;
use common_datavalues::prelude::SeriesFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_tracing::tracing;
use serde::Serialize;
use serde_json;

use crate::catalogs::CATALOG_DEFAULT;
use crate::sessions::QueryContext;

#[derive(Clone, Copy, Serialize)]
pub enum LogType {
    Start = 1,
    Finish = 2,
    Error = 3,
    Aborted = 4,
}

#[derive(Clone, Serialize)]
pub struct LogEvent {
    // Type.
    pub log_type: LogType,
    pub handler_type: String,

    // User.
    pub tenant_id: String,
    pub cluster_id: String,
    pub sql_user: String,
    pub sql_user_quota: String,
    #[serde(skip_serializing)]
    pub sql_user_privileges: String,

    // Query.
    pub query_id: String,
    pub query_kind: String,
    pub query_text: String,
    pub event_date: i32,
    pub event_time: u64,

    // Schema.
    pub current_database: String,
    pub databases: String,
    pub tables: String,
    pub columns: String,
    pub projections: String,

    // Stats.
    pub written_rows: u64,
    pub written_bytes: u64,
    pub written_io_bytes: u64,
    pub written_io_bytes_cost_ms: u64,
    pub scan_rows: u64,
    pub scan_bytes: u64,
    pub scan_io_bytes: u64,
    pub scan_io_bytes_cost_ms: u64,
    pub scan_partitions: u64,
    pub total_partitions: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub cpu_usage: u32,
    pub memory_usage: u64,

    // Client.
    pub client_info: String,
    pub client_address: String,

    // Exception.
    pub exception_code: i32,
    pub exception: String,
    pub stack_trace: String,

    // Server.
    pub server_version: String,

    // Session settings
    #[serde(skip_serializing)]
    pub session_settings: String,

    // Extra.
    pub extra: String,
}

#[derive(Clone)]
pub struct InterpreterQueryLog {
    ctx: Arc<QueryContext>,
    plan: Option<PlanNode>,
}

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
    pub fn create(ctx: Arc<QueryContext>, plan: Option<PlanNode>) -> Self {
        InterpreterQueryLog { ctx, plan }
    }

    async fn write_log(&self, event: &LogEvent) -> Result<()> {
        let query_log = self
            .ctx
            .get_table(CATALOG_DEFAULT, "system", "query_log")
            .await?;
        let schema = query_log.get_table_info().meta.schema.clone();

        let block = DataBlock::create(schema.clone(), vec![
            // Type.
            Series::from_data(vec![event.log_type as i8]),
            Series::from_data(vec![event.handler_type.as_str()]),
            // User.
            Series::from_data(vec![event.tenant_id.as_str()]),
            Series::from_data(vec![event.cluster_id.as_str()]),
            Series::from_data(vec![event.sql_user.as_str()]),
            Series::from_data(vec![event.sql_user_quota.as_str()]),
            Series::from_data(vec![event.sql_user_privileges.as_str()]),
            // Query.
            Series::from_data(vec![event.query_id.as_str()]),
            Series::from_data(vec![event.query_kind.as_str()]),
            Series::from_data(vec![event.query_text.as_str()]),
            Series::from_data(vec![event.event_date as i32]),
            Series::from_data(vec![event.event_time as i64]),
            // Schema.
            Series::from_data(vec![event.current_database.as_str()]),
            Series::from_data(vec![event.databases.as_str()]),
            Series::from_data(vec![event.tables.as_str()]),
            Series::from_data(vec![event.columns.as_str()]),
            Series::from_data(vec![event.projections.as_str()]),
            // Stats.
            Series::from_data(vec![event.written_rows as u64]),
            Series::from_data(vec![event.written_bytes as u64]),
            Series::from_data(vec![event.written_io_bytes as u64]),
            Series::from_data(vec![event.written_io_bytes_cost_ms as u64]),
            Series::from_data(vec![event.scan_rows as u64]),
            Series::from_data(vec![event.scan_bytes as u64]),
            Series::from_data(vec![event.scan_io_bytes as u64]),
            Series::from_data(vec![event.scan_io_bytes_cost_ms as u64]),
            Series::from_data(vec![event.scan_partitions as u64]),
            Series::from_data(vec![event.total_partitions as u64]),
            Series::from_data(vec![event.result_rows as u64]),
            Series::from_data(vec![event.result_bytes as u64]),
            Series::from_data(vec![event.cpu_usage]),
            Series::from_data(vec![event.memory_usage as u64]),
            // Client.
            Series::from_data(vec![event.client_info.as_str()]),
            Series::from_data(vec![event.client_address.as_str()]),
            // Exception.
            Series::from_data(vec![event.exception_code]),
            Series::from_data(vec![event.exception.as_str()]),
            Series::from_data(vec![event.stack_trace.as_str()]),
            // Server.
            Series::from_data(vec![event.server_version.as_str()]),
            // Session settings
            Series::from_data(vec![event.session_settings.as_str()]),
            // Extra.
            Series::from_data(vec![event.extra.as_str()]),
        ]);
        let blocks = vec![Ok(block)];
        let input_stream = futures::stream::iter::<Vec<Result<DataBlock>>>(blocks);
        let _ = query_log
            .append_data(self.ctx.clone(), Box::pin(input_stream))
            .await?;

        match self.ctx.get_query_logger() {
            Some(logger) => {
                let event_str = serde_json::to_string(event)?;
                tracing::subscriber::with_default(logger, || {
                    tracing::info!("{}", event_str);
                });
            }
            None => {}
        };

        Ok(())
    }

    pub async fn fail_to_start(ctx: Arc<QueryContext>, err: ErrorCode) {
        ctx.set_error(err.clone());
        InterpreterQueryLog::create(ctx, None)
            .log_start(SystemTime::now(), Some(err))
            .await
            .unwrap_or_else(|e| tracing::error!("fail to write query_log {:?}", e));
    }

    pub async fn log_start(&self, now: SystemTime, err: Option<ErrorCode>) -> Result<()> {
        // User.
        let handler_type = self.ctx.get_current_session().get_type().to_string();
        let tenant_id = self.ctx.get_tenant();
        let cluster_id = self.ctx.get_config().query.cluster_id;
        let user = self.ctx.get_current_user()?;
        let sql_user = user.name;
        let sql_user_quota = format!("{:?}", user.quota);
        let sql_user_privileges = user.grants.to_string();

        // Query.
        let query_id = self.ctx.get_id();
        let query_kind = self
            .plan
            .as_ref()
            .map(|p| p.name().to_string())
            .unwrap_or_else(|| "".to_string());
        let query_text = self.ctx.get_query_str();
        // Schema.
        let current_database = self.ctx.get_current_database();

        // Stats.
        let event_time = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let event_date = (event_time / (24 * 3600000)) as i32;

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
        let cpu_usage = self.ctx.get_settings().get_max_threads()? as u32;
        let memory_usage = self.ctx.get_current_session().get_memory_usage() as u64;

        // Client.
        let client_address = match self.ctx.get_client_address() {
            Some(addr) => format!("{:?}", addr),
            None => "".to_string(),
        };

        // Session settings
        let mut session_settings = String::new();
        for (key, value) in self
            .ctx
            .get_current_session()
            .get_settings()
            .get_setting_values_short()
        {
            session_settings.push_str(&format!("{}={}, ", key, value));
        }
        session_settings.push_str("scope: SESSION");

        // Error
        let (log_type, exception_code, exception, stack_trace) = error_fields(LogType::Start, err);

        let log_event = LogEvent {
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
            exception,
            stack_trace,
            server_version: "".to_string(),
            session_settings,
            extra: "".to_string(),
        };

        self.write_log(&log_event).await
    }

    pub async fn log_finish(&self, now: SystemTime, err: Option<ErrorCode>) -> Result<()> {
        // User.
        let handler_type = self.ctx.get_current_session().get_type().to_string();
        let tenant_id = self.ctx.get_config().query.tenant_id;
        let cluster_id = self.ctx.get_config().query.cluster_id;
        let user = self.ctx.get_current_user()?;
        let sql_user = user.name;
        let sql_user_quota = format!("{:?}", user.quota);
        let sql_user_privileges = user.grants.to_string();

        // Query.
        let query_id = self.ctx.get_id();
        let query_kind = self
            .plan
            .as_ref()
            .map(|p| p.name())
            .unwrap_or("")
            .to_string();
        let query_text = self.ctx.get_query_str();

        // Stats.
        let event_time = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let event_date = (event_time / (24 * 3600000)) as i32;
        let dal_metrics = self.ctx.get_dal_metrics();

        let written_rows = self.ctx.get_write_progress_value().rows as u64;
        let written_bytes = self.ctx.get_write_progress_value().bytes as u64;
        let written_io_bytes = dal_metrics.get_write_bytes() as u64;
        let written_io_bytes_cost_ms = dal_metrics.get_write_bytes_cost();

        let scan_rows = self.ctx.get_scan_progress_value().rows as u64;
        let scan_bytes = self.ctx.get_scan_progress_value().bytes as u64;
        let scan_io_bytes = dal_metrics.get_read_bytes() as u64;
        let scan_io_bytes_cost_ms = dal_metrics.get_read_bytes_cost();

        let scan_partitions = dal_metrics.get_partitions_scanned();
        let total_partitions = dal_metrics.get_partitions_total();
        let cpu_usage = self.ctx.get_settings().get_max_threads()? as u32;
        let memory_usage = self.ctx.get_current_session().get_memory_usage() as u64;

        // Result.
        let result_rows = self.ctx.get_result_progress_value().rows as u64;
        let result_bytes = self.ctx.get_result_progress_value().bytes as u64;

        // Client.
        let client_address = match self.ctx.get_client_address() {
            Some(addr) => format!("{:?}", addr),
            None => "".to_string(),
        };

        // Schema.
        let current_database = self.ctx.get_current_database();

        // Session settings
        let mut session_settings = String::new();
        for (key, value) in self
            .ctx
            .get_current_session()
            .get_settings()
            .get_setting_values_short()
        {
            session_settings.push_str(&format!("{}={}, ", key, value));
        }
        session_settings.push_str("scope: SESSION");

        // Error
        let (log_type, exception_code, exception, stack_trace) = error_fields(LogType::Finish, err);

        let log_event = LogEvent {
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
            exception,
            stack_trace,
            server_version: "".to_string(),
            session_settings,
            extra: "".to_string(),
        };

        self.write_log(&log_event).await
    }
}
