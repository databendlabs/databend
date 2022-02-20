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
use common_exception::Result;
use common_planners::PlanNode;

use crate::sessions::QueryContext;

#[derive(Clone, Copy)]
pub enum LogType {
    Start = 1,
    Finish = 2,
    Error = 3,
}

#[derive(Clone)]
pub struct LogEvent {
    // Type.
    pub log_type: LogType,
    pub handler_type: String,

    // User.
    pub tenant_id: String,
    pub cluster_id: String,
    pub sql_user: String,
    pub sql_user_quota: String,
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
    pub scan_rows: u64,
    pub scan_bytes: u64,
    pub scan_byte_cost_ms: u64,
    pub scan_seeks: u64,
    pub scan_seek_cost_ms: u64,
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

    // Extra.
    pub extra: String,
}

pub struct InterpreterQueryLog {
    ctx: Arc<QueryContext>,
    plan: PlanNode,
}

impl InterpreterQueryLog {
    pub fn create(ctx: Arc<QueryContext>, plan: PlanNode) -> Self {
        InterpreterQueryLog { ctx, plan }
    }

    async fn write_log(&self, event: &LogEvent) -> Result<()> {
        let query_log = self.ctx.get_table("system", "query_log").await?;
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
            Series::from_data(vec![event.event_time as u64]),
            // Schema.
            Series::from_data(vec![event.current_database.as_str()]),
            Series::from_data(vec![event.databases.as_str()]),
            Series::from_data(vec![event.tables.as_str()]),
            Series::from_data(vec![event.columns.as_str()]),
            Series::from_data(vec![event.projections.as_str()]),
            // Stats.
            Series::from_data(vec![event.written_rows as u64]),
            Series::from_data(vec![event.written_bytes as u64]),
            Series::from_data(vec![event.scan_rows as u64]),
            Series::from_data(vec![event.scan_bytes as u64]),
            Series::from_data(vec![event.scan_byte_cost_ms as u64]),
            Series::from_data(vec![event.scan_seeks as u64]),
            Series::from_data(vec![event.scan_seek_cost_ms as u64]),
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
            // Extra.
            Series::from_data(vec![event.extra.as_str()]),
        ]);
        let blocks = vec![Ok(block)];
        let input_stream = futures::stream::iter::<Vec<Result<DataBlock>>>(blocks);
        let _ = query_log
            .append_data(self.ctx.clone(), Box::pin(input_stream))
            .await?;

        Ok(())
    }

    pub async fn log_start(&self) -> Result<()> {
        // User.
        let handler_type = self.ctx.get_current_session().get_type();
        let tenant_id = self.ctx.get_tenant();
        let cluster_id = self.ctx.get_config().query.cluster_id;
        let user = self.ctx.get_current_user()?;
        let sql_user = user.name;
        let sql_user_quota = format!("{:?}", user.quota);
        let sql_user_privileges = format!("{:?}", user.grants);

        // Query.
        let query_id = self.ctx.get_id();
        let query_kind = self.plan.name().to_string();
        let query_text = self.ctx.get_query_str();
        // Schema.
        let current_database = self.ctx.get_current_database();

        // Stats.
        let now = SystemTime::now();
        let event_time = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let event_date = (event_time / (24 * 3600000)) as i32;

        let written_rows = 0u64;
        let written_bytes = 0u64;
        let scan_rows = 0u64;
        let scan_bytes = 0u64;
        let scan_byte_cost_ms = 0u64;
        let scan_seeks = 0u64;
        let scan_seek_cost_ms = 0u64;
        let scan_partitions = 0u64;
        let total_partitions = 0u64;
        let result_rows = 0u64;
        let result_bytes = 0u64;
        let cpu_usage = self.ctx.get_settings().get_max_threads()? as u32;
        let memory_usage = self.ctx.get_current_session().get_memory_usage() as u64;

        // Client.
        let client_address = format!("{:?}", self.ctx.get_client_address());

        let log_event = LogEvent {
            log_type: LogType::Start,
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
            scan_rows,
            scan_bytes,
            scan_byte_cost_ms,
            scan_seeks,
            scan_seek_cost_ms,
            scan_partitions,
            total_partitions,
            result_rows,
            result_bytes,
            cpu_usage,
            memory_usage,
            client_info: "".to_string(),
            client_address,

            exception_code: 0,
            exception: "".to_string(),
            stack_trace: "".to_string(),
            server_version: "".to_string(),
            extra: "".to_string(),
        };

        self.write_log(&log_event).await
    }

    pub async fn log_finish(&self) -> Result<()> {
        // User.
        let handler_type = self.ctx.get_current_session().get_type();
        let tenant_id = self.ctx.get_config().query.tenant_id;
        let cluster_id = self.ctx.get_config().query.cluster_id;
        let user = self.ctx.get_current_user()?;
        let sql_user = user.name;
        let sql_user_quota = format!("{:?}", user.quota);
        let sql_user_privileges = format!("{:?}", user.grants);

        // Query.
        let query_id = self.ctx.get_id();
        let query_kind = self.plan.name().to_string();
        let query_text = self.ctx.get_query_str();

        // Stats.
        let now = SystemTime::now();
        let event_time = now
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let event_date = (event_time / (24 * 3600000)) as i32;
        let dal_metrics = self.ctx.get_dal_metrics();
        let written_rows = dal_metrics.write_rows as u64;
        let written_bytes = dal_metrics.write_bytes as u64;
        let scan_rows = self.ctx.get_scan_progress_value().read_rows as u64;
        let scan_bytes = self.ctx.get_scan_progress_value().read_bytes as u64;
        let scan_byte_cost_ms = dal_metrics.read_byte_cost_ms as u64;
        let scan_seeks = dal_metrics.read_seeks as u64;
        let scan_seek_cost_ms = dal_metrics.read_seek_cost_ms as u64;
        let scan_partitions = dal_metrics.partitions_scanned as u64;
        let total_partitions = dal_metrics.partitions_total as u64;
        let cpu_usage = self.ctx.get_settings().get_max_threads()? as u32;
        let memory_usage = self.ctx.get_current_session().get_memory_usage() as u64;

        // Result.
        let result_rows = self.ctx.get_result_progress_value().read_rows as u64;
        let result_bytes = self.ctx.get_result_progress_value().read_bytes as u64;

        // Client.
        let client_address = format!("{:?}", self.ctx.get_client_address());

        // Schema.
        let current_database = self.ctx.get_current_database();

        let log_event = LogEvent {
            log_type: LogType::Finish,
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
            scan_rows,
            scan_bytes,
            scan_byte_cost_ms,
            scan_seeks,
            scan_seek_cost_ms,
            scan_partitions,
            total_partitions,
            result_rows,
            result_bytes,
            cpu_usage,
            memory_usage,
            client_info: "".to_string(),
            client_address,
            current_database,

            exception_code: 0,
            exception: "".to_string(),
            stack_trace: "".to_string(),
            server_version: "".to_string(),
            extra: "".to_string(),
        };

        self.write_log(&log_event).await
    }
}
