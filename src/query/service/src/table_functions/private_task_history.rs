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
use std::sync::Arc;

use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::infer_table_schema;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_sql::QueryExecutor;
use databend_common_sql::plans::task_run_schema;

use crate::schedulers::ServiceQueryExecutor;
use crate::task::TaskService;

const TASK_HISTORY_DEFAULT_RESULT_LIMIT: i64 = 100;
const TASK_HISTORY_MAX_RESULT_LIMIT: i64 = 10_000;

pub struct PrivateTaskHistoryTable {
    table_info: TableInfo,
    args_parsed: PrivateTaskHistoryArgs,
    table_args: TableArgs,
}

impl PrivateTaskHistoryTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = PrivateTaskHistoryArgs::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("task_history"),
            meta: TableMeta {
                schema: infer_table_schema(&task_run_schema())
                    .expect("failed to infer task history schema"),
                engine: String::from(table_func_name),
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            args_parsed,
            table_args,
        }))
    }
}

#[async_trait::async_trait]
impl Table for PrivateTaskHistoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::new_exact(1, 1, 1, 1), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                PrivateTaskHistorySource::create(ctx.clone(), output, self.args_parsed.clone())
            },
            1,
        )?;
        Ok(())
    }
}

impl TableFunction for PrivateTaskHistoryTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct PrivateTaskHistorySource {
    ctx: Arc<dyn TableContext>,
    args: PrivateTaskHistoryArgs,
    blocks: Option<std::vec::IntoIter<DataBlock>>,
}

impl PrivateTaskHistorySource {
    fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args: PrivateTaskHistoryArgs,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            ctx,
            args,
            blocks: None,
        })
    }

    async fn load_blocks(&self) -> Result<Vec<DataBlock>> {
        let owners = self
            .ctx
            .get_all_available_roles()
            .await?
            .into_iter()
            .map(|role| role.identity().to_string());
        let ctx = TaskService::instance().create_context(None).await?;
        let executor = ServiceQueryExecutor::new(ctx);
        executor
            .execute_query_with_sql_string(&self.args.to_sql(owners))
            .await
    }
}

#[async_trait::async_trait]
impl AsyncSource for PrivateTaskHistorySource {
    const NAME: &'static str = "task_history";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.blocks.is_none() {
            self.blocks = Some(self.load_blocks().await?.into_iter());
        }

        Ok(self.blocks.as_mut().and_then(|blocks| blocks.next()))
    }
}

#[derive(Clone, Debug)]
struct PrivateTaskHistoryArgs {
    task_name: Option<String>,
    scheduled_time_range_start: Option<String>,
    scheduled_time_range_end: Option<String>,
    result_limit: Option<i64>,
    error_only: Option<bool>,
    root_task_id: Option<String>,
}

impl PrivateTaskHistoryArgs {
    fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_named("task_history")?;

        let mut parsed = Self {
            task_name: None,
            scheduled_time_range_start: None,
            scheduled_time_range_end: None,
            result_limit: None,
            error_only: None,
            root_task_id: None,
        };

        for (k, v) in &args {
            match k.to_lowercase().as_str() {
                "task_name" => parsed.task_name = v.as_string().cloned(),
                "scheduled_time_range_start" => {
                    parsed.scheduled_time_range_start = Some(timestamp_expr(k, v)?);
                }
                "scheduled_time_range_end" => {
                    parsed.scheduled_time_range_end = Some(timestamp_expr(k, v)?);
                }
                "result_limit" => {
                    parsed.result_limit = Some(v.get_i64().ok_or_else(|| {
                        ErrorCode::BadArguments(format!(
                            "unsupported data type for {}, only support integer",
                            k
                        ))
                    })?);
                }
                "error_only" => parsed.error_only = v.as_boolean().cloned(),
                "root_task_id" => parsed.root_task_id = v.as_string().cloned(),
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "unknown param {} for {}",
                        k, "task_history"
                    )));
                }
            }
        }

        Ok(parsed)
    }

    fn to_sql<I>(&self, owners: I) -> String
    where I: IntoIterator<Item = String> {
        let mut filters = Vec::new();
        push_owner_filter(&mut filters, owners);
        if let Some(task_name) = &self.task_name {
            filters.push(format!("task_name = {}", sql_string(task_name)));
        }
        if let Some(start) = &self.scheduled_time_range_start {
            filters.push(format!("scheduled_at >= {start}"));
        }
        if let Some(end) = &self.scheduled_time_range_end {
            filters.push(format!("scheduled_at <= {end}"));
        }
        if self.error_only.unwrap_or(false) {
            filters.push(
                "(state != 'SKIPPED' AND (COALESCE(error_code, 0) != 0 OR error_message IS NOT NULL OR state = 'FAILED'))"
                    .to_string(),
            );
        }
        if let Some(root_task_id) = &self.root_task_id {
            // TODO: private task runs do not yet propagate DAG root task IDs reliably.
            filters.push(format!(
                "CAST(root_task_id AS STRING) = {}",
                sql_string(root_task_id)
            ));
        }

        let where_clause = if filters.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", filters.join(" AND "))
        };
        let limit = self.effective_result_limit();

        format!(
            "SELECT
                task_name AS name,
                task_id AS id,
                owner,
                comment,
                CASE
                    WHEN schedule_type = 0 AND interval_milliseconds IS NOT NULL THEN CONCAT('INTERVAL ', COALESCE(CAST(interval AS STRING), '0'), ' SECOND ', CAST(interval_milliseconds AS STRING), ' MILLISECOND')
                    WHEN schedule_type = 0 THEN CONCAT('INTERVAL ', COALESCE(CAST(interval AS STRING), '0'), ' SECOND')
                    WHEN schedule_type = 1 AND time_zone IS NOT NULL THEN CONCAT('CRON ', cron, ' TIMEZONE ', time_zone)
                    WHEN schedule_type = 1 THEN CONCAT('CRON ', cron)
                    ELSE NULL
                END AS schedule,
                warehouse_name AS warehouse,
                state,
                query_text AS definition,
                COALESCE(when_condition, '') AS condition_text,
                CAST(run_id AS STRING) AS run_id,
                '' AS query_id,
                COALESCE(error_code, 0) AS exception_code,
                error_message AS exception_text,
                attempt_number,
                completed_at AS completed_time,
                scheduled_at AS scheduled_time,
                CAST(root_task_id AS STRING) AS root_task_id,
                session_params AS session_parameters
            FROM system_task.task_run{where_clause}
            ORDER BY scheduled_at DESC LIMIT {limit}"
        )
    }

    fn effective_result_limit(&self) -> i64 {
        match self.result_limit {
            Some(limit) if limit > 0 => limit.min(TASK_HISTORY_MAX_RESULT_LIMIT),
            _ => TASK_HISTORY_DEFAULT_RESULT_LIMIT,
        }
    }
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn push_owner_filter<I>(filters: &mut Vec<String>, owners: I)
where I: IntoIterator<Item = String> {
    let mut owners = owners.into_iter();
    let Some(first_owner) = owners.next() else {
        filters.push("FALSE".to_string());
        return;
    };

    let mut filter = format!("owner IN ({}", sql_string(&first_owner));
    for owner in owners {
        filter.push_str(", ");
        filter.push_str(&sql_string(&owner));
    }
    filter.push(')');
    filters.push(filter);
}

fn timestamp_expr(name: &str, value: &Scalar) -> Result<String> {
    if let Some(timestamp) = value.as_timestamp() {
        timestamp_literal(*timestamp)
    } else if let Some(date) = value.as_date() {
        timestamp_literal(i64::from(*date) * 24 * 60 * 60 * 1_000_000)
    } else {
        Err(ErrorCode::BadArguments(format!(
            "unsupported data type for {}, only support timestamp or date",
            name
        )))
    }
}

fn timestamp_literal(micros: i64) -> Result<String> {
    let secs = micros.div_euclid(1_000_000);
    let nanos = micros.rem_euclid(1_000_000) as u32 * 1_000;
    let timestamp = DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| ErrorCode::BadArguments("timestamp argument is out of range"))?;
    Ok(format!(
        "TIMESTAMP '{}'",
        timestamp.format("%Y-%m-%d %H:%M:%S%.6f")
    ))
}
