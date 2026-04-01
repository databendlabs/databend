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
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::ShowTaskRunsRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::date_helper::DateConverter;
use databend_common_expression::infer_table_schema;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_sql::plans::task_run_schema;
use databend_common_storages_factory::Table;
use databend_common_storages_system::parse_task_runs_to_datablock;
use jiff::tz::TimeZone;

pub struct TaskHistoryTable {
    table_info: TableInfo,
    args_parsed: TableHistoryArgsParsed,
    table_args: TableArgs,
}

impl TaskHistoryTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = TableHistoryArgsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("task_history"),
            meta: TableMeta {
                schema: infer_table_schema(&task_run_schema())
                    .expect("failed to infer table schema"),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(TaskHistoryTable {
            table_info,
            args_parsed,
            table_args,
        }))
    }
}

#[async_trait::async_trait]
impl Table for TaskHistoryTable {
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
        // dummy statistics
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
            |output| TaskHistorySource::create(ctx.clone(), output, self.args_parsed.clone()),
            1,
        )?;

        Ok(())
    }
}

struct TaskHistorySource {
    is_finished: bool,
    args_parsed: TableHistoryArgsParsed,
    ctx: Arc<dyn TableContext>,
}

impl TaskHistorySource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: TableHistoryArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, TaskHistorySource {
            ctx,
            args_parsed,
            is_finished: false,
        })
    }
    async fn build_request(&self) -> Result<ShowTaskRunsRequest> {
        let tenant = self.ctx.get_tenant();
        let available_roles = self.ctx.get_all_available_roles().await?;
        Ok(ShowTaskRunsRequest {
            tenant_id: tenant.tenant_name().to_string(),
            scheduled_time_start: self
                .args_parsed
                .scheduled_time_range_start
                .clone()
                .unwrap_or("".to_string()),
            scheduled_time_end: self
                .args_parsed
                .scheduled_time_range_end
                .clone()
                .unwrap_or("".to_string()),
            task_name: self.args_parsed.task_name.clone().unwrap_or("".to_string()),
            result_limit: self.args_parsed.result_limit.unwrap_or(0), // 0 means default
            error_only: self.args_parsed.error_only.unwrap_or(false),
            owners: available_roles
                .into_iter()
                .map(|x| x.identity().to_string())
                .collect(),
            task_names: vec![],
            root_task_id: self.args_parsed.root_task_id.clone(),
            next_page_token: None,
            page_size: None,
            previous_page_token: None,
            task_ids: vec![],
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for TaskHistorySource {
    const NAME: &'static str = "task_history";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }
        self.is_finished = true;
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot view system.task_history table without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let tenant = self.ctx.get_tenant();
        let query_id = self.ctx.get_id();
        let user = self
            .ctx
            .get_current_user()?
            .identity()
            .display()
            .to_string();
        let cfg = build_client_config(
            tenant.tenant_name().to_string(),
            user,
            query_id,
            cloud_api.get_timeout(),
        );
        let req = self.build_request().await?;
        let resp = cloud_api
            .get_task_client()
            .show_task_runs_full(cfg, req)
            .await?;
        let trs = resp
            .into_iter()
            .flat_map(|r| r.task_runs)
            .collect::<Vec<_>>();
        parse_task_runs_to_datablock(trs).map(Some)
    }
}

impl TableFunction for TaskHistoryTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct TableHistoryArgsParsed {
    pub(crate) task_name: Option<String>,
    pub(crate) scheduled_time_range_start: Option<String>,
    pub(crate) scheduled_time_range_end: Option<String>,
    pub(crate) result_limit: Option<i32>,
    pub(crate) error_only: Option<bool>,
    pub(crate) root_task_id: Option<String>,
}

fn parse_date_or_timestamp(v: &Scalar) -> Option<String> {
    if v.as_timestamp().is_some() {
        Some(
            v.as_timestamp()
                .map(|s| s.to_timestamp(&TimeZone::UTC).to_string())
                .unwrap(),
        )
    } else if v.as_date().is_some() {
        Some(
            v.as_date()
                .map(|s| {
                    s.to_date(&TimeZone::UTC)
                        .at(0, 0, 0, 0)
                        .in_tz("UTC")
                        .unwrap()
                        .to_string()
                })
                .unwrap(),
        )
    } else {
        return None;
    }
}

impl TableHistoryArgsParsed {
    pub fn parse(table_args: &TableArgs) -> databend_common_exception::Result<Self> {
        let args = table_args.expect_all_named("task_history")?;

        let mut task_name = None;
        let mut scheduled_time_range_start = None;
        let mut scheduled_time_range_end = None;
        let mut result_limit = None;
        let mut error_only = None;
        let mut root_task_id = None;
        for (k, v) in &args {
            match k.to_lowercase().as_str() {
                "task_name" => task_name = v.as_string().cloned(),
                "scheduled_time_range_start" | "scheduled_time_range_end" => {
                    if v.as_timestamp().is_none() && v.as_date().is_none() {
                        return Err(ErrorCode::BadArguments(format!(
                            "unsupported data type for {}, only support timestamp or date",
                            k,
                        )));
                    }
                    if k == "scheduled_time_range_start" {
                        scheduled_time_range_start = parse_date_or_timestamp(v);
                    } else {
                        scheduled_time_range_end = parse_date_or_timestamp(v);
                    }
                }
                "result_limit" => result_limit = Some(v.get_i64().map(|x| x as i32).unwrap()),
                "error_only" => error_only = v.as_boolean().cloned(),
                "root_task_id" => root_task_id = v.as_string().cloned(),
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "unknown param {} for {}",
                        k, "task_history"
                    )));
                }
            }
        }

        Ok(Self {
            task_name,
            scheduled_time_range_start,
            scheduled_time_range_end,
            result_limit,
            error_only,
            root_task_id,
        })
    }
}
