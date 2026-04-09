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

use std::sync::Arc;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::ShowTaskRunsRequest;
use databend_common_cloud_control::task_utils::TaskRun;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::date_helper::DateConverter;
use databend_common_expression::expr::*;
use databend_common_expression::filter_helper::FilterHelpers;
use databend_common_expression::infer_table_schema;
use databend_common_expression::type_check::check_string;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::plans::task_run_schema;
use databend_common_storages_system::AsyncOneBlockSystemTable;
use databend_common_storages_system::AsyncSystemTable;
use jiff::tz::TimeZone;

pub fn parse_task_runs_to_datablock(task_runs: Vec<TaskRun>) -> Result<DataBlock> {
    let mut name = Vec::with_capacity(task_runs.len());
    let mut id = Vec::with_capacity(task_runs.len());
    let mut owner = Vec::with_capacity(task_runs.len());
    let mut definition = Vec::with_capacity(task_runs.len());
    let mut condition_text = Vec::with_capacity(task_runs.len());
    let mut comment = Vec::with_capacity(task_runs.len());
    let mut schedule = Vec::with_capacity(task_runs.len());
    let mut warehouse = Vec::with_capacity(task_runs.len());
    let mut state = Vec::with_capacity(task_runs.len());
    let mut exception_text = Vec::with_capacity(task_runs.len());
    let mut exception_code = Vec::with_capacity(task_runs.len());
    let mut run_id = Vec::with_capacity(task_runs.len());
    let mut query_id = Vec::with_capacity(task_runs.len());
    let mut attempt_number = Vec::with_capacity(task_runs.len());
    let mut scheduled_time = Vec::with_capacity(task_runs.len());
    let mut completed_time = Vec::with_capacity(task_runs.len());
    let mut root_task_id = Vec::with_capacity(task_runs.len());
    let mut session_params = Vec::with_capacity(task_runs.len());

    for task_run in task_runs {
        name.push(task_run.task_name);
        id.push(task_run.task_id);
        owner.push(task_run.owner);
        comment.push(task_run.comment);
        schedule.push(task_run.schedule_options);
        warehouse.push(task_run.warehouse_options.and_then(|opts| opts.warehouse));
        state.push(task_run.state.to_string());
        exception_code.push(task_run.error_code);
        exception_text.push(task_run.error_message);
        definition.push(task_run.query_text);
        condition_text.push(task_run.condition_text);
        run_id.push(task_run.run_id);
        query_id.push(task_run.query_id);
        attempt_number.push(task_run.attempt_number);
        completed_time.push(task_run.completed_at.map(|t| t.timestamp_micros()));
        scheduled_time.push(task_run.scheduled_at.timestamp_micros());
        root_task_id.push(task_run.root_task_id);
        session_params.push(Some(serde_json::to_vec(&task_run.session_params).unwrap()));
    }

    Ok(DataBlock::new_from_columns(vec![
        StringType::from_data(name),
        UInt64Type::from_data(id),
        StringType::from_data(owner),
        StringType::from_opt_data(comment),
        StringType::from_opt_data(schedule),
        StringType::from_opt_data(warehouse),
        StringType::from_data(state),
        StringType::from_data(definition),
        StringType::from_data(condition_text),
        StringType::from_data(run_id),
        StringType::from_data(query_id),
        Int64Type::from_data(exception_code),
        StringType::from_opt_data(exception_text),
        Int32Type::from_data(attempt_number),
        TimestampType::from_opt_data(completed_time),
        TimestampType::from_data(scheduled_time),
        StringType::from_data(root_task_id),
        VariantType::from_opt_data(session_params),
    ]))
}

pub struct TaskHistoryTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for TaskHistoryTable {
    const NAME: &'static str = "system.task_history";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        if GlobalConfig::instance()
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot view system.task_history table without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }

        let tenant = ctx.get_tenant();
        let query_id = ctx.get_id();
        let user = ctx.get_current_user()?.identity().display().to_string();
        let available_roles = ctx.get_all_available_roles().await?;
        let result_limit = push_downs.as_ref().and_then(|v| v.limit.map(|i| i as i32));
        let mut task_name = None;
        let mut scheduled_time_start = None;
        let mut scheduled_time_end = None;

        if let Some(push_downs) = push_downs
            && let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter)
        {
            let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
            let func_ctx = ctx.get_function_context()?;
            let (name, _) = extract_leveled_strings(&expr, &["name"], &func_ctx)?;
            if name.len() == 1 {
                task_name = Some(name[0].clone());
            }
            find_lt_filter(&expr, &mut |col_name, scalar| {
                if col_name == "scheduled_time"
                    && let Scalar::Timestamp(s) = scalar
                {
                    scheduled_time_end =
                        Some(s.to_timestamp(&TimeZone::UTC).timestamp().to_string());
                }
            });
            find_gt_filter(&expr, &mut |col_name, scalar| {
                if col_name == "scheduled_time"
                    && let Scalar::Timestamp(s) = scalar
                {
                    scheduled_time_start =
                        Some(s.to_timestamp(&TimeZone::UTC).timestamp().to_string());
                }
            });
        }

        let req = ShowTaskRunsRequest {
            tenant_id: tenant.tenant_name().to_string(),
            scheduled_time_start: scheduled_time_start.unwrap_or_default(),
            scheduled_time_end: scheduled_time_end.unwrap_or_default(),
            task_name: task_name.unwrap_or_default(),
            result_limit: result_limit.unwrap_or(0),
            error_only: false,
            owners: available_roles
                .into_iter()
                .map(|x| x.identity().to_string())
                .collect(),
            next_page_token: None,
            page_size: None,
            previous_page_token: None,
            task_ids: vec![],
            task_names: vec![],
            root_task_id: None,
        };

        let cloud_api = CloudControlApiProvider::instance();
        let config = build_client_config(
            tenant.tenant_name().to_string(),
            user,
            query_id,
            cloud_api.get_timeout(),
        );
        let resp = cloud_api
            .get_task_client()
            .show_task_runs_full(config, req)
            .await?;
        let task_runs = resp
            .into_iter()
            .flat_map(|r| r.task_runs)
            .collect::<Vec<_>>();

        parse_task_runs_to_datablock(
            task_runs
                .into_iter()
                .map(TaskRun::try_from)
                .collect::<Result<Vec<_>>>()?,
        )
    }
}

impl TaskHistoryTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = infer_table_schema(&task_run_schema())
            .expect("failed to parse task history table schema");

        let table_info = TableInfo {
            desc: "'system'.'task_history'".to_string(),
            name: "task_history".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTaskHistory".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}

fn find_gt_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
        Expr::Cast(Cast { expr, .. }) => find_gt_filter(expr, visitor),
        Expr::FunctionCall(FunctionCall { function, args, .. }) => {
            if function.signature.name == "gt" || function.signature.name == "gte" {
                match args.as_slice() {
                    [
                        Expr::ColumnRef(ColumnRef { id, .. }),
                        Expr::Constant(Constant { scalar, .. }),
                    ]
                    | [
                        Expr::Constant(Constant { scalar, .. }),
                        Expr::ColumnRef(ColumnRef { id, .. }),
                    ] => visitor(id, scalar),
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                for arg in args {
                    find_gt_filter(arg, visitor);
                }
            }
        }
        Expr::LambdaFunctionCall(LambdaFunctionCall { args, .. }) => {
            for arg in args {
                find_gt_filter(arg, visitor);
            }
        }
    }
}

fn find_lt_filter(expr: &Expr<String>, visitor: &mut impl FnMut(&str, &Scalar)) {
    match expr {
        Expr::Constant(_) | Expr::ColumnRef(_) => {}
        Expr::Cast(Cast { expr, .. }) => find_lt_filter(expr, visitor),
        Expr::FunctionCall(FunctionCall { function, args, .. }) => {
            if function.signature.name == "lt" || function.signature.name == "lte" {
                match args.as_slice() {
                    [
                        Expr::ColumnRef(ColumnRef { id, .. }),
                        Expr::Constant(Constant { scalar, .. }),
                    ]
                    | [
                        Expr::Constant(Constant { scalar, .. }),
                        Expr::ColumnRef(ColumnRef { id, .. }),
                    ] => visitor(id, scalar),
                    _ => {}
                }
            } else if function.signature.name == "and_filters" {
                for arg in args {
                    find_lt_filter(arg, visitor);
                }
            }
        }
        Expr::LambdaFunctionCall(LambdaFunctionCall { args, .. }) => {
            for arg in args {
                find_lt_filter(arg, visitor);
            }
        }
    }
}

fn extract_leveled_strings(
    expr: &Expr<String>,
    level_names: &[&str],
    func_ctx: &FunctionContext,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut res1 = vec![];
    let mut res2 = vec![];
    let leveled_results =
        FilterHelpers::find_leveled_eq_filters(expr, level_names, func_ctx, &BUILTIN_FUNCTIONS)?;

    for (i, scalars) in leveled_results.iter().enumerate() {
        for r in scalars.iter() {
            let e = Expr::Constant(Constant {
                span: None,
                scalar: r.clone(),
                data_type: r.as_ref().infer_data_type(),
            });

            if let Ok(s) = check_string::<usize>(None, func_ctx, &e, &BUILTIN_FUNCTIONS) {
                match i {
                    0 => res1.push(s),
                    1 => res2.push(s),
                    _ => unreachable!(),
                }
            }
        }
    }

    Ok((res1, res2))
}
