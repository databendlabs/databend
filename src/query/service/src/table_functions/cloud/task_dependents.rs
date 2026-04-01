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
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::GetTaskDependentsRequest;
use databend_common_cloud_control::pb::Task;
use databend_common_cloud_control::task_utils;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;

pub struct TaskDependentsTable {
    table_info: TableInfo,
    args_parsed: TaskDependentsParsed,
    table_args: TableArgs,
}

impl TaskDependentsTable {
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new("name", TableDataType::String),
            TableField::new("owner", TableDataType::String),
            TableField::new("comment", TableDataType::String),
            TableField::new("warehouse", TableDataType::String),
            TableField::new("schedule", TableDataType::String),
            TableField::new(
                "predecessors",
                TableDataType::Array(Box::new(TableDataType::String)),
            ),
            TableField::new("state", TableDataType::String),
            TableField::new("definition", TableDataType::String),
            TableField::new("condition", TableDataType::String),
        ])
    }

    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> databend_common_exception::Result<Arc<dyn TableFunction>> {
        let args_parsed = TaskDependentsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("task_dependents"),
            meta: TableMeta {
                schema: Self::schema(),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(TaskDependentsTable {
            table_info,
            args_parsed,
            table_args,
        }))
    }
}

#[async_trait::async_trait]
impl Table for TaskDependentsTable {
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
    ) -> databend_common_exception::Result<(PartStatistics, Partitions)> {
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
    ) -> databend_common_exception::Result<()> {
        pipeline.add_source(
            |output| {
                TaskDependentsSource::create(
                    ctx.clone(),
                    output,
                    self.args_parsed.task_name.clone(),
                    self.args_parsed.recursive,
                )
            },
            1,
        )?;

        Ok(())
    }
}

struct TaskDependentsSource {
    is_finished: bool,
    task_name: String,
    recursive: bool,
    ctx: Arc<dyn TableContext>,
}

impl TaskDependentsSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        task_name: String,
        recursive: bool,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, TaskDependentsSource {
            ctx,
            task_name,
            recursive,
            is_finished: false,
        })
    }
    fn build_request(&self) -> GetTaskDependentsRequest {
        GetTaskDependentsRequest {
            task_name: self.task_name.clone(),
            tenant_id: self.ctx.get_tenant().tenant_name().to_string(),
            recursive: self.recursive,
        }
    }
    fn to_block(&self, tasks: &Vec<Task>) -> databend_common_exception::Result<DataBlock> {
        let mut created_on: Vec<i64> = Vec::with_capacity(tasks.len());
        let mut name: Vec<String> = Vec::with_capacity(tasks.len());
        let mut owner: Vec<String> = Vec::with_capacity(tasks.len());
        let mut comment: Vec<Option<String>> = Vec::with_capacity(tasks.len());
        let mut warehouse: Vec<Option<String>> = Vec::with_capacity(tasks.len());
        let mut schedule: Vec<Option<String>> = Vec::with_capacity(tasks.len());
        let mut predecessors: Vec<Vec<String>> = Vec::with_capacity(tasks.len());

        let mut state: Vec<String> = Vec::with_capacity(tasks.len());
        let mut definition: Vec<String> = Vec::with_capacity(tasks.len());
        let mut condition_text: Vec<String> = Vec::with_capacity(tasks.len());

        for task in tasks {
            let task = task.clone();
            let tsk: task_utils::Task = task.try_into()?;
            created_on.push(tsk.created_at.timestamp_micros());
            name.push(tsk.task_name.clone());
            owner.push(tsk.owner.clone());
            comment.push(tsk.comment.clone());
            warehouse.push(tsk.warehouse_options.and_then(|s| s.warehouse.clone()));
            schedule.push(tsk.schedule_options.clone());
            predecessors.push(tsk.after.clone());
            state.push(tsk.status.to_string());
            definition.push(tsk.query_text.clone());
            condition_text.push(tsk.condition_text.clone());
        }

        Ok(DataBlock::new_from_columns(vec![
            TimestampType::from_data(created_on),
            StringType::from_data(name),
            StringType::from_data(owner),
            StringType::from_opt_data(comment),
            StringType::from_opt_data(warehouse),
            StringType::from_opt_data(schedule),
            ArrayType::upcast_column_with_type(
                ArrayType::<StringType>::column_from_iter(
                    predecessors
                        .into_iter()
                        .map(|children| StringType::column_from_iter(children.into_iter(), &[])),
                    &[],
                ),
                &DataType::Array(Box::new(DataType::String)),
            ),
            StringType::from_data(state),
            StringType::from_data(definition),
            StringType::from_data(condition_text),
        ]))
    }
}

#[async_trait::async_trait]
impl AsyncSource for TaskDependentsSource {
    const NAME: &'static str = "task_dependents";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> databend_common_exception::Result<Option<DataBlock>> {
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
                "cannot create task without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }
        let cloud_api = CloudControlApiProvider::instance();
        let tenant = self.ctx.get_tenant();
        let user = self
            .ctx
            .get_current_user()?
            .identity()
            .display()
            .to_string();
        let query_id = self.ctx.get_id();

        let cfg = build_client_config(
            tenant.tenant_name().to_string(),
            user,
            query_id,
            cloud_api.get_timeout(),
        );

        let dependents = cloud_api
            .get_task_client()
            .get_task_dependents(make_request(self.build_request(), cfg))
            .await?;

        Ok(Some(self.to_block(dependents.task.as_ref())?))
    }
}

impl TableFunction for TaskDependentsTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[derive(Clone)]
pub(crate) struct TaskDependentsParsed {
    pub(crate) task_name: String,
    pub(crate) recursive: bool,
}

impl TaskDependentsParsed {
    pub fn parse(table_args: &TableArgs) -> databend_common_exception::Result<Self> {
        let args = table_args.expect_all_named("task_dependents")?;

        let mut task_name = None;
        let mut recursive = None;
        for (k, v) in &args {
            match k.to_lowercase().as_str() {
                "task_name" => task_name = v.as_string().cloned(),
                "recursive" => recursive = v.as_boolean().cloned(),
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "unknown param {} for {}",
                        k, "task_dependents"
                    )));
                }
            }
        }

        if task_name.is_none() {
            return Err(ErrorCode::BadArguments(format!(
                "task_name must be specified for {}",
                "task_dependents"
            )));
        }

        Ok(Self {
            task_name: task_name.unwrap(),
            recursive: recursive.unwrap_or_default(),
        })
    }
}
