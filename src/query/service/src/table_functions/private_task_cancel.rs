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
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::StringType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_users::UserApiProvider;

use crate::task::TaskService;

const FUNCTION_NAME: &str = "user_task_cancel_ongoing_executions";
const CANCEL_REASON: &str =
    "task execution was cancelled by SYSTEM$USER_TASK_CANCEL_ONGOING_EXECUTIONS";

pub struct PrivateTaskCancelTable {
    table_info: TableInfo,
    task_name: String,
}

impl PrivateTaskCancelTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.expect_all_positioned(table_func_name, Some(1))?;
        let task_name = args[0].as_string().cloned().ok_or_else(|| {
            ErrorCode::BadArguments(format!("{table_func_name} expects a string task name"))
        })?;

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from(FUNCTION_NAME),
            meta: TableMeta {
                schema: Self::schema(),
                engine: String::from(table_func_name),
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            task_name,
        }))
    }

    fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![TableField::new("result", TableDataType::String)])
    }
}

#[async_trait::async_trait]
impl Table for PrivateTaskCancelTable {
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
        Some(TableArgs::new_positioned(vec![Scalar::String(
            self.task_name.clone(),
        )]))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| PrivateTaskCancelSource::create(ctx.clone(), output, self.task_name.clone()),
            1,
        )?;
        Ok(())
    }
}

impl TableFunction for PrivateTaskCancelTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct PrivateTaskCancelSource {
    ctx: Arc<dyn TableContext>,
    task_name: String,
    finished: bool,
}

impl PrivateTaskCancelSource {
    fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        task_name: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            ctx,
            task_name,
            finished: false,
        })
    }

    async fn cancel_task_runs(&self) -> Result<String> {
        let task = UserApiProvider::instance()
            .task_api(&self.ctx.get_tenant())
            .describe_task(&self.task_name)
            .await??
            .ok_or_else(|| ErrorCode::UnknownTask(self.task_name.clone()))?;

        let owner = task.owner.clone();
        let available_roles = self.ctx.get_all_available_roles().await?;
        if !available_roles.iter().any(|role| role.identity() == owner) {
            let user = self
                .ctx
                .get_current_user()?
                .identity()
                .display()
                .to_string();
            return Err(ErrorCode::PermissionDenied(format!(
                "Permission denied: OWNERSHIP is required on task {} for user {}",
                task.task_name, user
            )));
        }

        let cancelled_rows = TaskService::instance()
            .cancel_open_task_runs(&task.task_name, task.task_id, CANCEL_REASON)
            .await?;

        if cancelled_rows == 0 {
            Ok(format!(
                "No ongoing execution found for task {}.",
                task.task_name
            ))
        } else {
            Ok(format!(
                "Cancelled {} ongoing execution(s) for task {}.",
                cancelled_rows, task.task_name
            ))
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for PrivateTaskCancelSource {
    const NAME: &'static str = FUNCTION_NAME;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        self.finished = true;
        let result = self.cancel_task_runs().await?;
        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(vec![result]),
        ])))
    }
}
