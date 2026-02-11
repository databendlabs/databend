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
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::EnableTaskDependentsRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
pub use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_storages_factory::Table;

pub struct TaskDependentsEnableTable {
    task_name: String,
    table_info: TableInfo,
}

impl TaskDependentsEnableTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.expect_all_positioned(table_func_name, Some(1))?;
        let task_name = args[0].as_string().unwrap();

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("task_dependents_enable"),
            meta: TableMeta {
                schema: TableSchemaRefExt::create_dummy(),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(TaskDependentsEnableTable {
            table_info,
            task_name: task_name.to_string(),
        }))
    }
}

#[async_trait::async_trait]
impl Table for TaskDependentsEnableTable {
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
            |output| {
                TaskDependentsEnableSource::create(ctx.clone(), output, self.task_name.clone())
            },
            1,
        )?;

        Ok(())
    }
}

struct TaskDependentsEnableSource {
    task_name: String,
    ctx: Arc<dyn TableContext>,
}

impl TaskDependentsEnableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        task_name: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(
            ctx.get_scan_progress(),
            output,
            TaskDependentsEnableSource { ctx, task_name },
        )
    }
    fn build_request(&self) -> EnableTaskDependentsRequest {
        EnableTaskDependentsRequest {
            task_name: self.task_name.clone(),
            tenant_id: self.ctx.get_tenant().tenant_name().to_string(),
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for TaskDependentsEnableSource {
    const NAME: &'static str = "task_dependents_enable";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
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
        let req = make_request(self.build_request(), cfg);
        cloud_api
            .get_task_client()
            .enable_task_dependents(req)
            .await?;
        Ok(None)
    }
}

impl TableFunction for TaskDependentsEnableTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
