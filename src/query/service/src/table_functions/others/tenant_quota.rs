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
use databend_base::non_empty::NonEmptyString;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt32Type;
use databend_common_meta_app::principal::UserOptionFlag;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_storages_factory::Table;
use databend_common_users::UserApiProvider;
use databend_meta_types::MatchSeq;
use fastrace::func_name;

pub struct TenantQuotaTable {
    table_info: TableInfo,
    args: Vec<String>,
}

impl TenantQuotaTable {
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new(
                "max_databases",
                TableDataType::Number(NumberDataType::UInt32),
            ),
            TableField::new(
                "max_tables_per_database",
                TableDataType::Number(NumberDataType::UInt32),
            ),
            TableField::new("max_stages", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new(
                "max_files_per_stage",
                TableDataType::Number(NumberDataType::UInt32),
            ),
        ])
    }

    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.expect_all_positioned(table_func_name, None)?;
        let args = TableArgs::expect_all_strings(args)?;

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("tenant_quota"),
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

        Ok(Arc::new(TenantQuotaTable { table_info, args }))
    }
}

#[async_trait::async_trait]
impl Table for TenantQuotaTable {
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
        let args = self
            .args
            .iter()
            .map(|s| Scalar::String(s.clone()))
            .collect();
        Some(TableArgs::new_positioned(args))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let tenants = self
            .args
            .iter()
            .map(|s| NonEmptyString::new(s.clone()))
            .collect::<std::result::Result<Vec<_>, &'static str>>()
            .map_err(|_e| {
                ErrorCode::TenantIsEmpty("tenant is empty when impl Table for TenantQutaTable")
            })?;

        pipeline.add_source(
            move |output| TenantQuotaSource::create(ctx.clone(), output, tenants.clone()),
            1,
        )?;

        Ok(())
    }
}

struct TenantQuotaSource {
    ctx: Arc<dyn TableContext>,
    args: Vec<NonEmptyString>,
    done: bool,
}

impl TenantQuotaSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args: Vec<NonEmptyString>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, TenantQuotaSource {
            ctx,
            args,
            done: false,
        })
    }
}

impl TenantQuotaSource {
    fn to_block(&self, quota: &TenantQuota) -> Result<DataBlock> {
        Ok(DataBlock::new(
            vec![
                BlockEntry::new_const_column_arg::<UInt32Type>(quota.max_databases, 1),
                BlockEntry::new_const_column_arg::<UInt32Type>(quota.max_tables_per_database, 1),
                BlockEntry::new_const_column_arg::<UInt32Type>(quota.max_stages, 1),
                BlockEntry::new_const_column_arg::<UInt32Type>(quota.max_files_per_stage, 1),
            ],
            1,
        ))
    }
}

/// args:
/// tenant_id: string
/// max_databases: u32
/// max_tables_per_database: u32
/// max_stages: u32
/// max_files_per_stage: u32
#[async_trait::async_trait]
impl AsyncSource for TenantQuotaSource {
    const NAME: &'static str = "tenant_quota";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.done {
            return Ok(None);
        }

        self.done = true;
        let mut tenant = self.ctx.get_tenant();
        let args = &self.args;
        if !args.is_empty() {
            let user_info = self.ctx.get_current_user()?;
            if !user_info.has_option_flag(UserOptionFlag::TenantSetting) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Access denied: '{}' requires user {} option flag",
                    Self::NAME,
                    UserOptionFlag::TenantSetting
                )));
            }
            tenant = Tenant::new_or_err(args[0].clone(), func_name!())?;
        }
        let quota_api = UserApiProvider::instance().tenant_quota_api(&tenant);
        let res = quota_api.get_quota(MatchSeq::GE(0)).await?;
        let mut quota = res.data;

        if args.len() <= 1 {
            return Ok(Some(self.to_block(&quota)?));
        };

        quota.max_databases = args[1].as_str().parse::<u32>()?;
        if let Some(max_tables) = args.get(2) {
            quota.max_tables_per_database = max_tables.as_str().parse::<u32>()?;
        };
        if let Some(max_stages) = args.get(3) {
            quota.max_stages = max_stages.as_str().parse::<u32>()?;
        };
        if let Some(max_files_per_stage) = args.get(4) {
            quota.max_files_per_stage = max_files_per_stage.as_str().parse::<u32>()?
        };

        quota_api
            .set_quota(&quota, MatchSeq::Exact(res.seq))
            .await?;

        Ok(Some(self.to_block(&quota)?))
    }
}

impl TableFunction for TenantQuotaTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
