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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::ColumnRange;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::ReusablePrunedMetas;
use databend_common_catalog::table::Table;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_meta_api::TableApi;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_common_pipeline::core::Pipeline;
use databend_storages_common_table_meta::meta::ClusterKey;

use crate::databases::DatabaseContext;
use crate::meta_service_error;
use crate::share::ShareMgr;
use crate::share::ShareTableContext;
use crate::share::resolve_share_storage_params;

pub struct SharedTable {
    consumer: Tenant,
    meta: MetaStore,
    share_mgr: ShareMgr,
    share_context: ShareTableContext,
    exposed_info: TableInfo,
    execution_info: TableInfo,
    provider_table: Arc<dyn Table>,
}

impl SharedTable {
    pub async fn try_create(
        ctx: DatabaseContext,
        consumer: Tenant,
        consumer_database: &str,
        table_context: &ShareTableContext,
        provider_info: TableInfo,
    ) -> Result<Arc<dyn Table>> {
        let provider_storage = provider_info
            .meta
            .storage_params
            .clone()
            .unwrap_or_else(|| GlobalConfig::instance().storage.params.clone());
        let execution_storage = resolve_share_storage_params(
            &Tenant::new_literal(&table_context.binding.provider_tenant),
            &table_context.connection,
            provider_storage,
        )
        .await?;

        let mut exposed_info = provider_info;
        exposed_info.name = table_context.provider_table.clone();
        exposed_info.desc = format!("'{}'.'{}'", consumer_database, table_context.provider_table);
        exposed_info.meta.storage_params = None;

        let mut execution_info = exposed_info.clone();
        execution_info.meta.storage_params = Some(execution_storage);
        let meta = ctx.meta.clone();
        let share_mgr = ShareMgr::create(Arc::new(meta.clone()));
        let share_context = table_context.clone();
        let provider_table = ctx
            .storage_factory
            .get_table(&execution_info, ctx.disable_table_info_refresh)?;

        Ok(Arc::new(Self {
            consumer,
            meta,
            share_mgr,
            share_context,
            exposed_info,
            execution_info,
            provider_table,
        }))
    }

    async fn revalidate(&self) -> Result<()> {
        let current = self
            .share_mgr
            .resolve_shared_table(
                &self.consumer,
                &self.share_context.binding,
                &self.share_context.provider_table,
            )
            .await?;
        if current != self.share_context {
            return Err(ErrorCode::InvalidOperation(
                "Shared table grant or credential changed; retry the query",
            ));
        }

        let table_name = self
            .meta
            .get_pb(&TableIdToName {
                table_id: self.share_context.provider_table_id,
            })
            .await
            .map_err(meta_service_error)?
            .map(|name| name.data.table_name)
            .ok_or_else(|| {
                ErrorCode::InvalidOperation(
                    "Shared provider table was dropped or recreated; retry the query",
                )
            })?;
        let table_name =
            DBIdTableName::new(self.share_context.binding.provider_database_id, table_name);
        let current_table = self
            .meta
            .get_table_in_db(&table_name)
            .await
            .map_err(meta_service_error)?;
        let Some(current_table) = current_table else {
            return Err(ErrorCode::InvalidOperation(
                "Shared provider table was dropped or recreated; retry the query",
            ));
        };
        let (_, current_id, current_meta) = current_table.unpack();
        if current_id.table_id != self.share_context.provider_table_id {
            return Err(ErrorCode::InvalidOperation(
                "Shared provider table was dropped or recreated; retry the query",
            ));
        }

        let provider_storage = current_meta
            .data
            .storage_params
            .clone()
            .unwrap_or_else(|| GlobalConfig::instance().storage.params.clone());
        let current_storage = resolve_share_storage_params(
            &Tenant::new_literal(&self.share_context.binding.provider_tenant),
            &self.share_context.connection,
            provider_storage,
        )
        .await?;
        if self.execution_info.meta.storage_params.as_ref() != Some(&current_storage) {
            return Err(ErrorCode::InvalidOperation(
                "Shared table location or connection changed; retry the query",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Table for SharedTable {
    fn name(&self) -> &str {
        &self.exposed_info.name
    }

    fn supported_internal_column(&self, column_id: ColumnId) -> bool {
        self.provider_table.supported_internal_column(column_id)
    }

    fn supported_lazy_materialize(&self) -> bool {
        false
    }

    fn support_column_projection(&self) -> bool {
        self.provider_table.support_column_projection()
    }

    fn has_exact_total_row_count(&self) -> bool {
        self.provider_table.has_exact_total_row_count()
    }

    fn cluster_key_meta(&self) -> Option<ClusterKey> {
        self.provider_table.cluster_key_meta()
    }

    fn support_prewhere(&self) -> bool {
        self.provider_table.support_prewhere()
    }

    fn support_index(&self) -> bool {
        self.provider_table.support_index()
    }

    fn storage_format_as_parquet(&self) -> bool {
        self.provider_table.storage_format_as_parquet()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.exposed_info
    }

    fn plan_can_be_cached(&self) -> bool {
        // Share grants and credentials can change without changing the provider
        // table snapshot. A cached plan would retain an obsolete SharedTable and
        // fail every retry during execution-time revalidation.
        false
    }

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::TableSource(self.execution_info.clone())
    }

    async fn read_partitions(
        &self,
        ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
        push_downs: Option<PushDownInfo>,
        dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.revalidate().await?;
        self.provider_table
            .read_partitions(ctx, push_downs, dry_run)
            .await
    }

    async fn read_partitions_with_reusable_pruned_metas(
        &self,
        ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
        push_downs: Option<PushDownInfo>,
        dry_run: bool,
        reusable_pruned_metas: Option<ReusablePrunedMetas>,
    ) -> Result<(PartStatistics, Partitions, Option<ReusablePrunedMetas>)> {
        self.revalidate().await?;
        self.provider_table
            .read_partitions_with_reusable_pruned_metas(
                ctx,
                push_downs,
                dry_run,
                reusable_pruned_metas,
            )
            .await
    }

    fn read_data(
        &self,
        ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        self.provider_table
            .read_data(ctx, plan, pipeline, put_cache)
    }

    fn build_prune_pipeline(
        &self,
        table_ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
        plan: &DataSourcePlan,
        source_pipeline: &mut Pipeline,
        plan_id: u32,
    ) -> Result<Option<Pipeline>> {
        self.provider_table
            .build_prune_pipeline(table_ctx, plan, source_pipeline, plan_id)
    }

    async fn column_statistics_provider(
        &self,
        ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        self.revalidate().await?;
        self.provider_table.column_statistics_provider(ctx).await
    }

    async fn accurate_columns_ranges(
        &self,
        ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
        column_ids: &[ColumnId],
    ) -> Result<Option<HashMap<ColumnId, ColumnRange>>> {
        self.revalidate().await?;
        self.provider_table
            .accurate_columns_ranges(ctx, column_ids)
            .await
    }

    fn result_can_be_cached(&self) -> bool {
        false
    }

    fn is_read_only(&self) -> bool {
        true
    }
}
