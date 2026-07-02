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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::ColumnRange;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::ReusablePrunedMetas;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline::core::Pipeline;
use databend_storages_common_table_meta::meta::ClusterKey;

use crate::databases::DatabaseContext;
use crate::share::ShareTableContext;
use crate::share::share_service;

pub struct SharedTable {
    exposed_info: TableInfo,
    provider_table: Arc<dyn Table>,
}

impl SharedTable {
    pub async fn try_create(
        ctx: DatabaseContext,
        consumer_database: &str,
        table_context: &ShareTableContext,
        provider_info: TableInfo,
    ) -> Result<Arc<dyn Table>> {
        if !provider_info.meta.engine.eq_ignore_ascii_case("FUSE") {
            return Err(ErrorCode::Unimplemented(format!(
                "Shared table only supports Fuse provider tables, got '{}'",
                provider_info.meta.engine
            )));
        }

        let credential = share_service()
            .get_share_credential(&table_context.provider_tenant, table_context.share_id)
            .await?;

        let mut provider_info_with_credential = provider_info.clone();
        provider_info_with_credential.meta.storage_params = Some(credential.storage_params);
        let provider_table = ctx.storage_factory.get_table(
            &provider_info_with_credential,
            ctx.disable_table_info_refresh,
        )?;

        let mut exposed_info = provider_info;
        exposed_info.name = table_context.provider_table.clone();
        exposed_info.desc = format!("'{}'.'{}'", consumer_database, table_context.provider_table);
        exposed_info.meta.storage_params = None;

        Ok(Arc::new(Self {
            exposed_info,
            provider_table,
        }))
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

    async fn read_partitions(
        &self,
        ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
        push_downs: Option<PushDownInfo>,
        dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
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
        self.provider_table.column_statistics_provider(ctx).await
    }

    async fn accurate_columns_ranges(
        &self,
        ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
        column_ids: &[ColumnId],
    ) -> Result<Option<HashMap<ColumnId, ColumnRange>>> {
        self.provider_table
            .accurate_columns_ranges(ctx, column_ids)
            .await
    }

    fn result_can_be_cached(&self) -> bool {
        self.provider_table.result_can_be_cached()
    }

    fn is_read_only(&self) -> bool {
        true
    }
}
