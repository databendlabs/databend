// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_materialized_view::MaterializedViewHandler;
use databend_enterprise_materialized_view::MaterializedViewHandlerWrapper;
use databend_meta_client::types::SeqV;
use databend_query::locks::LockManager;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sessions::TableContextTableManagement;

use super::MaterializedViewRefresh;

pub struct RealMaterializedViewHandler;

#[async_trait::async_trait]
impl MaterializedViewHandler for RealMaterializedViewHandler {
    #[async_backtrace::framed]
    async fn get_mv_definition(
        &self,
        catalog: &dyn Catalog,
        tenant: &Tenant,
        mv_table_id: u64,
    ) -> Result<Option<SeqV<MVDefinition>>> {
        catalog.get_mv_definition(tenant, mv_table_id).await
    }

    #[async_backtrace::framed]
    async fn get_active_mv_definition(
        &self,
        catalog: &dyn Catalog,
        tenant: &Tenant,
        source_table_id: u64,
        mv_table_id: u64,
    ) -> Result<Option<SeqV<MVDefinition>>> {
        catalog
            .get_active_mv_definition(tenant, source_table_id, mv_table_id)
            .await
    }

    #[async_backtrace::framed]
    async fn get_mv_current_source_generation(
        &self,
        catalog: &dyn Catalog,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Option<u64>> {
        catalog
            .get_mv_current_source_generation(tenant, source_table_id)
            .await
    }

    #[async_backtrace::framed]
    async fn do_refresh_materialized_view(
        &self,
        ctx: Arc<dyn Any + Send + Sync>,
        table: Arc<dyn Table>,
        catalog: &str,
        database: &str,
        view_name: &str,
    ) -> Result<()> {
        let query_ctx = ctx
            .downcast::<databend_query::sessions::QueryContext>()
            .map_err(|_| {
                ErrorCode::Internal("materialized view refresh requires QueryContext".to_string())
            })?;

        // Refresh consumes one source checkpoint range. Keep the same mandatory non-waiting lock
        // across the complete lifecycle so concurrent refreshes cannot consume the same changes.
        let locked_table_id = table.get_id();
        let table_lock = LockManager::create_table_lock(table.get_table_info().clone())?;
        let _lock_guard = table_lock.try_lock(query_ctx.clone(), false).await?;

        // Reload after acquiring the lock: the table may have been replaced while lock acquisition
        // was in progress, and refresh must use the endpoint protected by this guard.
        query_ctx.evict_table_from_cache(catalog, database, view_name)?;
        let table = query_ctx.get_table(catalog, database, view_name).await?;
        if table.get_id() != locked_table_id {
            return Err(ErrorCode::InvalidMaterializedView(format!(
                "materialized view {}.{} changed while acquiring its refresh lock",
                database, view_name
            )));
        }

        let table = FuseTable::try_from_table(table.as_ref())?;
        if let Some(refresh) =
            MaterializedViewRefresh::create(table, query_ctx, catalog, database, view_name).await?
        {
            refresh.execute().await?;
        }
        Ok(())
    }
}

impl RealMaterializedViewHandler {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Arc::new(MaterializedViewHandlerWrapper::new(Box::new(
            Self,
        ))));
        Ok(())
    }
}
