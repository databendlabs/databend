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

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::types::SeqV;

#[async_trait::async_trait]
pub trait MaterializedViewHandler: Sync + Send {
    async fn get_mv_definition(
        &self,
        catalog: &dyn Catalog,
        tenant: &Tenant,
        mv_table_id: u64,
    ) -> Result<Option<SeqV<MVDefinition>>>;

    async fn get_active_mv_definition(
        &self,
        catalog: &dyn Catalog,
        tenant: &Tenant,
        source_table_id: u64,
        mv_table_id: u64,
    ) -> Result<Option<SeqV<MVDefinition>>>;

    async fn get_mv_current_source_generation(
        &self,
        catalog: &dyn Catalog,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Option<u64>>;

    async fn do_refresh_materialized_view(
        &self,
        ctx: Arc<dyn Any + Send + Sync>,
        table: Arc<dyn Table>,
        catalog: &str,
        database: &str,
        view_name: &str,
    ) -> Result<()>;
}

pub struct MaterializedViewHandlerWrapper {
    handler: Box<dyn MaterializedViewHandler>,
}

impl MaterializedViewHandlerWrapper {
    pub fn new(handler: Box<dyn MaterializedViewHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn get_mv_definition(
        &self,
        catalog: &dyn Catalog,
        tenant: &Tenant,
        mv_table_id: u64,
    ) -> Result<Option<SeqV<MVDefinition>>> {
        self.handler
            .get_mv_definition(catalog, tenant, mv_table_id)
            .await
    }

    #[async_backtrace::framed]
    pub async fn get_active_mv_definition(
        &self,
        catalog: &dyn Catalog,
        tenant: &Tenant,
        source_table_id: u64,
        mv_table_id: u64,
    ) -> Result<Option<SeqV<MVDefinition>>> {
        self.handler
            .get_active_mv_definition(catalog, tenant, source_table_id, mv_table_id)
            .await
    }

    #[async_backtrace::framed]
    pub async fn get_mv_current_source_generation(
        &self,
        catalog: &dyn Catalog,
        tenant: &Tenant,
        source_table_id: u64,
    ) -> Result<Option<u64>> {
        self.handler
            .get_mv_current_source_generation(catalog, tenant, source_table_id)
            .await
    }

    #[async_backtrace::framed]
    pub async fn do_refresh_materialized_view(
        &self,
        ctx: Arc<dyn Any + Send + Sync>,
        table: Arc<dyn Table>,
        catalog: &str,
        database: &str,
        view_name: &str,
    ) -> Result<()> {
        self.handler
            .do_refresh_materialized_view(ctx, table, catalog, database, view_name)
            .await
    }
}

pub fn get_materialized_view_handler() -> std::sync::Arc<MaterializedViewHandlerWrapper> {
    GlobalInstance::get()
}
