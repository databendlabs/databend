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

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_pipeline_core::Pipeline;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::Location;

#[async_trait::async_trait]
pub trait TableIndexHandler: Sync + Send {
    async fn do_create_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateTableIndexReq,
    ) -> Result<()>;

    async fn do_drop_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropTableIndexReq,
    ) -> Result<()>;

    async fn do_refresh_table_index(
        &self,
        index_ty: TableIndexType,
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        index_name: String,
        segment_locs: Option<Vec<Location>>,
        pipeline: &mut Pipeline,
    ) -> Result<()>;
}

pub struct TableIndexHandlerWrapper {
    handler: Box<dyn TableIndexHandler>,
}

impl TableIndexHandlerWrapper {
    pub fn new(handler: Box<dyn TableIndexHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_create_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateTableIndexReq,
    ) -> Result<()> {
        self.handler.do_create_table_index(catalog, req).await
    }

    #[async_backtrace::framed]
    pub async fn do_drop_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropTableIndexReq,
    ) -> Result<()> {
        self.handler.do_drop_table_index(catalog, req).await
    }

    #[async_backtrace::framed]
    pub async fn do_refresh_table_index(
        &self,
        index_ty: TableIndexType,
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        index_name: String,
        segment_locs: Option<Vec<Location>>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.handler
            .do_refresh_table_index(index_ty, table, ctx, index_name, segment_locs, pipeline)
            .await
    }
}

pub fn get_table_index_handler() -> Arc<TableIndexHandlerWrapper> {
    GlobalInstance::get()
}
