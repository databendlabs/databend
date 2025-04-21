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

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_virtual_column::VirtualColumnHandler;
use databend_enterprise_virtual_column::VirtualColumnHandlerWrapper;
use databend_storages_common_table_meta::meta::Location;

use crate::storages::fuse::do_refresh_virtual_column;

pub struct RealVirtualColumnHandler {}

#[async_trait::async_trait]
impl VirtualColumnHandler for RealVirtualColumnHandler {
    #[async_backtrace::framed]
    async fn do_create_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateVirtualColumnReq,
    ) -> Result<()> {
        catalog.create_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn do_update_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: UpdateVirtualColumnReq,
    ) -> Result<()> {
        catalog.update_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn do_drop_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropVirtualColumnReq,
    ) -> Result<()> {
        catalog.drop_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn do_list_virtual_columns(
        &self,
        catalog: Arc<dyn Catalog>,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        catalog.list_virtual_columns(req).await
    }

    async fn do_refresh_virtual_column(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
        segment_locs: Option<Vec<Location>>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        do_refresh_virtual_column(ctx, fuse_table, segment_locs, pipeline).await
    }
}

impl RealVirtualColumnHandler {
    pub fn init() -> Result<()> {
        let rm = RealVirtualColumnHandler {};
        let wrapper = VirtualColumnHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
