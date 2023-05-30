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

use common_base::base::GlobalInstance;
use common_catalog::catalog::Catalog;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_meta_app::schema::CreateVirtualColumnReply;
use common_meta_app::schema::CreateVirtualColumnReq;
use common_meta_app::schema::DropVirtualColumnReply;
use common_meta_app::schema::DropVirtualColumnReq;
use common_meta_app::schema::ListVirtualColumnsReq;
use common_meta_app::schema::UpdateVirtualColumnReply;
use common_meta_app::schema::UpdateVirtualColumnReq;
use common_meta_app::schema::VirtualColumnMeta;
use common_storages_fuse::FuseTable;
use virtual_columns_handler::VirtualColumnsHandler;
use virtual_columns_handler::VirtualColumnsHandlerWrapper;

use crate::storages::fuse::do_generate_virtual_columns;

pub struct RealVirtualColumnsHandler {}

#[async_trait::async_trait]
impl VirtualColumnsHandler for RealVirtualColumnsHandler {
    #[async_backtrace::framed]
    async fn do_create_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply> {
        catalog.create_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn do_update_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply> {
        catalog.update_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn do_drop_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply> {
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

    async fn do_generate_virtual_columns(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        virtual_columns: Vec<String>,
    ) -> Result<()> {
        do_generate_virtual_columns(fuse_table, ctx, virtual_columns).await
    }
}

impl RealVirtualColumnsHandler {
    pub fn init() -> Result<()> {
        let rm = RealVirtualColumnsHandler {};
        let wrapper = VirtualColumnsHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
