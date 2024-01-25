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
use databend_common_meta_app::schema::CreateVirtualColumnReply;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DropVirtualColumnReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::UpdateVirtualColumnReply;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::Location;

#[async_trait::async_trait]
pub trait VirtualColumnHandler: Sync + Send {
    async fn do_create_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply>;

    async fn do_update_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply>;

    async fn do_drop_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply>;

    async fn do_list_virtual_columns(
        &self,
        catalog: Arc<dyn Catalog>,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>>;

    async fn do_refresh_virtual_column(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        virtual_columns: Vec<String>,
        segment_locs: Option<Vec<Location>>,
    ) -> Result<()>;
}

pub struct VirtualColumnHandlerWrapper {
    handler: Box<dyn VirtualColumnHandler>,
}

impl VirtualColumnHandlerWrapper {
    pub fn new(handler: Box<dyn VirtualColumnHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_create_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply> {
        self.handler.do_create_virtual_column(catalog, req).await
    }

    #[async_backtrace::framed]
    pub async fn do_update_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply> {
        self.handler.do_update_virtual_column(catalog, req).await
    }

    #[async_backtrace::framed]
    pub async fn do_drop_virtual_column(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply> {
        self.handler.do_drop_virtual_column(catalog, req).await
    }

    #[async_backtrace::framed]
    pub async fn do_list_virtual_columns(
        &self,
        catalog: Arc<dyn Catalog>,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        self.handler.do_list_virtual_columns(catalog, req).await
    }

    #[async_backtrace::framed]
    pub async fn do_refresh_virtual_column(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        virtual_columns: Vec<String>,
        segment_locs: Option<Vec<Location>>,
    ) -> Result<()> {
        self.handler
            .do_refresh_virtual_column(fuse_table, ctx, virtual_columns, segment_locs)
            .await
    }
}

pub fn get_virtual_column_handler() -> Arc<VirtualColumnHandlerWrapper> {
    GlobalInstance::get()
}
