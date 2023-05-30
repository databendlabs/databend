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

#[async_trait::async_trait]
pub trait VirtualColumnsHandler: Sync + Send {
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

    async fn do_generate_virtual_columns(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        virtual_columns: Vec<String>,
    ) -> Result<()>;
}

pub struct VirtualColumnsHandlerWrapper {
    handler: Box<dyn VirtualColumnsHandler>,
}

impl VirtualColumnsHandlerWrapper {
    pub fn new(handler: Box<dyn VirtualColumnsHandler>) -> Self {
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
    pub async fn do_generate_virtual_columns(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        virtual_columns: Vec<String>,
    ) -> Result<()> {
        self.handler
            .do_generate_virtual_columns(fuse_table, ctx, virtual_columns)
            .await
    }
}

pub fn get_virtual_columns_handler() -> Arc<VirtualColumnsHandlerWrapper> {
    GlobalInstance::get()
}
