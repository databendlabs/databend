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
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateTableIndexReply;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::DropTableIndexReply;
use databend_common_meta_app::schema::DropTableIndexReq;

#[async_trait::async_trait]
pub trait InvertedIndexHandler: Sync + Send {
    async fn do_create_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateTableIndexReq,
    ) -> Result<CreateTableIndexReply>;

    async fn do_drop_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropTableIndexReq,
    ) -> Result<DropTableIndexReply>;
}

pub struct InvertedIndexHandlerWrapper {
    handler: Box<dyn InvertedIndexHandler>,
}

impl InvertedIndexHandlerWrapper {
    pub fn new(handler: Box<dyn InvertedIndexHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_create_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateTableIndexReq,
    ) -> Result<CreateTableIndexReply> {
        self.handler.do_create_table_index(catalog, req).await
    }

    #[async_backtrace::framed]
    pub async fn do_drop_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropTableIndexReq,
    ) -> Result<DropTableIndexReply> {
        self.handler.do_drop_table_index(catalog, req).await
    }
}

pub fn get_inverted_index_handler() -> Arc<InvertedIndexHandlerWrapper> {
    GlobalInstance::get()
}
