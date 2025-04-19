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
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::DropTableIndexReq;

#[async_trait::async_trait]
pub trait NgramIndexHandler: Sync + Send {
    async fn do_create_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateTableIndexReq,
    ) -> databend_common_exception::Result<()>;

    async fn do_drop_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropTableIndexReq,
    ) -> databend_common_exception::Result<()>;
}

pub struct NgramIndexHandlerWrapper {
    handler: Box<dyn NgramIndexHandler>,
}

impl NgramIndexHandlerWrapper {
    pub fn new(handler: Box<dyn NgramIndexHandler>) -> Self {
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
}

pub fn get_ngram_index_handler() -> Arc<NgramIndexHandlerWrapper> {
    GlobalInstance::get()
}
