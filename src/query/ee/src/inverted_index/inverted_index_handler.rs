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
use databend_common_expression::DataSchema;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::DropIndexReply;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_inverted_index::InvertedIndexHandler;
use databend_enterprise_inverted_index::InvertedIndexHandlerWrapper;
use databend_storages_common_table_meta::meta::Location;

use super::indexer::Indexer;

pub struct RealInvertedIndexHandler {}

#[async_trait::async_trait]
impl InvertedIndexHandler for RealInvertedIndexHandler {
    #[async_backtrace::framed]
    async fn do_create_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateIndexReq,
    ) -> Result<CreateIndexReply> {
        catalog.create_index(req).await
    }

    #[async_backtrace::framed]
    async fn do_drop_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropIndexReq,
    ) -> Result<DropIndexReply> {
        catalog.drop_index(req).await
    }

    #[async_backtrace::framed]
    async fn do_get_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: GetIndexReq,
    ) -> Result<GetIndexReply> {
        catalog.get_index(req).await
    }

    #[async_backtrace::framed]
    async fn do_update_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: UpdateIndexReq,
    ) -> Result<UpdateIndexReply> {
        catalog.update_index(req).await
    }

    #[async_backtrace::framed]
    async fn do_refresh_index(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        schema: DataSchema,
        segment_locs: Option<Vec<Location>>,
    ) -> Result<String> {
        let indexer = Indexer::new();
        indexer.index(fuse_table, ctx, schema, segment_locs).await
    }
}

impl RealInvertedIndexHandler {
    pub fn init() -> Result<()> {
        let rm = RealInvertedIndexHandler {};
        let wrapper = InvertedIndexHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
