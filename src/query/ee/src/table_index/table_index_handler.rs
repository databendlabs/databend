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
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_enterprise_table_index::TableIndexHandler;
use databend_enterprise_table_index::TableIndexHandlerWrapper;

pub struct RealTableIndexHandler {}

#[async_trait::async_trait]
impl TableIndexHandler for RealTableIndexHandler {
    #[async_backtrace::framed]
    async fn do_create_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: CreateTableIndexReq,
    ) -> Result<()> {
        catalog.create_table_index(req).await
    }

    #[async_backtrace::framed]
    async fn do_drop_table_index(
        &self,
        catalog: Arc<dyn Catalog>,
        req: DropTableIndexReq,
    ) -> Result<()> {
        catalog.drop_table_index(req).await
    }
}

impl RealTableIndexHandler {
    pub fn init() -> Result<()> {
        let rm = RealTableIndexHandler {};
        let wrapper = TableIndexHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
