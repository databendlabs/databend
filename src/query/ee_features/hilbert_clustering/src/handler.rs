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
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::TableSnapshot;

#[async_trait::async_trait]
pub trait HilbertClusteringHandler: Sync + Send {
    async fn do_hilbert_clustering(
        &self,
        table: Arc<dyn Table>,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Option<(ReclusterInfoSideCar, Arc<TableSnapshot>)>>;
}

pub struct HilbertClusteringHandlerWrapper {
    handler: Box<dyn HilbertClusteringHandler>,
}

impl HilbertClusteringHandlerWrapper {
    pub fn new(handler: Box<dyn HilbertClusteringHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_hilbert_clustering(
        &self,
        table: Arc<dyn Table>,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<Option<(ReclusterInfoSideCar, Arc<TableSnapshot>)>> {
        self.handler
            .do_hilbert_clustering(table, ctx, push_downs)
            .await
    }
}

pub fn get_hilbert_clustering_handler() -> Arc<HilbertClusteringHandlerWrapper> {
    GlobalInstance::get()
}
