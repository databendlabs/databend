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
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_enterprise_hilbert_clustering::HilbertClusteringHandler;
use databend_enterprise_hilbert_clustering::HilbertClusteringHandlerWrapper;
use databend_storages_common_table_meta::meta::TableSnapshot;

pub struct RealHilbertClusteringHandler {}

#[async_trait::async_trait]
impl HilbertClusteringHandler for RealHilbertClusteringHandler {
    #[async_backtrace::framed]
    async fn do_hilbert_clustering(
        &self,
        table: Arc<dyn Table>,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        limit: Option<usize>,
    ) -> Result<Option<(ReclusterInfoSideCar, Arc<TableSnapshot>)>> {
        if table.cluster_key_meta().is_none() {
            return Ok(None);
        }
        
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
            // no snapshot, no recluster.
            return Ok(None);
        };

        let segment_locations = snapshot.segments.clone();
        let segment_locations = create_segment_location_vector(segment_locations, None);
        
        
        todo!()
    }
}

impl RealHilbertClusteringHandler {
    pub fn init() -> Result<()> {
        let handler = RealHilbertClusteringHandler {};
        let wrapper = HilbertClusteringHandlerWrapper::new(Box::new(handler));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
