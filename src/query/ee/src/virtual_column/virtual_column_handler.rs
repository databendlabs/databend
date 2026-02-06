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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline::core::Pipeline;
use databend_common_sql::plans::RefreshSelection;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_virtual_column::VirtualColumnHandler;
use databend_enterprise_virtual_column::VirtualColumnHandlerWrapper;
use databend_enterprise_virtual_column::VirtualColumnRefreshResult;

use crate::storages::fuse::operations::virtual_columns::commit_refresh_virtual_column;
use crate::storages::fuse::operations::virtual_columns::prepare_refresh_virtual_column;

pub struct RealVirtualColumnHandler {}

#[async_trait::async_trait]
impl VirtualColumnHandler for RealVirtualColumnHandler {
    async fn prepare_refresh_virtual_column(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
        limit: Option<u64>,
        overwrite: bool,
        selection: Option<RefreshSelection>,
    ) -> Result<Vec<VirtualColumnRefreshResult>> {
        prepare_refresh_virtual_column(ctx, fuse_table, limit, overwrite, selection).await
    }

    async fn commit_refresh_virtual_column(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
        pipeline: &mut Pipeline,
        results: Vec<VirtualColumnRefreshResult>,
    ) -> Result<u64> {
        commit_refresh_virtual_column(ctx, fuse_table, pipeline, results).await
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
