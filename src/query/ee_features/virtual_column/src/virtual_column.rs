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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline::core::Pipeline;
use databend_common_sql::plans::RefreshSelection;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::DraftVirtualBlockMeta;
use databend_storages_common_table_meta::meta::RawBlockHLL;

#[derive(Clone, Debug)]
pub struct VirtualColumnRefreshResult {
    pub block_location: String,
    pub draft_virtual_block_meta: DraftVirtualBlockMeta,
    pub column_hlls: Option<RawBlockHLL>,
}

#[async_trait::async_trait]
pub trait VirtualColumnHandler: Sync + Send {
    async fn prepare_refresh_virtual_column(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
        limit: Option<u64>,
        overwrite: bool,
        selection: Option<RefreshSelection>,
    ) -> Result<Vec<VirtualColumnRefreshResult>>;

    async fn commit_refresh_virtual_column(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
        pipeline: &mut Pipeline,
        results: Vec<VirtualColumnRefreshResult>,
    ) -> Result<u64>;
}

pub struct VirtualColumnHandlerWrapper {
    handler: Box<dyn VirtualColumnHandler>,
}

impl VirtualColumnHandlerWrapper {
    pub fn new(handler: Box<dyn VirtualColumnHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn prepare_refresh_virtual_column(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
        limit: Option<u64>,
        overwrite: bool,
        selection: Option<RefreshSelection>,
    ) -> Result<Vec<VirtualColumnRefreshResult>> {
        self.handler
            .prepare_refresh_virtual_column(ctx, fuse_table, limit, overwrite, selection)
            .await
    }

    pub async fn commit_refresh_virtual_column(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
        pipeline: &mut Pipeline,
        results: Vec<VirtualColumnRefreshResult>,
    ) -> Result<u64> {
        self.handler
            .commit_refresh_virtual_column(ctx, fuse_table, pipeline, results)
            .await
    }
}

pub fn get_virtual_column_handler() -> Arc<VirtualColumnHandlerWrapper> {
    GlobalInstance::get()
}
