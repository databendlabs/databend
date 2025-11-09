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
use databend_common_storages_fuse::FuseTable;

#[async_trait::async_trait]
pub trait VirtualColumnHandler: Sync + Send {
    async fn do_refresh_virtual_column(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
        pipeline: &mut Pipeline,
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
    pub async fn do_refresh_virtual_column(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        self.handler
            .do_refresh_virtual_column(ctx, fuse_table, pipeline)
            .await
    }
}

pub fn get_virtual_column_handler() -> Arc<VirtualColumnHandlerWrapper> {
    GlobalInstance::get()
}
