// Copyright 2024 Datafuse Labs
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
use databend_common_expression::DataSchema;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::Location;

#[async_trait::async_trait]
pub trait InvertedIndexHandler: Sync + Send {
    async fn do_refresh_index(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        schema: DataSchema,
        segment_locs: Option<Vec<Location>>,
    ) -> Result<String>;
}

pub struct InvertedIndexHandlerWrapper {
    handler: Box<dyn InvertedIndexHandler>,
}

impl InvertedIndexHandlerWrapper {
    pub fn new(handler: Box<dyn InvertedIndexHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_refresh_index(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        schema: DataSchema,
        segment_locs: Option<Vec<Location>>,
    ) -> Result<String> {
        self.handler
            .do_refresh_index(fuse_table, ctx, schema, segment_locs)
            .await
    }
}

pub fn get_inverted_index_handler() -> Arc<InvertedIndexHandlerWrapper> {
    GlobalInstance::get()
}
