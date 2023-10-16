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

use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_exception::Result;
use common_expression::DataBlock;
use common_meta_app::schema::TableInfo;
use common_storages_fuse::TableContext;

use crate::table::AsyncSystemTable;

pub struct EncodingInfoTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for EncodingInfoTable {
    const NAME: &'static str = "system.encoding_info";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        todo!()
    }
}

impl EncodingInfoTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        todo!()
    }
}
