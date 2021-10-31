//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_context::IOContext;
use common_context::TableIOContext;
use common_dal::read_obj;
use common_exception::Result;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::Statistics;

use super::index;
use crate::datasources::table::fuse::FuseTable;
use crate::datasources::table::fuse::MetaInfoReader;

impl FuseTable {
    #[inline]
    pub async fn do_read_partitions(
        &self,
        io_ctx: &TableIOContext,
        push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        match self.snapshot_loc() {
            None => Ok((Statistics::default(), vec![])),
            Some(location) => {
                let schema = self.table_info.schema();
                let da = io_ctx.get_data_accessor()?;
                let snapshot = read_obj(da.clone(), location).await?;
                let meta_reader = MetaInfoReader::new(da, io_ctx.get_runtime());
                let block_metas =
                    index::range_filter(&snapshot, schema, push_downs, meta_reader).await?;
                Ok(self.to_partitions(&block_metas))
            }
        }
    }
}
