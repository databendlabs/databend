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

use std::sync::Arc;

use common_context::IOContext;
use common_context::TableIOContext;
use common_exception::Result;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::Statistics;

use super::util;
use crate::datasources::table::fuse::FuseTable;
use crate::datasources::table::fuse::MetaInfoReader;

impl FuseTable {
    #[inline]
    pub fn do_read_partitions(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let tbl_snapshot = self.table_snapshot(io_ctx.clone())?;
        if let Some(snapshot) = tbl_snapshot {
            let da = io_ctx.get_data_accessor()?;
            let meta_reader = MetaInfoReader::new(da, io_ctx.get_runtime());
            let block_locations = util::range_filter(&snapshot, &push_downs, meta_reader)?;
            let (statistics, parts) = self.to_partitions(&block_locations);
            Ok((statistics, parts))
        } else {
            Ok((Statistics::default(), vec![]))
        }
    }
}
