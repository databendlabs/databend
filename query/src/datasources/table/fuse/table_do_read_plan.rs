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

use common_catalog::IOContext;
use common_catalog::TableIOContext;
use common_exception::Result;
use common_planners::Extras;
use common_planners::ReadDataSourcePlan;

use super::util;
use crate::datasources::table::fuse::FuseTable;
use crate::datasources::table::fuse::MetaInfoReader;

impl FuseTable {
    #[inline]
    pub fn do_read_plan(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: Option<Extras>,
    ) -> Result<ReadDataSourcePlan> {
        let tbl_snapshot = self.table_snapshot(io_ctx.clone())?;
        if let Some(snapshot) = tbl_snapshot {
            let da = io_ctx.get_data_accessor()?;
            let meta_reader = MetaInfoReader::new(da, io_ctx.get_runtime());
            let block_locations = util::range_filter(&snapshot, &push_downs, meta_reader)?;
            let (statistics, parts) = self.to_partitions(&block_locations);
            let plan = ReadDataSourcePlan {
                table_info: self.tbl_info.clone(),
                parts,
                statistics,
                description: "".to_string(),
                scan_plan: Default::default(),
                tbl_args: None,
                push_downs,
            };
            Ok(plan)
        } else {
            self.empty_read_source_plan()
        }
    }
}
