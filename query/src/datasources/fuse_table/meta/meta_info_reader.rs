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

use common_arrow::parquet::read::read_metadata;
use common_dal::DataAccessor;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::datasources::fuse_table::io::segment_reader::read_segment;
use crate::datasources::fuse_table::meta::table_snapshot::RawBlockStats;
use crate::datasources::fuse_table::meta::table_snapshot::SegmentInfo;
use crate::sessions::DatafuseQueryContextRef;

// TODO cache
pub struct MetaInfoReader {
    da: Arc<dyn DataAccessor>,
    ctx: DatafuseQueryContextRef,
}

impl MetaInfoReader {
    pub fn new(da: Arc<dyn DataAccessor>, ctx: DatafuseQueryContextRef) -> Self {
        MetaInfoReader { da, ctx }
    }
}

impl MetaInfoReader {
    #[allow(dead_code)]
    // this method is called by Table::read_plan, which is sync
    pub fn read_block_statistics(&self, location: &str) -> Result<RawBlockStats> {
        let mut reader = self.da.get_reader(location, None)?;
        let file_meta = read_metadata(&mut reader).map_err(ErrorCode::from_std_error)?; // TODO
                                                                                        // one row group only
        let cols = file_meta.row_groups[0].columns();
        let mut res = std::collections::HashMap::new();
        for (id, x) in cols.iter().enumerate() {
            let s = x.statistics();
            if let Some(Ok(stats)) = s {
                res.insert(id as u32, stats);
            }
        }
        Ok(res)
    }
    #[allow(dead_code)]
    pub fn read_segment_info(&self, location: &str) -> Result<SegmentInfo> {
        read_segment(self.da.clone(), &self.ctx, location)
    }
}
