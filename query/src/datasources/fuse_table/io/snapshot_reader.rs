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

use common_exception::Result;

use crate::datasources::dal::DataAccessor;
use crate::datasources::fuse_table::io::reader_util::do_read_obj;
use crate::datasources::fuse_table::io::reader_util::do_read_obj_async;
use crate::datasources::fuse_table::meta::table_snapshot::TableSnapshot;
use crate::sessions::DatafuseQueryContextRef;

pub fn read_table_snapshot(
    da: Arc<dyn DataAccessor>,
    ctx: &DatafuseQueryContextRef,
    loc: &str,
) -> Result<TableSnapshot> {
    do_read_obj(da, ctx, loc)
}

#[allow(dead_code)]
pub async fn read_table_snapshot_async(
    da: Arc<dyn DataAccessor>,
    loc: &str,
) -> Result<TableSnapshot> {
    do_read_obj_async(da, loc).await
}
