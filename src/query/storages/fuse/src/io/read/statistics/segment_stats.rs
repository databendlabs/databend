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

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_exception::Result;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::SegmentStatistics;
use databend_storages_common_table_meta::meta::Location;
use fastrace::func_path;
use fastrace::prelude::*;
use opendal::Operator;

use crate::io::MetaReaders;

#[async_backtrace::framed]
#[fastrace::trace]
pub async fn read_segment_stats(dal: Operator, loc: Location) -> Result<Arc<SegmentStatistics>> {
    let reader = MetaReaders::segment_stats_reader(dal);
    let (location, ver) = loc;
    let load_params = LoadParams {
        location,
        len_hint: None,
        ver,
        put_cache: true,
    };
    reader.read(&load_params).await
}

#[async_backtrace::framed]
#[fastrace::trace]
pub async fn read_segment_stats_in_parallel(
    dal: Operator,
    locations: &[Location],
    threads_nums: usize,
) -> Result<Vec<Arc<SegmentStatistics>>> {
    // combine all the tasks.
    let mut iter = locations.iter();
    let tasks = std::iter::from_fn(|| {
        iter.next().map(|location| {
            let dal = dal.clone();
            let loc = location.clone();
            async move { read_segment_stats(dal, loc).await }
                .in_span(Span::enter_with_local_parent(func_path!()))
        })
    });

    let permit_nums = threads_nums * 2;
    execute_futures_in_parallel(
        tasks,
        threads_nums,
        permit_nums,
        "fuse-segment-stats-worker".to_owned(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<_>>>()
}
