//  Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SegmentInfo;

use crate::io::SegmentsIO;

impl SegmentsIO {
    // Read SegmentLite with chunks for memory usage.
    // Used for optimize table compact etc.
    pub async fn read_segment_lites(
        &self,
        segment_locations: &[Location],
    ) -> Result<Vec<Arc<SegmentInfo>>> {
        let chunk_size = 100;
        let mut lites = vec![];

        for chunk in segment_locations.chunks(chunk_size) {
            let results = self.read_segments(chunk).await?;
            for res in results.into_iter().flatten() {
                lites.push(Self::segment_lite(&res));
            }
        }
        Ok(lites)
    }

    // Trim the col_stats to default to reduce the memory usage.
    fn segment_lite(segment: &Arc<SegmentInfo>) -> Arc<SegmentInfo> {
        let blocks = segment
            .blocks
            .iter()
            .map(|v| {
                BlockMeta::new(
                    v.row_count,
                    v.block_size,
                    v.file_size,
                    Default::default(),
                    v.col_metas.clone(),
                    v.cluster_stats.clone(),
                    v.location.clone(),
                    v.bloom_filter_index_location.clone(),
                    v.bloom_filter_index_size,
                )
            })
            .collect();

        Arc::new(SegmentInfo::new(blocks, segment.summary.clone()))
    }
}
