// Copyright 2023 Datafuse Labs.
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

use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;
use itertools::Itertools;

#[test]
fn test_partition() -> Result<()> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
        let number_segment: usize = rng.gen_range(1..100);

        // do not matter, arbitrarily picked
        let format_version = 2;

        let segments = (0..number_segment)
            .map(|idx| (format!("{idx}"), format_version))
            .collect::<Vec<_>>();
        let segments: Vec<_> = segments.into_iter().enumerate().collect();

        for _ in 0..100 {
            let num_partition: usize = if number_segment == 1 {
                1
            } else {
                rng.gen_range(1..number_segment)
            };

            let partitions = FuseTable::partition_segments(&segments, num_partition);
            // check number of partitions are as expected
            assert_eq!(partitions.len(), num_partition);

            // check segments
            let origin = &segments;
            let segment_of_chunks = partitions
                .iter()
                .flatten()
                .sorted_by(|a, b| a.0.cmp(&b.0))
                .collect::<Vec<_>>();

            for (origin_idx, origin_location) in origin {
                let (seg_idx, seg_location) = segment_of_chunks[*origin_idx];
                assert_eq!(origin_idx, seg_idx);
                assert_eq!(origin_location, seg_location);
            }
        }
    }
    Ok(())
}
