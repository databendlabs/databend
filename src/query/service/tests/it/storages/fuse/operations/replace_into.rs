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

use common_exception::Result;
use common_storages_fuse::FuseTable;

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

        for _ in 0..100 {
            let num_partition: usize = if number_segment == 1 {
                1
            } else {
                rng.gen_range(1..number_segment)
            };

            let chunks = FuseTable::partition_segments(&segments, num_partition);
            assert_eq!(chunks.len(), num_partition);
            for (idx, (segment_idx, _)) in chunks.clone().into_iter().flatten().enumerate() {
                assert_eq!(idx, segment_idx)
            }
        }
    }
    Ok(())
}
