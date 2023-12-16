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

use databend_storages_common_table_meta::meta::Location;

#[derive(Debug, Clone)]
pub struct SegmentLocation {
    pub segment_idx: usize,
    pub location: Location,
    pub snapshot_loc: Option<String>,
}

pub fn create_segment_location_vector(
    locations: Vec<Location>,
    snapshot_loc: Option<String>,
) -> Vec<SegmentLocation> {
    let segment_count = locations.len();
    let mut seg_locations = Vec::with_capacity(segment_count);
    for (segment_idx, location) in locations.into_iter().enumerate() {
        seg_locations.push(SegmentLocation {
            segment_idx,
            location,
            snapshot_loc: snapshot_loc.clone(),
        });
    }

    seg_locations
}
