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

use storages_common_table_meta::meta::Location;

pub struct SegmentLocation {
    pub segment_count: usize,
    pub location: Location,
    pub snapshot_loc: Option<String>,
}

pub fn create_segment_location_vector(
    locations: Vec<Location>,
    snapshot_loc: Option<String>,
) -> Vec<SegmentLocation> {
    let segment_count = locations.len();
    let mut seg_locations = Vec::with_capacity(segment_count);
    for location in locations.iter() {
        seg_locations.push(SegmentLocation {
            segment_count,
            location: location.to_owned(),
            snapshot_loc: snapshot_loc.clone(),
        });
    }

    seg_locations
}
