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

use std::collections::HashSet;
use std::sync::RwLock;

use databend_storages_common_table_meta::meta::Location;

#[derive(Default)]
pub struct SegmentLocationsState {
    locations: RwLock<HashSet<Location>>,
}

impl SegmentLocationsState {
    pub fn add(&self, location: Location) {
        self.locations.write().unwrap().insert(location);
    }

    pub fn clear(&self) {
        self.locations.write().unwrap().clear();
    }

    pub fn list(&self) -> Vec<Location> {
        self.locations.read().unwrap().iter().cloned().collect()
    }
}
