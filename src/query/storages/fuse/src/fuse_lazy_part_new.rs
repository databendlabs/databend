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

use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Range;
use std::sync::Arc;

use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use storages_common_table_meta::meta::Location;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct NewFuseLazyPartInfo {
    pub snapshot_location: Location,
    pub segments_range: Range<usize>,
}

#[typetag::serde(name = "fuse_lazy")]
impl PartInfo for NewFuseLazyPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<NewFuseLazyPartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn hash(&self) -> u64 {
        0
        // let mut s = DefaultHasher::new();
        // self.segment_location.0.hash(&mut s);
        // s.finish()
    }
}

impl NewFuseLazyPartInfo {
    pub fn create(idx: usize, snapshot_location: Location, range: Range<usize>) -> PartInfoPtr {
        Arc::new(Box::new(NewFuseLazyPartInfo {
            snapshot_location,
            segments_range: range,
        }))
    }
}
