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

use common_cache::Cache;
use common_exception::Result;
use opendal::Operator;

use crate::storages::fuse::cache::SegmentInfoCache;
use crate::storages::fuse::io::write_meta;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::Location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::Versioned;

pub struct SegmentWriter<'a> {
    location_generator: &'a TableMetaLocationGenerator,
    data_accessor: &'a Operator,
    cache: &'a Option<SegmentInfoCache>,
}

impl<'a> SegmentWriter<'a> {
    pub fn new(
        data_accessor: &'a Operator,
        location_generator: &'a TableMetaLocationGenerator,
        cache: &'a Option<SegmentInfoCache>,
    ) -> Self {
        Self {
            location_generator,
            data_accessor,
            cache,
        }
    }
    pub async fn write_segment(&self, segment: SegmentInfo) -> Result<Location> {
        let segment_path = self.location_generator.gen_segment_info_location();
        let segment_location = (segment_path, SegmentInfo::VERSION);
        write_meta(self.data_accessor, segment_location.0.as_str(), &segment).await?;

        if let Some(ref cache) = self.cache {
            let cache = &mut cache.write().await;
            cache.put(segment_location.0.clone(), Arc::new(segment));
        }
        Ok(segment_location)
    }
}
