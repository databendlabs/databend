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

use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::Statistics;

/// A segment comprised of one or more blocks
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SegmentInfo {
    // TODO format_version
    // pub format_version: u32,
    /// blocks belong to this segment
    pub blocks: Vec<BlockMeta>,

    /// summary statistics
    pub summary: Statistics,
}
