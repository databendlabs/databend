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
//

use crate::storages::fuse::meta::v0::snapshot::Statistics;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::Versioned;

/// A segment comprises one or more blocks
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SegmentInfo {
    /// format version
    format_version: u64,
    /// blocks belong to this segment
    pub blocks: Vec<BlockMeta>,
    /// summary statistics
    pub summary: Statistics,
}

impl SegmentInfo {
    pub fn new(blocks: Vec<BlockMeta>, summary: Statistics) -> Self {
        Self {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        }
    }

    pub fn format_version(&self) -> u64 {
        self.format_version
    }
}

use super::super::v0::segment::SegmentInfo as SegmentInfoV0;
impl From<SegmentInfoV0> for SegmentInfo {
    fn from(s: SegmentInfoV0) -> Self {
        Self {
            format_version: 1,
            blocks: s.blocks.into_iter().map(|b| b.into()).collect::<_>(),
            summary: s.summary,
        }
    }
}
