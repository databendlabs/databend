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

use std::sync::Arc;

use serde::Deserialize;
use serde::Serialize;

use super::super::v2;
use super::super::v3;
use crate::meta::v2::BlockMeta;
use crate::meta::v4;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::Statistics;
use crate::meta::Versioned;

/// An InternalSegment comprises one or more segmentss
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct InternalSegmentInfo {
    /// format version of SegmentInfo table meta data
    ///
    /// Note that:
    ///
    /// - A instance of v3::SegmentInfo may have a value of v2/v1::SegmentInfo::VERSION for this field.
    ///
    ///   That indicates this instance is converted from a v2/v1::SegmentInfo.
    ///
    /// - The meta writers are responsible for only writing down the latest version of SegmentInfo, and
    /// the format_version being written is of the latest version.
    ///
    ///   e.g. if the current version of SegmentInfo is v3::SegmentInfo, then the format_version
    ///   that will be written down to object storage as part of SegmentInfo table meta data,
    ///   should always be v3::SegmentInfo::VERSION (which is 3)
    pub format_version: FormatVersion,
    /// segments belong to this internal_segment
    /// there will be normal segments and internal segment
    /// the suffix name of different segments is different,
    /// so we can use this tag to distinct them.
    pub son_segments: Vec<Arc<Location>>,
    /// summary statistics
    pub summary: Statistics,
}

/// we need to reuse name 'SegmentInfo' in V5
/// to reduce changes.
pub type LeafSegmentInfo = v4::SegmentInfo;

pub enum SegmentInfo {
    #[allow(unused)]
    InternalSegment(InternalSegmentInfo),
    LeafSegment(LeafSegmentInfo),
}

impl From<v3::SegmentInfo> for SegmentInfo {
    fn from(value: v3::SegmentInfo) -> Self {
        SegmentInfo::LeafSegment(LeafSegmentInfo::from_v3(value))
    }
}

impl<T> From<T> for SegmentInfo
where T: Into<v2::SegmentInfo>
{
    fn from(value: T) -> Self {
        SegmentInfo::LeafSegment(LeafSegmentInfo::from_v2(value.into()))
    }
}

impl SegmentInfo {
    #[allow(unused)]
    pub fn new_leaf(blocks: Vec<Arc<BlockMeta>>, summary: Statistics) -> Self {
        Self::LeafSegment(LeafSegmentInfo {
            format_version: LeafSegmentInfo::VERSION,
            blocks,
            summary,
        })
    }

    #[allow(unused)]
    pub fn new_internal(segments: Vec<Arc<Location>>, summary: Statistics) -> Self {
        Self::InternalSegment(InternalSegmentInfo {
            format_version: InternalSegmentInfo::VERSION,
            son_segments: segments,
            summary,
        })
    }

    // Total block bytes of this segment.
    pub fn total_bytes(&self) -> u64 {
        match self {
            Self::LeafSegment(leaf_segment_info) => {
                leaf_segment_info.blocks.iter().map(|v| v.block_size).sum()
            }
            Self::InternalSegment(internal_segment_info) => {
                internal_segment_info.summary.uncompressed_byte_size
            }
        }
    }

    // Encode self.blocks as RawBlockMeta.
    fn block_raw_bytes(&self) -> Result<RawBlockMeta> {
        let encoding = MetaEncoding::MessagePack;
        let bytes = encode(&encoding, &self.blocks)?;

        let compression = MetaCompression::default();
        let compressed = compress(&compression, bytes)?;

        Ok(RawBlockMeta {
            bytes: compressed,
            encoding,
            compression,
        })
    }
}
