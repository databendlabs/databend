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

use std::io::Cursor;

use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use super::frozen;
use crate::meta::format::decode_segment_header;
use crate::meta::format::read_and_deserialize;
use crate::meta::format::SegmentHeader;
use crate::meta::FormatVersion;
use crate::meta::MetaEncoding;
use crate::meta::Versioned;

/// A segment comprises one or more blocks
/// The structure of the segment is the same as that of v2, but the serialization and deserialization methods are different
#[derive(Serialize, Deserialize)]
pub struct SegmentInfo {
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
    /// blocks belong to this segment
    pub blocks: Vec<frozen::BlockMeta>,
    /// summary statistics
    pub summary: frozen::Statistics,
}

impl SegmentInfo {
    pub fn new(blocks: Vec<frozen::BlockMeta>, summary: frozen::Statistics) -> Self {
        Self {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        }
    }

    // Total block bytes of this segment.
    pub fn total_bytes(&self) -> u64 {
        self.blocks.iter().map(|v| v.block_size).sum()
    }

    #[inline]
    pub fn encoding() -> MetaEncoding {
        MetaEncoding::Bincode
    }
}

impl SegmentInfo {
    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let SegmentHeader {
            version,
            encoding,
            compression,
            blocks_size,
            summary_size,
        } = decode_segment_header(&mut cursor)?;

        let blocks: Vec<frozen::BlockMeta> =
            read_and_deserialize(&mut cursor, blocks_size, &encoding, &compression)?;
        let summary: frozen::Statistics =
            read_and_deserialize(&mut cursor, summary_size, &encoding, &compression)?;

        let mut segment = Self::new(blocks, summary);

        // bytes may represent an encoded v[n]::SegmentInfo, where n <= self::SegmentInfo::VERSION
        // please see PR https://github.com/datafuselabs/databend/pull/11211 for the adjustment of
        // format_version`'s "semantic"
        segment.format_version = version;
        Ok(segment)
    }
}
