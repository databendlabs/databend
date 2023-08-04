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
use std::sync::Arc;

use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use super::super::v2;
use super::super::v3;
use crate::meta::format::compress;
use crate::meta::format::decode_segment_header;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::format::SegmentHeader;
use crate::meta::v2::BlockMeta;
use crate::meta::v4;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::MetaCompression;
use crate::meta::MetaEncoding;
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
    pub child_segments: Vec<Location>,
    /// summary statistics
    pub summary: Statistics,
}

impl InternalSegmentInfo {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.to_bytes_with_encoding(MetaEncoding::MessagePack)
    }

    fn to_bytes_with_encoding(&self, encoding: MetaEncoding) -> Result<Vec<u8>> {
        let compression = MetaCompression::default();

        let son_segments = encode(&encoding, &self.child_segments)?;
        let segments_compress = compress(&compression, son_segments)?;

        let summary = encode(&encoding, &self.summary)?;
        let summary_compress = compress(&compression, summary)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + segments_compress.len().to_le_bytes().len()
            + segments_compress.len()
            + summary_compress.len().to_le_bytes().len()
            + summary_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&segments_compress.len().to_le_bytes());
        buf.extend_from_slice(&summary_compress.len().to_le_bytes());

        buf.extend(segments_compress);
        buf.extend(summary_compress);

        Ok(buf)
    }
}

/// we need to reuse name 'SegmentInfo' in V5
/// to reduce changes.
pub type LeafSegmentInfo = v4::SegmentInfo;

#[derive(Serialize, Deserialize)]
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
    pub fn new_internal(segments: Vec<Location>, summary: Statistics) -> Self {
        Self::InternalSegment(InternalSegmentInfo {
            format_version: SegmentInfo::VERSION,
            child_segments: segments,
            summary,
        })
    }

    // Total block bytes of this segment.
    #[allow(unused)]
    pub fn total_bytes(&self) -> u64 {
        match self {
            Self::LeafSegment(leaf_segment_info) => {
                leaf_segment_info.summary.uncompressed_byte_size
            }
            Self::InternalSegment(internal_segment_info) => {
                internal_segment_info.summary.uncompressed_byte_size
            }
        }
    }

    /// Serializes the Segment struct to a byte vector.
    ///
    /// The byte vector contains the format version, encoding, compression, and compressed block data and
    /// summary data. The encoding and compression are set to default values. The block data and summary
    /// data are encoded and compressed, respectively.
    ///
    /// # Returns
    ///
    /// A Result containing the serialized Segment data as a byte vector. If any errors occur during
    /// encoding, compression, or writing to the byte vector, an error will be returned.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Self::LeafSegment(leaf_segment_info) => leaf_segment_info.to_bytes(),
            Self::InternalSegment(internal_segment_info) => internal_segment_info.to_bytes(),
        }
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let segment;
        let mut cursor = Cursor::new(bytes);
        let SegmentHeader {
            version,
            encoding,
            compression,
            // maybe we need to modify as blocks_or_segments_size
            blocks_size,
            summary_size,
        } = decode_segment_header(&mut cursor)?;

        if version == LeafSegmentInfo::VERSION {
            let blocks: Vec<Arc<BlockMeta>> =
                read_and_deserialize(&mut cursor, blocks_size, &encoding, &compression)?;
            let summary: Statistics =
                read_and_deserialize(&mut cursor, summary_size, &encoding, &compression)?;

            segment = Self::new_leaf(blocks, summary);
        } else if version == Self::VERSION {
            let child_segments: Vec<Location> =
                read_and_deserialize(&mut cursor, blocks_size, &encoding, &compression)?;
            let summary: Statistics =
                read_and_deserialize(&mut cursor, summary_size, &encoding, &compression)?;

            segment = Self::new_internal(child_segments, summary);
        } else {
            panic!("read bad version for v5")
        }

        Ok(segment)
    }
}
