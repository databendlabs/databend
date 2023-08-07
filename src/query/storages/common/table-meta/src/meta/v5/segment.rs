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
use std::io::Read;
use std::sync::Arc;

use common_exception::Result;
use common_io::prelude::BinaryRead;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::format::compress;
use crate::meta::format::decode_segment_header;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::format::SegmentHeader;
use crate::meta::v2;
use crate::meta::v2::BlockMeta;
use crate::meta::v3;
use crate::meta::v4;
use crate::meta::v5::Statistics;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::MetaCompression;
use crate::meta::MetaEncoding;
use crate::meta::Versioned;

/// A segment comprises one or more blocks
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct LeafSegmentInfo {
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
    pub blocks: Vec<Arc<BlockMeta>>,
    /// summary statistics
    pub summary: Statistics,
}

impl LeafSegmentInfo {
    pub fn tag() -> u8 {
        0
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.to_bytes_with_encoding(MetaEncoding::MessagePack)
    }

    fn to_bytes_with_encoding(&self, encoding: MetaEncoding) -> Result<Vec<u8>> {
        let compression = MetaCompression::default();

        let blocks = encode(&encoding, &self.blocks)?;
        let blocks_compress = compress(&compression, blocks)?;

        let summary = encode(&encoding, &self.summary)?;
        let summary_compress = compress(&compression, summary)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + blocks_compress.len().to_le_bytes().len()
            + blocks_compress.len()
            + summary_compress.len().to_le_bytes().len()
            + summary_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&blocks_compress.len().to_le_bytes());
        buf.extend_from_slice(&summary_compress.len().to_le_bytes());
        // add tag to distinct leaf and internal
        buf.push(Self::tag());
        buf.extend(blocks_compress);
        buf.extend(summary_compress);

        Ok(buf)
    }
}

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
    pub fn tag() -> u8 {
        1
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.to_bytes_with_encoding(MetaEncoding::MessagePack)
    }

    fn to_bytes_with_encoding(&self, encoding: MetaEncoding) -> Result<Vec<u8>> {
        let compression = MetaCompression::default();

        let child_segments = encode(&encoding, &self.child_segments)?;
        let child_segments_compress = compress(&compression, child_segments)?;

        let summary = encode(&encoding, &self.summary)?;
        let summary_compress = compress(&compression, summary)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + child_segments_compress.len().to_le_bytes().len()
            + child_segments_compress.len()
            + summary_compress.len().to_le_bytes().len()
            + summary_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(compression as u8);
        buf.push(encoding as u8);
        buf.extend_from_slice(&child_segments_compress.len().to_le_bytes());
        buf.extend_from_slice(&summary_compress.len().to_le_bytes());
        // add tag to distinct leaf and internal
        buf.push(Self::tag());
        buf.extend(child_segments_compress);
        buf.extend(summary_compress);

        Ok(buf)
    }
}

#[derive(Serialize, Deserialize)]
pub enum SegmentInfo {
    InternalSegment(InternalSegmentInfo),
    LeafSegment(LeafSegmentInfo),
}

impl From<v3::SegmentInfo> for SegmentInfo {
    fn from(value: v3::SegmentInfo) -> Self {
        SegmentInfo::from(v4::SegmentInfo::from_v3(value))
    }
}

impl<T> From<T> for SegmentInfo
where T: Into<v2::SegmentInfo>
{
    fn from(value: T) -> Self {
        SegmentInfo::from(v4::SegmentInfo::from_v2(value.into()))
    }
}

impl From<v4::SegmentInfo> for SegmentInfo {
    fn from(value: v4::SegmentInfo) -> Self {
        SegmentInfo::LeafSegment(LeafSegmentInfo {
            format_version: value.format_version,
            blocks: value.blocks,
            summary: Statistics::from_v2(value.summary),
        })
    }
}

impl SegmentInfo {
    pub fn new_leaf(blocks: Vec<Arc<BlockMeta>>, summary: Statistics) -> Self {
        Self::LeafSegment(LeafSegmentInfo {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        })
    }

    pub fn new_leaf_with_version(
        blocks: Vec<Arc<BlockMeta>>,
        summary: Statistics,
        version: FormatVersion,
    ) -> Self {
        Self::LeafSegment(LeafSegmentInfo {
            format_version: version,
            blocks,
            summary,
        })
    }

    pub fn new_internal(segments: Vec<Location>, summary: Statistics) -> Self {
        Self::InternalSegment(InternalSegmentInfo {
            format_version: SegmentInfo::VERSION,
            child_segments: segments,
            summary,
        })
    }

    pub fn new_internal_with_version(
        segments: Vec<Location>,
        summary: Statistics,
        version: FormatVersion,
    ) -> Self {
        Self::InternalSegment(InternalSegmentInfo {
            format_version: version,
            child_segments: segments,
            summary,
        })
    }

    // Total block bytes of this segment.
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

    fn read_tag<R>(reader: &mut R) -> Result<u8>
    where R: Read + Unpin + Send {
        reader.read_scalar::<u8>()
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let SegmentHeader {
            version,
            encoding,
            compression,
            // maybe we need to modify as blocks_or_segments_size
            blocks_size,
            summary_size,
        } = decode_segment_header(&mut cursor)?;

        let tag = Self::read_tag(&mut cursor)?;
        let segment = match tag {
            0 => {
                // leaf segment
                let blocks: Vec<Arc<BlockMeta>> =
                    read_and_deserialize(&mut cursor, blocks_size, &encoding, &compression)?;
                let summary: Statistics =
                    read_and_deserialize(&mut cursor, summary_size, &encoding, &compression)?;
                Self::new_leaf_with_version(blocks, summary, version)
            }
            1 => {
                // internal segment
                let child_segments: Vec<Location> =
                    read_and_deserialize(&mut cursor, blocks_size, &encoding, &compression)?;
                let summary: Statistics =
                    read_and_deserialize(&mut cursor, summary_size, &encoding, &compression)?;
                Self::new_internal_with_version(child_segments, summary, version)
            }
            _ => panic!("read bad tag for v5"),
        };

        Ok(segment)
    }
}
