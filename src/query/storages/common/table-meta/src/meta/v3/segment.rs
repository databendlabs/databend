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
use std::sync::Arc;

use common_exception::Result;
use common_expression::TableField;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::segment_format::compress;
use crate::meta::segment_format::encode;
use crate::meta::segment_format::Compression;
use crate::meta::statistics::FormatVersion;
use crate::meta::v2::BlockMeta;
use crate::meta::Encoding;
use crate::meta::Statistics;
use crate::meta::Versioned;

/// A segment comprises one or more blocks
/// The structure of the segment is the same as that of v2, but the serialization and deserialization methods are different
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SegmentInfo {
    /// format version
    format_version: FormatVersion,
    /// blocks belong to this segment
    pub blocks: Vec<Arc<BlockMeta>>,
    /// summary statistics
    pub summary: Statistics,
}

impl SegmentInfo {
    #[inline]
    pub fn version(&self) -> FormatVersion {
        self.format_version
    }
}

use super::super::v0;
use super::super::v1;
use super::super::v2;

impl SegmentInfo {
    pub fn from_v0(s: v0::SegmentInfo, fields: &[TableField]) -> Self {
        let summary = Statistics::from_v0(s.summary, fields);
        Self {
            format_version: SegmentInfo::VERSION,
            blocks: s
                .blocks
                .into_iter()
                .map(|b| Arc::new(BlockMeta::from_v0(&b, fields)))
                .collect::<_>(),
            summary,
        }
    }

    pub fn from_v1(s: v1::SegmentInfo, fields: &[TableField]) -> Self {
        let summary = Statistics::from_v0(s.summary, fields);
        Self {
            format_version: SegmentInfo::VERSION,
            blocks: s
                .blocks
                .into_iter()
                .map(|b| Arc::new(BlockMeta::from_v1(b.as_ref(), fields)))
                .collect::<_>(),
            summary,
        }
    }

    pub fn from_v2(s: v2::SegmentInfo) -> Self {
        Self {
            format_version: SegmentInfo::VERSION,
            blocks: s.blocks,
            summary: s.summary,
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
        let mut buf = Vec::new();

        let encoding = Encoding::default();
        let compression = Compression::default();

        let blocks = encode(&encoding, &self.blocks)?;
        let blocks_compress = compress(&compression, blocks)?;

        let summary = encode(&encoding, &self.summary)?;
        let summary_compress = compress(&compression, summary)?;

        buf.extend(
            vec![
                self.format_version.to_le_bytes(),
                (encoding as u64).to_le_bytes(),
                (compression as u64).to_le_bytes(),
                blocks_compress.len().to_le_bytes(),
                summary_compress.len().to_le_bytes(),
            ]
            .concat(),
        );
        buf.extend(blocks_compress);
        buf.extend(summary_compress);

        Ok(buf)
    }
}

impl SegmentInfo {
    pub fn new(blocks: Vec<Arc<BlockMeta>>, summary: Statistics) -> Self {
        Self {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        }
    }

    pub fn format_version(&self) -> u64 {
        self.format_version
    }

    // Total block bytes of this segment.
    pub fn total_bytes(&self) -> u64 {
        self.blocks.iter().map(|v| v.block_size).sum()
    }
}
