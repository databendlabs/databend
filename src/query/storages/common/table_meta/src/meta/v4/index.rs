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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io::Cursor;
use std::ops::Range;

use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::format::compress;
use crate::meta::format::decode_index_header;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::format::IndexHeader;
use crate::meta::format::MetaCompression;
use crate::meta::FormatVersion;
use crate::meta::MetaEncoding;
use crate::meta::Versioned;

/// The IndexInfo structure stores the relationship between indexes and segments.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct IndexInfo {
    pub format_version: FormatVersion,
    // Index schema
    pub schema: TableSchemaRef,
    // The relationship of indexes and segments,
    // an index can correspond to multiple segments.
    // The key is index file location,
    // and the value is related segment infos
    pub indexes: BTreeMap<String, Vec<IndexSegmentInfo>>,
    // Indexed segments locations, used to determine if the segment
    // is already indexed and to prevent duplicate indexed.
    pub indexed_segments: BTreeSet<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct IndexSegmentInfo {
    // Segment file location
    pub segment_location: String,
    // Row count of the segment, used to skip deleted segments
    pub row_count: u64,
    // Optional range of blocks in a segment.
    // For segments with a large amount of data,
    // multiple index files may be needed for indexing.
    // None means all the blocks in the segment are indexed.
    pub block_range: Option<Range<u64>>,
}

impl IndexSegmentInfo {
    pub fn new(segment_location: String, row_count: u64, block_range: Option<Range<u64>>) -> Self {
        Self {
            segment_location,
            row_count,
            block_range,
        }
    }
}

impl IndexInfo {
    pub fn new(
        schema: TableSchemaRef,
        indexes: BTreeMap<String, Vec<IndexSegmentInfo>>,
        indexed_segments: BTreeSet<String>,
    ) -> Self {
        Self {
            format_version: IndexInfo::VERSION,
            schema,
            indexes,
            indexed_segments,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.to_bytes_with_encoding(MetaEncoding::MessagePack)
    }

    fn to_bytes_with_encoding(&self, encoding: MetaEncoding) -> Result<Vec<u8>> {
        let compression = MetaCompression::default();

        let schema = encode(&encoding, &self.schema)?;
        let schema_compress = compress(&compression, schema)?;
        let indexes = encode(&encoding, &self.indexes)?;
        let indexes_compress = compress(&compression, indexes)?;
        let indexed_segments = encode(&encoding, &self.indexed_segments)?;
        let indexed_segments_compress = compress(&compression, indexed_segments)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + schema_compress.len().to_le_bytes().len()
            + schema_compress.len()
            + indexes_compress.len().to_le_bytes().len()
            + indexes_compress.len()
            + indexed_segments_compress.len().to_le_bytes().len()
            + indexed_segments_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&schema_compress.len().to_le_bytes());
        buf.extend_from_slice(&indexes_compress.len().to_le_bytes());
        buf.extend_from_slice(&indexed_segments_compress.len().to_le_bytes());

        buf.extend(schema_compress);
        buf.extend(indexes_compress);
        buf.extend(indexed_segments_compress);

        Ok(buf)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let IndexHeader {
            version,
            encoding,
            compression,
            schema_size,
            indexes_size,
            indexed_segments_size,
        } = decode_index_header(&mut cursor)?;

        let schema: TableSchema =
            read_and_deserialize(&mut cursor, schema_size, &encoding, &compression)?;
        let indexes: BTreeMap<String, Vec<IndexSegmentInfo>> =
            read_and_deserialize(&mut cursor, indexes_size, &encoding, &compression)?;
        let indexed_segments: BTreeSet<String> =
            read_and_deserialize(&mut cursor, indexed_segments_size, &encoding, &compression)?;

        let mut index_info = Self::new(schema.into(), indexes, indexed_segments);

        index_info.format_version = version;
        Ok(index_info)
    }
}
