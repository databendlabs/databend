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

use std::collections::BTreeSet;
use std::io::Cursor;

use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::format::compress;
use crate::meta::format::decode_index_header;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::format::IndexHeader;
use crate::meta::format::MetaCompression;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::MetaEncoding;
use crate::meta::Versioned;

/// The IndexInfo structure stores the relationship between index and segments.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct IndexInfo {
    pub format_version: FormatVersion,
    // Index version updated every time schema changed.
    pub index_version: String,
    // Indexed segments locations, used to determine if the segment
    // is already indexed and to prevent duplicate indexed.
    pub indexed_segments: BTreeSet<Location>,
}

impl IndexInfo {
    pub fn new(index_version: String, indexed_segments: BTreeSet<Location>) -> Self {
        Self {
            format_version: IndexInfo::VERSION,
            index_version,
            indexed_segments,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.to_bytes_with_encoding(MetaEncoding::MessagePack)
    }

    fn to_bytes_with_encoding(&self, encoding: MetaEncoding) -> Result<Vec<u8>> {
        let compression = MetaCompression::default();

        let index_version = encode(&encoding, &self.index_version)?;
        let index_version_compress = compress(&compression, index_version)?;
        let indexed_segments = encode(&encoding, &self.indexed_segments)?;
        let indexed_segments_compress = compress(&compression, indexed_segments)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + index_version_compress.len().to_le_bytes().len()
            + index_version_compress.len()
            + indexed_segments_compress.len().to_le_bytes().len()
            + indexed_segments_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&index_version_compress.len().to_le_bytes());
        buf.extend_from_slice(&indexed_segments_compress.len().to_le_bytes());

        buf.extend(index_version_compress);
        buf.extend(indexed_segments_compress);

        Ok(buf)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let IndexHeader {
            version,
            encoding,
            compression,
            index_version_size,
            indexed_segments_size,
        } = decode_index_header(&mut cursor)?;

        let index_version: String =
            read_and_deserialize(&mut cursor, index_version_size, &encoding, &compression)?;
        let indexed_segments: BTreeSet<Location> =
            read_and_deserialize(&mut cursor, indexed_segments_size, &encoding, &compression)?;

        let mut index_info = Self::new(index_version, indexed_segments);

        index_info.format_version = version;
        Ok(index_info)
    }
}
