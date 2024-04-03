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
use databend_common_io::prelude::BinaryRead;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
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
        let encoding = MetaEncoding::MessagePack;
        let compression = MetaCompression::default();

        let data = encode(&encoding, &self)?;
        let data_compress = compress(&compression, data)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + data_compress.len().to_le_bytes().len()
            + data_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&data_compress.len().to_le_bytes());

        buf.extend(data_compress);

        Ok(buf)
    }

    pub fn from_slice(buffer: &[u8]) -> Result<IndexInfo> {
        let mut cursor = Cursor::new(buffer);
        let version = cursor.read_scalar::<u64>()?;
        assert_eq!(version, IndexInfo::VERSION);
        let encoding = MetaEncoding::try_from(cursor.read_scalar::<u8>()?)?;
        let compression = MetaCompression::try_from(cursor.read_scalar::<u8>()?)?;
        let index_info_size: u64 = cursor.read_scalar::<u64>()?;

        read_and_deserialize(&mut cursor, index_info_size, &encoding, &compression)
    }
}
