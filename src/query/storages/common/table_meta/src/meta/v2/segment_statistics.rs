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

use std::io::Read;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_frozen_api::FrozenAPI;
use databend_common_frozen_api::frozen_api;
use databend_common_io::prelude::BinaryRead;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::BlockTopN;
use crate::meta::FormatVersion;
use crate::meta::MetaCompression;
use crate::meta::MetaEncoding;
use crate::meta::RawBlockHLL;
use crate::meta::Versioned;
use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;

#[frozen_api("4c9b22c5")]
#[derive(Serialize, Deserialize, Clone, Debug, FrozenAPI)]
pub struct SegmentStatistics {
    pub format_version: FormatVersion,

    /// HLL data for blocks within the segment.
    /// This stores the HyperLogLog statistics for each block in the segment.
    pub block_hlls: Vec<RawBlockHLL>,

    /// Top-N statistics for each block in the segment.
    #[serde(default)]
    pub block_top_ns: Vec<BlockTopN>,
}

impl SegmentStatistics {
    pub fn new(block_hlls: Vec<RawBlockHLL>, block_top_ns: Vec<BlockTopN>) -> Self {
        Self {
            format_version: SegmentStatistics::VERSION,
            block_hlls,
            block_top_ns,
        }
    }

    pub fn encoding() -> MetaEncoding {
        MetaEncoding::MessagePack
    }

    pub fn compression() -> MetaCompression {
        MetaCompression::Zstd
    }

    pub fn memory_size(&self) -> usize {
        let hll_size = self.block_hlls.capacity() * std::mem::size_of::<RawBlockHLL>()
            + self.block_hlls.iter().map(Vec::capacity).sum::<usize>();
        let top_n_size = self.block_top_ns.capacity() * std::mem::size_of::<BlockTopN>()
            + self
                .block_top_ns
                .iter()
                .map(|top_n| {
                    top_n.capacity() * std::mem::size_of::<(ColumnId, crate::meta::ColumnTopN)>()
                        + top_n
                            .values()
                            .map(|column_top_n| {
                                column_top_n.values.capacity()
                                    * std::mem::size_of::<crate::meta::ColumnTopNEntry>()
                                    + column_top_n
                                        .values
                                        .iter()
                                        .map(|entry| entry.scalar.as_ref().memory_size())
                                        .sum::<usize>()
                            })
                            .sum::<usize>()
                })
                .sum::<usize>();
        std::mem::size_of::<Self>() + hll_size + top_n_size
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

    pub fn from_read(mut r: impl Read) -> Result<SegmentStatistics> {
        let version = r.read_scalar::<u64>()?;
        assert_eq!(version, SegmentStatistics::VERSION);
        let encoding = MetaEncoding::try_from(r.read_scalar::<u8>()?)?;
        let compression = MetaCompression::try_from(r.read_scalar::<u8>()?)?;
        let statistics_size: u64 = r.read_scalar::<u64>()?;
        read_and_deserialize(&mut r, statistics_size, &encoding, &compression)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;

    use serde::Serialize;

    use super::*;
    use crate::meta::ColumnTopN;

    #[derive(Serialize)]
    struct LegacySegmentStatistics {
        format_version: FormatVersion,
        block_hlls: Vec<RawBlockHLL>,
    }

    fn legacy_bytes(statistics: &LegacySegmentStatistics) -> Result<Vec<u8>> {
        let encoding = SegmentStatistics::encoding();
        let compression = SegmentStatistics::compression();
        let data = encode(&encoding, statistics)?;
        let data = compress(&compression, data)?;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&statistics.format_version.to_le_bytes());
        bytes.push(encoding as u8);
        bytes.push(compression as u8);
        bytes.extend_from_slice(&(data.len() as u64).to_le_bytes());
        bytes.extend(data);
        Ok(bytes)
    }

    #[test]
    fn reads_legacy_statistics_without_top_n() -> Result<()> {
        let legacy = LegacySegmentStatistics {
            format_version: SegmentStatistics::VERSION,
            block_hlls: vec![vec![1, 2, 3]],
        };

        let statistics = SegmentStatistics::from_read(Cursor::new(legacy_bytes(&legacy)?))?;

        assert_eq!(statistics.block_hlls, legacy.block_hlls);
        assert!(statistics.block_top_ns.is_empty());
        Ok(())
    }

    #[test]
    fn round_trips_empty_block_top_n_entries() -> Result<()> {
        let block_top_ns = vec![HashMap::from([(7, ColumnTopN::with_capacity(16))])];
        let bytes = SegmentStatistics::new(vec![], block_top_ns.clone()).to_bytes()?;

        let statistics = SegmentStatistics::from_read(Cursor::new(bytes))?;

        assert_eq!(statistics.block_top_ns, block_top_ns);
        assert!(statistics.memory_size() > std::mem::size_of::<SegmentStatistics>());
        Ok(())
    }
}
