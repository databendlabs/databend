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

use arrow::array::RecordBatch;
use bytes::Bytes;
use databend_common_exception::Result;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::block_meta::ColumnOrientedBlockMeta;
use crate::meta::format::compress;
use crate::meta::format::decode;
use crate::meta::format::decompress;
use crate::meta::format::encode;
use crate::meta::AbstractBlockMeta;
use crate::meta::MetaCompression;
use crate::meta::MetaEncoding;
use crate::meta::SegmentInfo;
use crate::meta::Statistics;

pub trait AbstractSegment: Send + Sync + 'static {
    fn blocks(&self) -> Box<dyn Iterator<Item = Arc<dyn AbstractBlockMeta>> + '_>;
    fn summary(&self) -> Statistics;
    fn serialize(&self) -> Result<Vec<u8>>;
    fn deserialize(&self, data: Bytes) -> Result<Arc<dyn AbstractSegment>>;
}

impl AbstractSegment for SegmentInfo {
    fn blocks(&self) -> Box<dyn Iterator<Item = Arc<dyn AbstractBlockMeta>> + '_> {
        Box::new(
            self.blocks
                .iter()
                .map(|b| b.clone() as Arc<dyn AbstractBlockMeta>),
        )
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        self.to_bytes()
    }

    fn summary(&self) -> Statistics {
        self.summary.clone()
    }

    fn deserialize(&self, data: Bytes) -> Result<Arc<dyn AbstractSegment>> {
        Ok(Arc::new(Self::from_slice(&data)?))
    }
}

pub struct ColumnOrientedSegment {
    pub block_metas: RecordBatch,
    pub summary: Statistics,
}

impl AbstractSegment for ColumnOrientedSegment {
    fn blocks(&self) -> Box<dyn Iterator<Item = Arc<(dyn AbstractBlockMeta + 'static)>>> {
        Box::new(BlockMetaIter::new(self.block_metas.clone()))
    }

    fn summary(&self) -> Statistics {
        self.summary.clone()
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        // TODO(Sky): Reuse the buffer.
        let mut write_buffer = Vec::new();
        let encoding = MetaEncoding::MessagePack;
        let compression = MetaCompression::default();
        {
            // TODO(Sky): Construct the optimal props, enabling compression, encoding, etc., if performance is better.
            let props = Some(
                WriterProperties::builder()
                    .set_max_row_group_size(usize::MAX)
                    .build(),
            );
            let mut writer =
                ArrowWriter::try_new(&mut write_buffer, self.block_metas.schema(), props)?;
            writer.write(&self.block_metas)?;
            let _ = writer.close()?;
        }
        let blocks_size = write_buffer.len() as u64;
        {
            let summary = encode(&encoding, &self.summary)?;
            let summary_compress = compress(&compression, summary)?;
            // TODO(Sky): Avoid extra copy.
            write_buffer.extend(summary_compress);
        }
        let summary_size = write_buffer.len() as u64 - blocks_size;
        write_buffer.push(encoding as u8);
        write_buffer.push(compression as u8);
        write_buffer.extend_from_slice(&blocks_size.to_le_bytes());
        write_buffer.extend_from_slice(&summary_size.to_le_bytes());
        Ok(write_buffer)
    }

    fn deserialize(&self, data: Bytes) -> Result<Arc<dyn AbstractSegment>> {
        const FOOTER_SIZE: usize = 18;
        let footer = &data[data.len() - FOOTER_SIZE..];
        let encoding = MetaEncoding::try_from(footer[0])?;
        let compression = MetaCompression::try_from(footer[1])?;
        let blocks_size = u64::from_le_bytes(footer[2..10].try_into().unwrap()) as usize;
        let summary_size = u64::from_le_bytes(footer[10..].try_into().unwrap());

        let block_metas = data.slice(0..blocks_size);
        let mut record_reader = ParquetRecordBatchReader::try_new(block_metas, usize::MAX)?;
        let block_metas = record_reader.next().unwrap()?;
        assert!(record_reader.next().is_none());

        // TODO(Sky): Avoid extra copy.
        let summary = data[blocks_size..blocks_size + summary_size as usize].to_vec();
        let summary = decompress(&compression, summary)?;
        let summary = decode(&encoding, &summary)?;
        Ok(Arc::new(ColumnOrientedSegment {
            block_metas,
            summary,
        }))
    }
}

pub struct BlockMetaIter {
    blocks: RecordBatch,
    index: usize,
}

impl BlockMetaIter {
    pub fn new(blocks: RecordBatch) -> Self {
        Self { blocks, index: 0 }
    }
}

impl Iterator for BlockMetaIter {
    type Item = Arc<dyn AbstractBlockMeta>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.blocks.num_rows() {
            return None;
        }
        let index = self.index;
        self.index += 1;
        Some(Arc::new(ColumnOrientedBlockMeta::new(
            self.blocks.clone(),
            index,
        )))
    }
}
