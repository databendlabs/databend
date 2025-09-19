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

use byteorder::BigEndian;
use byteorder::WriteBytesExt;
use bytes::BufMut;
use bytes::BytesMut;
use databend_common_base::base::dma_buffer_to_bytes;
use databend_common_base::base::Alignment;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::traits::Location;
use opendal::Buffer;
use opendal::Writer;

use crate::spillers::serialize::BlocksEncoder;

pub struct BlocksWriter {
    writer: Writer,
    location: Location,
    written: usize,
    offsets: Vec<usize>,
}

impl BlocksWriter {
    pub fn create(writer: Writer, location: Location) -> BlocksWriter {
        BlocksWriter {
            writer,
            location,
            written: 0,
            offsets: vec![],
        }
    }
    pub async fn write(&mut self, block: DataBlock) -> Result<()> {
        let mut block_encoder = BlocksEncoder::new(true, Alignment::MIN, 8 * 1024 * 1024);
        block_encoder.add_blocks(vec![block]);

        let buf = block_encoder
            .buf
            .into_data()
            .into_iter()
            .map(dma_buffer_to_bytes)
            .collect::<Buffer>();

        self.written += buf.len();
        self.writer.write(buf).await?;
        self.offsets.push(self.written);
        Ok(())
    }

    pub async fn close(mut self) -> Result<(Location, usize, usize)> {
        let bytes = BytesMut::new();

        let mut offset_writer = bytes.writer();
        let written_blocks = self.offsets.len();
        for offset in self.offsets {
            offset_writer.write_u64::<BigEndian>(offset as u64)?;
        }

        let bytes = offset_writer.into_inner().freeze();
        self.written += bytes.len();
        self.writer.write(bytes).await?;
        self.writer.close().await?;

        Ok((self.location, self.written, written_blocks))
    }
}
