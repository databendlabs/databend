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

use std::collections::VecDeque;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use bytes::Buf;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::traits::Location;
use opendal::Operator;

use super::Layout;
use super::serialize::deserialize_block;

pub struct BlocksReader<'a> {
    read_bytes: u64,
    current: usize,
    location_offset: usize,
    offsets: VecDeque<u64>,
    locations: &'a [(Location, usize, usize)],
    operator: Operator,
}

impl<'a> BlocksReader<'a> {
    pub fn new(locations: &'a [(Location, usize, usize)], operator: Operator) -> Self {
        BlocksReader {
            operator,
            locations,
            current: 0,
            read_bytes: 0,
            location_offset: 0,
            offsets: VecDeque::new(),
        }
    }

    pub async fn read(&mut self) -> Result<Option<DataBlock>> {
        if self.offsets.is_empty() {
            self.advance_location().await?;
        }

        if let Some(offset) = self.offsets.pop_front() {
            let (Location::Remote(location), _, _) = &self.locations[self.current] else {
                unreachable!();
            };

            let data = self
                .operator
                .read_with(location)
                .range(self.read_bytes..offset)
                .await?;

            let data_block = deserialize_block(&Layout::Parquet, data)?;

            self.read_bytes = offset;
            return Ok(Some(data_block));
        }

        Ok(None)
    }

    async fn advance_location(&mut self) -> Result<()> {
        if self.location_offset >= self.locations.len() {
            return Ok(());
        }

        let (location, data_size, block_size) = &self.locations[self.location_offset];

        let Location::Remote(location) = location else {
            unreachable!()
        };

        let offset = (*data_size - (*block_size * size_of::<u64>())) as u64;
        let bytes = self
            .operator
            .read_with(location)
            .range(offset..(*data_size as u64))
            .await?;

        let mut offset_reader = bytes.reader();
        for _index in 0..*block_size {
            let offset = offset_reader.read_u64::<BigEndian>()?;
            self.offsets.push_back(offset);
        }

        self.read_bytes = 0;
        self.current = self.location_offset;
        self.location_offset += 1;
        Ok(())
    }
}
