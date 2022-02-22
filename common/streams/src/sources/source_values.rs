// Copyright 2021 Datafuse Labs.
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

use std::io;
use std::io::BufReader;

use async_trait::async_trait;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_io::prelude::*;

use crate::Source;

pub struct ValueSource<R> {
    reader: BufReader<R>,
    schema: DataSchemaRef,
    block_size: usize,
    rows: usize,
}

impl<R> ValueSource<R>
where R: io::Read + Send + Sync
{
    pub fn new(reader: R, schema: DataSchemaRef, block_size: usize) -> Self {
        let reader = BufReader::new(reader);
        Self {
            reader,
            block_size,
            schema,
            rows: 0,
        }
    }
}

#[async_trait]
impl<R> Source for ValueSource<R>
where R: io::Read + Send + Sync
{
    async fn read(&mut self) -> Result<Option<DataBlock>> {
        let reader = &mut self.reader;
        let mut buf = Vec::new();
        let mut temp = Vec::new();

        let mut desers = self
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_deserializer(self.block_size))
            .collect::<Vec<_>>();

        let col_size = desers.len();
        let mut rows = 0;
        for _row in 0..self.block_size {
            let _ = reader.ignore_spaces()?;
            if reader.buffer().is_empty() {
                break;
            }
            // not the first row
            if rows + self.rows != 0 {
                reader.until(b',', &mut buf)?;
            }
            let _ = reader.ignore_spaces()?;
            let _ = reader.ignore_byte(b'(')?;

            for (col, deser) in desers.iter_mut().enumerate().take(col_size) {
                buf.clear();
                let _ = reader.ignore_spaces()?;

                let bs: Result<&[u8]> = {
                    if reader.ignore_byte(b'\'')? {
                        reader.until(b'\'', &mut buf)?;

                        let res = &buf.as_slice()[0..buf.len() - 1];
                        if col != col_size - 1 {
                            reader.until(b',', &mut temp)?;
                        } else {
                            reader.until(b')', &mut temp)?;
                        }
                        Ok(res)
                    } else if reader.ignore_byte(b'"')? {
                        reader.until(b'"', &mut buf)?;

                        let res = &buf.as_slice()[0..buf.len() - 1];
                        if col != col_size - 1 {
                            reader.until(b',', &mut temp)?;
                        } else {
                            reader.until(b')', &mut temp)?;
                        }
                        Ok(res)
                    } else if col != col_size - 1 {
                        reader.until(b',', &mut buf)?;
                        Ok(&buf.as_slice()[0..buf.len() - 1])
                    } else {
                        reader.until(b')', &mut buf)?;
                        Ok(&buf.as_slice()[0..buf.len() - 1])
                    }
                };
                let bs = bs?;
                deser.de_text(bs)?;
            }
            rows += 1;
        }

        if rows == 0 {
            return Ok(None);
        }
        self.rows += rows;
        let columns = desers
            .iter_mut()
            .map(|deser| deser.finish_to_column())
            .collect::<Vec<_>>();

        Ok(Some(DataBlock::create(self.schema.clone(), columns)))
    }
}
