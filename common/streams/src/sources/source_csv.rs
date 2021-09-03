// Copyright 2020 Datafuse Labs.
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
use std::sync::Arc;

use common_arrow::arrow::io::csv::read::ByteRecord;
use common_arrow::arrow::io::csv::read::Reader;
use common_arrow::arrow::io::csv::read::ReaderBuilder;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_infallible::RwLock;

use crate::Source;

pub struct CsvSource<R> {
    reader: Arc<RwLock<Reader<R>>>,
    schema: DataSchemaRef,
    block_size: usize,
    rows: usize,
}

impl<R> CsvSource<R>
where R: io::Read + Sync + Send
{
    pub fn new(reader: R, schema: DataSchemaRef, block_size: usize) -> Self {
        let reader = ReaderBuilder::new().has_headers(false).from_reader(reader);

        Self {
            reader: Arc::new(RwLock::new(reader)),
            block_size,
            schema,
            rows: 0,
        }
    }
}

impl<R> Source for CsvSource<R>
where R: io::Read + Sync + Send
{
    fn read(&mut self) -> Result<Option<DataBlock>> {
        let mut reader = self.reader.write();
        let mut record = ByteRecord::new();
        let mut desers = self
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_deserializer(self.block_size))
            .collect::<Result<Vec<_>>>()?;

        for row in 0..self.block_size {
            let v = reader
                .read_byte_record(&mut record)
                .map_err_to_code(ErrorCode::BadBytes, || {
                    format!("Parse csv error at line {}", self.rows)
                })?;

            if !v {
                if row == 0 {
                    return Ok(None);
                }
                break;
            }
            desers
                .iter_mut()
                .enumerate()
                .for_each(|(col, deser)| match record.get(col) {
                    Some(bytes) => deser.de_text(bytes),
                    None => deser.de_null(),
                });

            self.rows += 1;
        }

        let series = desers
            .iter_mut()
            .map(|deser| deser.finish_to_series())
            .collect::<Vec<_>>();

        Ok(Some(DataBlock::create_by_array(
            self.schema.clone(),
            series,
        )))
    }
}
