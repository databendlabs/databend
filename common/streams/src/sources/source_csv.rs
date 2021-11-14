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

use async_trait::async_trait;
use common_base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use csv_async::AsyncReader;
use csv_async::AsyncReaderBuilder;
use tokio_stream::StreamExt;

use crate::Source;

pub struct CsvSource<R> {
    reader: AsyncReader<R>,
    schema: DataSchemaRef,
    block_size: usize,
    rows: usize,
}

impl<R> CsvSource<R>
where R: tokio::io::AsyncRead + Unpin + Send + Sync
{
    pub fn new(reader: R, schema: DataSchemaRef, header: bool, block_size: usize) -> Self {
        let reader = AsyncReaderBuilder::new()
            .has_headers(header)
            .create_reader(reader);

        Self {
            reader,
            block_size,
            schema,
            rows: 0,
        }
    }
}

#[async_trait]
impl<R> Source for CsvSource<R>
where R: tokio::io::AsyncRead + Unpin + Send + Sync
{
    async fn read(&mut self) -> Result<Option<DataBlock>> {
        let mut desers = self
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_serializer(self.block_size))
            .collect::<Result<Vec<_>>>()?;

        let mut rows = 0;
        let mut records = self.reader.byte_records();
        loop {
            if let Some(record) = records.next().await {
                let record = record.map_err_to_code(ErrorCode::BadBytes, || {
                    format!("Parse csv error at line {}", self.rows)
                })?;

                if record.is_empty() {
                    break;
                }
                for (col, deser) in desers.iter_mut().enumerate() {
                    match record.get(col) {
                        Some(bytes) => deser.de_text(bytes).unwrap(),
                        None => deser.de_null(),
                    }
                }
                rows += 1;
                self.rows += 1;

                if rows >= self.block_size {
                    break;
                }
            } else {
                break;
            }
        }

        if rows == 0 {
            return Ok(None);
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
