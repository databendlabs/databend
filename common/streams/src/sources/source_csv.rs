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

use async_trait::async_trait;
use chrono_tz::Tz;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::TypeDeserializer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_io::prelude::FormatSettings;
use common_tracing::tracing::info;
use csv_async::AsyncReader;
use csv_async::AsyncReaderBuilder;
use csv_async::Terminator;
use futures::stream::StreamExt;
use futures::AsyncRead;

use crate::Source;

#[derive(Debug, Clone)]
pub struct CsvSourceBuilder {
    schema: DataSchemaRef,
    skip_header: bool,
    empty_as_default: bool,
    block_size: usize,
    size_limit: usize,
    field_delimiter: u8,
    record_delimiter: Terminator,
    tz: Tz,
}

impl CsvSourceBuilder {
    pub fn create(schema: DataSchemaRef, format_settings: FormatSettings) -> Self {
        let field_delimiter = match format_settings.field_delimiter.len() {
            n if n >= 1 => format_settings.field_delimiter[0],
            _ => b',',
        };
        let record_delimiter = match format_settings.record_delimiter.len() {
            n if n >= 1 => format_settings.record_delimiter[0],
            _ => b'\n',
        };

        let record_delimiter = if record_delimiter == b'\n' || record_delimiter == b'\r' {
            Terminator::CRLF
        } else {
            Terminator::Any(record_delimiter)
        };

        let empty_as_default = format_settings.empty_as_default;
        let skip_header = format_settings.skip_header;
        let tz = format_settings.timezone;

        CsvSourceBuilder {
            schema,
            skip_header,
            field_delimiter,
            record_delimiter,
            empty_as_default,
            block_size: 10000,
            size_limit: usize::MAX,
            tz,
        }
    }

    pub fn block_size(&mut self, block_size: usize) -> &mut Self {
        self.block_size = block_size;
        self
    }

    pub fn size_limit(&mut self, size_limit: usize) -> &mut Self {
        self.size_limit = size_limit;
        self
    }

    // Whether to skip the header
    pub fn skip_header(&mut self, skip_header: bool) -> &mut Self {
        self.skip_header = skip_header;
        self
    }

    pub fn field_delimiter(&mut self, field_delimiter_str: &str) -> &mut Self {
        if !field_delimiter_str.is_empty() {
            let field_delimiter = match field_delimiter_str.len() {
                n if n >= 1 => field_delimiter_str.as_bytes()[0],
                _ => b',',
            };
            self.field_delimiter = field_delimiter;
        }
        self
    }

    pub fn record_delimiter(&mut self, record_delimiter_str: &str) -> &mut Self {
        if !record_delimiter_str.is_empty() {
            let record_delimiter = match record_delimiter_str.len() {
                n if n >= 1 => record_delimiter_str.as_bytes()[0],
                _ => b'\n',
            };

            let record_delimiter = if record_delimiter == b'\n' || record_delimiter == b'\r' {
                Terminator::CRLF
            } else {
                Terminator::Any(record_delimiter)
            };
            self.record_delimiter = record_delimiter;
        }
        self
    }

    pub fn build<R>(&self, reader: R) -> Result<CsvSource<R>>
    where R: AsyncRead + Unpin + Send {
        CsvSource::try_create(self.clone(), reader)
    }
}

pub struct CsvSource<R> {
    builder: CsvSourceBuilder,
    reader: AsyncReader<R>,
    rows: usize,
}

impl<R> CsvSource<R>
where R: AsyncRead + Unpin + Send
{
    fn try_create(builder: CsvSourceBuilder, reader: R) -> Result<Self> {
        let reader = AsyncReaderBuilder::new()
            .has_headers(builder.skip_header)
            .delimiter(builder.field_delimiter)
            .terminator(builder.record_delimiter)
            .create_reader(reader);

        Ok(Self {
            builder,
            reader,
            rows: 0,
        })
    }
}

#[async_trait]
impl<R> Source for CsvSource<R>
where R: AsyncRead + Unpin + Send
{
    async fn read(&mut self) -> Result<Option<DataBlock>> {
        // Check size_limit.
        if self.rows >= self.builder.size_limit {
            return Ok(None);
        }

        let mut packs = self
            .builder
            .schema
            .fields()
            .iter()
            .map(|f| f.data_type().create_deserializer(self.builder.block_size))
            .collect::<Vec<_>>();

        let mut rows = 0;
        let mut records = self.reader.byte_records();

        while let Some(record) = records.next().await {
            let record = record.map_err_to_code(ErrorCode::BadBytes, || {
                format!("Parse csv error at line {}", self.rows)
            })?;

            if record.is_empty() {
                break;
            }
            let format = FormatSettings {
                timezone: self.builder.tz,
                ..Default::default()
            };
            for (col, pack) in packs.iter_mut().enumerate() {
                match record.get(col) {
                    Some(bytes) => {
                        if bytes.is_empty() && self.builder.empty_as_default {
                            pack.de_default(&format);
                        } else {
                            pack.de_whole_text(bytes, &format)?
                        }
                    }
                    None => pack.de_default(&format),
                }
            }
            rows += 1;
            self.rows += 1;

            // Check size_limit.
            if self.rows >= self.builder.size_limit {
                break;
            }

            // Check block_size.
            if rows >= self.builder.block_size {
                break;
            }
        }

        if rows == 0 {
            return Ok(None);
        }

        let series = packs
            .iter_mut()
            .map(|deser| deser.finish_to_column())
            .collect::<Vec<_>>();

        Ok(Some(DataBlock::create(self.builder.schema.clone(), series)))
    }
}
