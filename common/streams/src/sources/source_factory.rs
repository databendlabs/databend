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

use std::collections::HashMap;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::AsyncRead;
use futures::AsyncSeek;

use crate::CsvSource;
use crate::ParquetSource;
use crate::Source;

pub struct SourceFactory {}

pub struct SourceParams<'a, R>
where R: AsyncRead + Unpin + Send
{
    pub reader: R,
    pub path: &'a str,
    pub format: &'a str,
    pub schema: DataSchemaRef,
    pub max_block_size: usize,
    pub projection: Vec<usize>,
    pub options: &'a HashMap<String, String>,
}

impl SourceFactory {
    pub fn try_get<R>(params: SourceParams<R>) -> Result<Box<dyn Source>>
    where R: AsyncRead + AsyncSeek + Unpin + Send + 'static {
        let format = params.format.to_lowercase();
        match format.as_str() {
            "csv" => {
                let has_header = params
                    .options
                    .get("csv_header")
                    .cloned()
                    .unwrap_or_else(|| "0".to_string());

                let field_delimitor = params
                    .options
                    .get("field_delimitor")
                    .map(|v| match v.len() {
                        n if n >= 1 => v.as_bytes()[0],
                        _ => b',',
                    })
                    .unwrap_or(b',');

                let record_delimitor = params
                    .options
                    .get("record_delimitor")
                    .map(|v| match v.len() {
                        n if n >= 1 => v.as_bytes()[0],
                        _ => b'\n',
                    })
                    .unwrap_or(b'\n');

                Ok(Box::new(CsvSource::try_create(
                    params.reader,
                    params.schema,
                    has_header.eq_ignore_ascii_case("1"),
                    field_delimitor,
                    record_delimitor,
                    params.max_block_size,
                )?))
            }
            "parquet" => Ok(Box::new(ParquetSource::new(
                params.reader,
                params.schema,
                params.projection,
            ))),
            _ => Err(ErrorCode::InvalidSourceFormat(format)),
        }
    }
}
