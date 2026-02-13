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

use std::collections::HashMap;
use std::io::Cursor;

use arrow_csv::reader::Format;
use arrow_json::reader::ValueIter;
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_schema::ArrowError;
use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableSchema;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_storages_stage::BytesBatch;
use itertools::Itertools;

use crate::table_functions::infer_schema::merge::merge_schema;

const MAX_SINGLE_FILE_BYTES: usize = 100 * 1024 * 1024;

pub struct InferSchemaSeparator {
    pub file_format_params: FileFormatParams,
    files: HashMap<String, Vec<u8>>,
    pub max_records: Option<usize>,
    schemas: Option<TableSchema>,
    files_len: usize,
    filenames: Vec<String>,
    is_finished: bool,
}

impl InferSchemaSeparator {
    pub fn create(
        file_format_params: FileFormatParams,
        max_records: Option<usize>,
        files_len: usize,
    ) -> Self {
        InferSchemaSeparator {
            file_format_params,
            files: HashMap::new(),
            max_records,
            schemas: None,
            files_len,
            filenames: Vec::with_capacity(files_len),
            is_finished: false,
        }
    }

    fn infer_delimited_schema(
        bytes: Cursor<&[u8]>,
        field_delimiter: &str,
        quote: &str,
        headers: u64,
        escape: &str,
        max_records: Option<usize>,
    ) -> std::result::Result<Schema, Option<ArrowError>> {
        let mut format = Format::default()
            .with_delimiter(field_delimiter.as_bytes()[0])
            .with_quote(quote.as_bytes()[0])
            .with_header(headers != 0);

        if !escape.is_empty() {
            format = format.with_escape(escape.as_bytes()[0]);
        }

        format
            .infer_schema(bytes, max_records)
            .map(|(schema, _)| schema)
            .map_err(Some)
    }
}

impl AccumulatingTransform for InferSchemaSeparator {
    const NAME: &'static str = "InferSchemaSeparator";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        if self.is_finished {
            return Ok(vec![DataBlock::empty()]);
        }
        let batch = data
            .get_owned_meta()
            .and_then(BytesBatch::downcast_from)
            .unwrap();

        let bytes = self.files.entry(batch.path.clone()).or_default();
        bytes.extend(batch.data);

        if bytes.len() > MAX_SINGLE_FILE_BYTES {
            return Err(ErrorCode::InvalidArgument(format!(
                "The file '{}' is too large(maximum allowed: {})",
                batch.path,
                human_readable_size(MAX_SINGLE_FILE_BYTES),
            )));
        }

        // When max_records exists, it will try to use the current bytes to read, otherwise it will buffer all bytes
        if self.max_records.is_none() && !batch.is_eof {
            return Ok(vec![DataBlock::empty()]);
        }
        let bytes = Cursor::new(bytes.as_slice());
        let result = match &self.file_format_params {
            FileFormatParams::Csv(params) => Self::infer_delimited_schema(
                bytes,
                &params.field_delimiter,
                &params.quote,
                params.headers,
                &params.escape,
                self.max_records,
            ),
            FileFormatParams::Tsv(params) => Self::infer_delimited_schema(
                bytes,
                &params.field_delimiter,
                &params.quote,
                params.headers,
                &params.escape,
                self.max_records,
            ),
            FileFormatParams::NdJson(_) => {
                let mut records = ValueIter::new(bytes, self.max_records);
                let fn_ndjson = |max_records| -> std::result::Result<Schema, Option<ArrowError>> {
                    if let Some(max_record) = max_records {
                        let mut tmp: Vec<std::result::Result<_, ArrowError>> =
                            Vec::with_capacity(max_record);

                        for result in records {
                            tmp.push(Ok(result.map_err(|_| None)?));
                        }
                        infer_json_schema_from_iterator(tmp.into_iter()).map_err(Some)
                    } else {
                        infer_json_schema_from_iterator(&mut records).map_err(Some)
                    }
                };
                fn_ndjson(self.max_records)
            }
            _ => {
                return Err(ErrorCode::BadArguments(
                    "InferSchemaSeparator is currently limited to format CSV, TSV and NDJSON",
                ));
            }
        };
        let arrow_schema = match result {
            Ok(schema) => schema,
            Err(None) => return Ok(vec![DataBlock::empty()]),
            Err(Some(err)) => {
                if matches!(err, ArrowError::CsvError(_))
                    && self.max_records.is_some()
                    && !batch.is_eof
                {
                    return Ok(vec![DataBlock::empty()]);
                }
                return Err(err.into());
            }
        };
        self.files.remove(&batch.path);
        self.filenames.push(batch.path);

        let merge_schema = match self.schemas.take() {
            None => TableSchema::try_from(&arrow_schema)?,
            Some(schema) => merge_schema(schema, TableSchema::try_from(&arrow_schema)?),
        };
        self.schemas = Some(merge_schema);

        if self.files_len > self.filenames.len() {
            return Ok(vec![DataBlock::empty()]);
        }
        self.is_finished = true;
        let Some(table_schema) = self.schemas.take() else {
            return Ok(vec![DataBlock::empty()]);
        };

        let mut names: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        let mut nulls: Vec<bool> = vec![];
        let mut filenames: Vec<String> = vec![];
        let filenames_str = self.filenames.iter().join(", ");

        for field in table_schema.fields().iter() {
            names.push(field.name().to_string());

            let non_null_type = field.data_type().remove_recursive_nullable();
            types.push(non_null_type.sql_name());
            nulls.push(field.is_nullable());
            filenames.push(filenames_str.clone());
        }

        let order_ids = (0..table_schema.fields().len() as u64).collect::<Vec<_>>();

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            BooleanType::from_data(nulls),
            StringType::from_data(filenames),
            UInt64Type::from_data(order_ids),
        ]);
        Ok(vec![block])
    }
}

fn human_readable_size(bytes: usize) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GB", b / GB)
    } else if b >= MB {
        format!("{:.2} MB", b / MB)
    } else if b >= KB {
        format!("{:.2} KB", b / KB)
    } else {
        format!("{} B", bytes)
    }
}
