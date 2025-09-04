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
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_json::reader::ValueIter;
use arrow_schema::ArrowError;
use arrow_schema::Schema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_common_storages_stage::BytesBatch;

use crate::table_functions::infer_schema::merge::merge_schema;

pub struct InferSchemaSeparator {
    pub file_format_params: FileFormatParams,
    files: HashMap<String, Vec<u8>>,
    pub max_records: Option<usize>,
    schemas: Vec<Schema>,
    files_len: usize,
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
            schemas: Vec::with_capacity(files_len),
            files_len,
            is_finished: false,
        }
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

        let bytes = self.files.entry(batch.path.clone()).or_insert(Vec::new());
        bytes.extend(batch.data);

        // When max_records exists, it will try to use the current bytes to read, otherwise it will buffer all bytes
        if self.max_records.is_none() && !batch.is_eof {
            return Ok(vec![DataBlock::empty()]);
        }
        let bytes = Cursor::new(bytes);
        let result = match &self.file_format_params {
            FileFormatParams::Csv(params) => {
                let escape = if params.escape.is_empty() {
                    None
                } else {
                    Some(params.escape.as_bytes()[0])
                };

                let mut format = Format::default()
                    .with_delimiter(params.field_delimiter.as_bytes()[0])
                    .with_quote(params.quote.as_bytes()[0])
                    .with_header(params.headers != 0);
                if let Some(escape) = escape {
                    format = format.with_escape(escape);
                }
                format
                    .infer_schema(bytes, self.max_records)
                    .map(|(schema, _)| schema)
                    .map_err(Some)
            }
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
                    "InferSchemaSeparator is currently limited to format CSV and NDJSON",
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
        self.schemas.push(arrow_schema);

        if self.schemas.len() != self.files_len {
            return Ok(vec![DataBlock::empty()]);
        }
        self.is_finished = true;
        if self.schemas.len() == 0 {
            return Ok(vec![DataBlock::empty()]);
        }
        let table_schema = if self.schemas.len() == 1 {
            TableSchema::try_from(&self.schemas.pop().unwrap())?
        } else {
            self.schemas[1..]
                .iter()
                .try_fold(TableSchema::try_from(&self.schemas[0])?, |acc, schema| {
                    TableSchema::try_from(schema).map(|schema| merge_schema(acc, schema))
                })?
        };

        let mut names: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        let mut nulls: Vec<bool> = vec![];

        for field in table_schema.fields().iter() {
            names.push(field.name().to_string());

            let non_null_type = field.data_type().remove_recursive_nullable();
            types.push(non_null_type.sql_name());
            nulls.push(field.is_nullable());
        }

        let order_ids = (0..table_schema.fields().len() as u64).collect::<Vec<_>>();

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            BooleanType::from_data(nulls),
            UInt64Type::from_data(order_ids),
        ]);
        Ok(vec![block])
    }
}
