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

use arrow_schema::ArrowError;
use arrow_schema::Field;
use arrow_schema::Fields;
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
use databend_common_meta_app::principal::TextFileFormatParams;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_query_storage_stage_support::BytesBatch;
use databend_query_storage_stage_support::parse_tsv_records_for_infer_schema;
use itertools::Itertools;

use crate::table_functions::infer_schema::csv::infer_csv_schema;
use crate::table_functions::infer_schema::json;
use crate::table_functions::infer_schema::merge::merge_schema;
use crate::table_functions::infer_schema::text::TextInferredDataType;

const MAX_SINGLE_FILE_BYTES: usize = 100 * 1024 * 1024;

pub struct InferSchemaSeparator {
    pub file_format_params: FileFormatParams,
    files: HashMap<String, Vec<u8>>,
    pub max_records: Option<usize>,
    schemas: Option<TableSchema>,
    json_schema: Option<json::InferredJsonSchema>,
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
            json_schema: None,
            files_len,
            filenames: Vec::with_capacity(files_len),
            is_finished: false,
        }
    }

    pub(crate) fn infer_ndjson_schema_state_with_max_depth(
        file_bytes: &[u8],
        max_records: Option<usize>,
        json_max_depth: usize,
    ) -> Result<json::InferredJsonSchema> {
        Ok(json::infer_ndjson_schema_state(
            file_bytes,
            max_records,
            Some(json_max_depth),
        )?)
    }

    fn infer_tsv_schema(
        bytes: &[u8],
        params: &TextFileFormatParams,
        is_eof: bool,
        max_records: Option<usize>,
    ) -> std::result::Result<Schema, Option<ArrowError>> {
        let mut fields = parse_tsv_records_for_infer_schema(bytes, params, is_eof)
            .map_err(|e| Some(ArrowError::ParseError(e.message())))?;

        let mut next_row_head: Option<(usize, String)> = None;
        let mut next_row = || -> std::result::Result<Option<Vec<String>>, ArrowError> {
            let first = if let Some(head) = next_row_head.take() {
                head
            } else {
                match fields.next() {
                    Some(item) => item.map_err(|e| ArrowError::ParseError(e.message()))?,
                    None => return Ok(None),
                }
            };

            let (first_idx, first_field) = first;
            if first_idx != 0 {
                return Err(ArrowError::ParseError(
                    "invalid TEXT field index stream when inferring schema".to_string(),
                ));
            }

            let mut row = vec![first_field];
            loop {
                match fields.next() {
                    Some(Ok((0, field))) => {
                        next_row_head = Some((0, field));
                        return Ok(Some(row));
                    }
                    Some(Ok((idx, field))) => {
                        if idx == row.len() {
                            row.push(field);
                        } else if idx < row.len() {
                            row[idx] = field;
                        } else {
                            row.resize(idx, String::new());
                            row.push(field);
                        }
                    }
                    Some(Err(e)) => return Err(ArrowError::ParseError(e.message())),
                    None => return Ok(Some(row)),
                }
            }
        };

        let first_record = next_row().map_err(Some)?.ok_or(None)?;
        let mut rows_seen = 0;
        let headers = if params.headers != 0 {
            first_record
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
        } else {
            (0..first_record.len())
                .map(|i| format!("column_{}", i + 1))
                .collect::<Vec<_>>()
        };

        let mut column_types = vec![TextInferredDataType::default(); headers.len()];
        let max_records = max_records.unwrap_or(usize::MAX);
        if params.headers == 0 && rows_seen < max_records {
            for (idx, t) in column_types.iter_mut().enumerate().take(headers.len()) {
                if let Some(field) = first_record.get(idx) {
                    t.update(field.as_bytes());
                }
            }
            rows_seen += 1;
        }

        while rows_seen < max_records {
            let Some(record) = next_row().map_err(Some)? else {
                break;
            };
            if rows_seen >= max_records {
                break;
            }
            for (idx, t) in column_types.iter_mut().enumerate().take(headers.len()) {
                if let Some(field) = record.get(idx) {
                    t.update(field.as_bytes());
                }
            }
            rows_seen += 1;
        }

        let fields: Fields = column_types
            .iter()
            .zip(headers.iter())
            .map(|(inferred, field_name)| Field::new(field_name, inferred.get(), true))
            .collect();
        Ok(Schema::new(fields))
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
        let file_bytes = bytes.as_slice();
        enum InferError {
            Incomplete,
            Arrow(ArrowError),
            Error(ErrorCode),
        }
        enum InferResult {
            Table(TableSchema),
            Json(json::InferredJsonSchema),
        }

        let result: std::result::Result<InferResult, InferError> = match &self.file_format_params {
            FileFormatParams::Csv(params) => {
                infer_csv_schema(file_bytes, params, batch.is_eof, self.max_records)
                    .map_err(|err| match err {
                        Some(err) => InferError::Arrow(err),
                        None => InferError::Incomplete,
                    })
                    .and_then(|schema| TableSchema::try_from(&schema).map_err(InferError::Error))
                    .map(InferResult::Table)
            }
            FileFormatParams::Text(params) => {
                Self::infer_tsv_schema(file_bytes, params, batch.is_eof, self.max_records)
                    .map_err(|err| match err {
                        Some(err) => InferError::Arrow(err),
                        None => InferError::Incomplete,
                    })
                    .and_then(|schema| TableSchema::try_from(&schema).map_err(InferError::Error))
                    .map(InferResult::Table)
            }
            FileFormatParams::NdJson(_) => {
                json::infer_ndjson_schema_state(file_bytes, self.max_records, None)
                    .map_err(InferError::Arrow)
                    .map(InferResult::Json)
            }
            _ => {
                return Err(ErrorCode::BadArguments(
                    "InferSchemaSeparator is currently limited to format CSV, TEXT and NDJSON",
                ));
            }
        };
        let inferred = match result {
            Ok(inferred) => inferred,
            Err(InferError::Incomplete) => return Ok(vec![DataBlock::empty()]),
            Err(InferError::Arrow(err)) => {
                if self.max_records.is_some()
                    && !batch.is_eof
                    && (matches!(err, ArrowError::CsvError(_))
                        || is_ndjson_incomplete_parse_error(
                            &err,
                            file_bytes,
                            &self.file_format_params,
                        ))
                {
                    return Ok(vec![DataBlock::empty()]);
                }
                return Err(err.into());
            }
            Err(InferError::Error(err)) => return Err(err),
        };
        self.files.remove(&batch.path);
        self.filenames.push(batch.path);

        match inferred {
            InferResult::Table(table_schema) => {
                let merge_schema = match self.schemas.take() {
                    None => table_schema,
                    Some(schema) => merge_schema(schema, table_schema),
                };
                self.schemas = Some(merge_schema);
            }
            InferResult::Json(json_schema) => match self.json_schema.as_mut() {
                Some(schema) => schema.merge(json_schema),
                None => self.json_schema = Some(json_schema),
            },
        }

        if self.files_len > self.filenames.len() {
            return Ok(vec![DataBlock::empty()]);
        }
        self.is_finished = true;
        let table_schema = if matches!(self.file_format_params, FileFormatParams::NdJson(_)) {
            let Some(json_schema) = self.json_schema.take() else {
                return Ok(vec![DataBlock::empty()]);
            };
            json_schema.into_table_schema()
        } else {
            let Some(table_schema) = self.schemas.take() else {
                return Ok(vec![DataBlock::empty()]);
            };
            table_schema
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

fn is_ndjson_incomplete_parse_error(
    err: &ArrowError,
    file_bytes: &[u8],
    file_format_params: &FileFormatParams,
) -> bool {
    matches!(file_format_params, FileFormatParams::NdJson(_))
        && matches!(err, ArrowError::JsonError(_))
        && !matches!(file_bytes.last(), Some(b'\n' | b'\r'))
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use databend_common_meta_app::principal::NdJsonFileFormatParams;
    use databend_common_meta_app::principal::TextFileFormatParams;

    use super::*;

    #[test]
    fn test_infer_tsv_schema_with_custom_record_delimiter() {
        let params = TextFileFormatParams {
            record_delimiter: "|".to_string(),
            ..TextFileFormatParams::default()
        };
        let schema =
            InferSchemaSeparator::infer_tsv_schema("1\t2|3\t4|".as_bytes(), &params, true, None)
                .unwrap();

        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn test_infer_tsv_schema_with_escaped_delimiters() {
        let params = TextFileFormatParams::default();
        let schema =
            InferSchemaSeparator::infer_tsv_schema(b"1\\t2\t3\\n4\n5\t6\n", &params, true, None)
                .unwrap();

        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn test_infer_tsv_schema_all_supported_types() {
        let params = TextFileFormatParams::default();
        let schema = InferSchemaSeparator::infer_tsv_schema(
            b"true\t42\t3.14\t2024-01-02\t2024-01-02 03:04:05\t2024-01-02 03:04:05.123\t2024-01-02 03:04:05.123456\t2024-01-02 03:04:05.123456789\thello\n",
            &params,
            true,
            None,
        )
        .unwrap();

        assert_eq!(schema.fields().len(), 9);
        assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
        assert_eq!(schema.field(3).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(4).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Second, None)
        );
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
        );
        assert_eq!(
            schema.field(6).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            schema.field(7).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
        assert_eq!(schema.field(8).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_infer_tsv_schema_header_names() {
        let params = TextFileFormatParams {
            headers: 1,
            ..Default::default()
        };
        let schema = InferSchemaSeparator::infer_tsv_schema(
            b"c_bool\tc_int\tc_float\tc_date\tc_ts_s\tc_ts_ms\tc_ts_us\tc_ts_ns\tc_str\ntrue\t42\t3.14\t2024-01-02\t2024-01-02 03:04:05\t2024-01-02 03:04:05.123\t2024-01-02 03:04:05.123456\t2024-01-02 03:04:05.123456789\thello\n",
            &params,
            true,
            None,
        )
        .unwrap();

        assert_eq!(schema.field(0).name(), "c_bool");
        assert_eq!(schema.field(1).name(), "c_int");
        assert_eq!(schema.field(2).name(), "c_float");
        assert_eq!(schema.field(8).name(), "c_str");
    }

    #[test]
    fn test_infer_tsv_schema_max_records() {
        let params = TextFileFormatParams::default();
        let schema =
            InferSchemaSeparator::infer_tsv_schema(b"1\ntext\n", &params, true, Some(1)).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);

        let schema =
            InferSchemaSeparator::infer_tsv_schema(b"1\ntext\n", &params, true, None).unwrap();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_infer_ndjson_incomplete_parse_error_waits_for_next_batch() -> Result<()> {
        let mut separator = InferSchemaSeparator::create(
            FileFormatParams::NdJson(NdJsonFileFormatParams::default()),
            Some(2),
            1,
        );
        let output = separator.transform(DataBlock::empty_with_meta(Box::new(BytesBatch {
            data: b"{\"a\":1}\n{\"a\"".to_vec(),
            path: "sample.ndjson".to_string(),
            offset: 0,
            is_eof: false,
            content_key: None,
            last_modified: None,
        })))?;
        assert_eq!(output.len(), 1);
        assert!(output[0].is_empty());

        let output = separator.transform(DataBlock::empty_with_meta(Box::new(BytesBatch {
            data: b":2}\n".to_vec(),
            path: "sample.ndjson".to_string(),
            offset: 13,
            is_eof: true,
            content_key: None,
            last_modified: None,
        })))?;
        assert_eq!(output.len(), 1);
        assert!(!output[0].is_empty());
        Ok(())
    }

    #[test]
    fn test_infer_tsv_table_schema_type_conversion() {
        let params = TextFileFormatParams::default();
        let arrow_schema = InferSchemaSeparator::infer_tsv_schema(
            b"true\t42\t3.14\t2024-01-02\t2024-01-02 03:04:05\t2024-01-02 03:04:05.123\t2024-01-02 03:04:05.123456\t2024-01-02 03:04:05.123456789\thello\n",
            &params,
            true,
            None,
        )
        .unwrap();
        let table_schema = TableSchema::try_from(&arrow_schema).unwrap();

        assert_eq!(table_schema.field(0).data_type().sql_name(), "BOOLEAN NULL");
        assert_eq!(table_schema.field(1).data_type().sql_name(), "BIGINT NULL");
        assert_eq!(table_schema.field(2).data_type().sql_name(), "DOUBLE NULL");
        assert_eq!(table_schema.field(3).data_type().sql_name(), "DATE NULL");
        assert_eq!(
            table_schema.field(4).data_type().sql_name(),
            "TIMESTAMP NULL"
        );
        assert_eq!(
            table_schema.field(5).data_type().sql_name(),
            "TIMESTAMP NULL"
        );
        assert_eq!(
            table_schema.field(6).data_type().sql_name(),
            "TIMESTAMP NULL"
        );
        assert_eq!(
            table_schema.field(7).data_type().sql_name(),
            "TIMESTAMP NULL"
        );
        assert_eq!(table_schema.field(8).data_type().sql_name(), "VARCHAR NULL");
    }
}
