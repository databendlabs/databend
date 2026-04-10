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
use std::sync::LazyLock;

use arrow_json::reader::ValueIter;
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::Schema;
use csv_core::ReadRecordResult;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableSchema;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_formats::RecordDelimiter;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::TextFileFormatParams;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_query_storage_stage_support::BytesBatch;
use databend_query_storage_stage_support::parse_tsv_records_for_infer_schema;
use itertools::Itertools;
use regex::RegexSet;

use crate::table_functions::infer_schema::merge::merge_schema;

const MAX_SINGLE_FILE_BYTES: usize = 100 * 1024 * 1024;
const MAX_CSV_COLUMNS: usize = 1024;
static TEXT_INFER_REGEX_SET: LazyLock<RegexSet> = LazyLock::new(|| {
    RegexSet::new([
        r"(?i)^(true)$|^(false)$(?-i)", // BOOLEAN
        r"^-?(\d+)$",                   // INTEGER
        r"^-?((\d*\.\d+|\d+\.\d*)([eE][-+]?\d+)?|\d+([eE][-+]?\d+))$", // DECIMAL
        r"^\d{4}-\d\d-\d\d$",           // DATE32
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:[^\d\.].*)?$", // Timestamp(Second)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.\d{1,3}(?:[^\d].*)?$", // Timestamp(Millisecond)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.\d{1,6}(?:[^\d].*)?$", // Timestamp(Microsecond)
        r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.\d{1,9}(?:[^\d].*)?$", // Timestamp(Nanosecond)
    ])
    .expect("failed to build tsv infer regex set")
});

#[derive(Default, Copy, Clone)]
struct TextInferredDataType {
    // 0:Boolean,1:Integer,2:Float64,3:Date32,4:TsS,5:TsMS,6:TsUS,7:TsNS,8:Utf8
    packed: u16,
}

impl TextInferredDataType {
    fn get(&self) -> DataType {
        match self.packed {
            0 => DataType::Null,
            1 => DataType::Boolean,
            2 => DataType::Int64,
            4 | 6 => DataType::Float64,
            b if b != 0 && (b & !0b11111000) == 0 => match b.leading_zeros() {
                8 => DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                9 => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                10 => DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                11 => DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                12 => DataType::Date32,
                _ => unreachable!(),
            },
            _ => DataType::Utf8,
        }
    }

    fn update(&mut self, field: &[u8]) {
        if field.is_empty() {
            return;
        }

        let Ok(string) = std::str::from_utf8(field) else {
            self.packed |= 1 << 8;
            return;
        };

        self.packed |= if let Some(m) = TEXT_INFER_REGEX_SET.matches(string).into_iter().next() {
            if m == 1 && string.len() >= 19 && string.parse::<i64>().is_err() {
                1 << 8
            } else {
                1 << m
            }
        } else if string == "NaN" || string == "nan" || string == "inf" || string == "-inf" {
            1 << 2
        } else {
            1 << 8
        };
    }
}

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

    fn process_csv_record(
        params: &CsvFileFormatParams,
        row: &[u8],
        ends: &[usize],
        headers: &mut Option<Vec<String>>,
        column_types: &mut Vec<TextInferredDataType>,
        expected_num_fields: &mut Option<usize>,
    ) -> std::result::Result<bool, ArrowError> {
        if let Some(expected) = *expected_num_fields {
            if ends.len() != expected {
                return Err(ArrowError::CsvError(format!(
                    "incorrect number of fields in CSV record: expected {expected}, got {}",
                    ends.len()
                )));
            }
        }

        let mut field_start = 0usize;
        let mut fields = Vec::with_capacity(ends.len());
        for field_end in ends {
            let field = &row[field_start..*field_end];
            let field = if params.trim_space {
                trim_ascii_space(field)
            } else {
                field
            };
            fields.push(field);
            field_start = *field_end;
        }

        if headers.is_none() {
            *expected_num_fields = Some(fields.len());
            *headers = Some(if params.headers != 0 {
                fields
                    .iter()
                    .map(|f| String::from_utf8_lossy(f).to_string())
                    .collect::<Vec<_>>()
            } else {
                (0..fields.len())
                    .map(|i| format!("column_{}", i + 1))
                    .collect::<Vec<_>>()
            });
            *column_types = vec![TextInferredDataType::default(); fields.len()];

            if params.headers != 0 {
                return Ok(false);
            }
        }

        for (idx, inferred) in column_types.iter_mut().enumerate() {
            if let Some(field) = fields.get(idx) {
                inferred.update(field);
            }
        }
        Ok(true)
    }

    fn infer_csv_schema(
        bytes: &[u8],
        params: &CsvFileFormatParams,
        is_eof: bool,
        max_records: Option<usize>,
    ) -> std::result::Result<Schema, Option<ArrowError>> {
        let escape = if params.escape.is_empty() {
            None
        } else {
            Some(params.escape.as_bytes()[0])
        };
        let terminator = match params.record_delimiter.as_str().try_into() {
            Ok(RecordDelimiter::Crlf) => csv_core::Terminator::CRLF,
            Ok(RecordDelimiter::Any(v)) => csv_core::Terminator::Any(v),
            Err(err) => return Err(Some(ArrowError::ParseError(err.message()))),
        };

        let mut reader = csv_core::ReaderBuilder::new()
            .delimiter_bytes(params.field_delimiter.as_bytes())
            .quote(params.quote.as_bytes()[0])
            .escape(escape)
            .terminator(terminator)
            .build();

        let max_records = max_records.unwrap_or(usize::MAX);
        let mut headers: Option<Vec<String>> = None;
        let mut column_types = vec![];
        let mut expected_num_fields = None;
        let mut rows_seen = 0usize;

        let mut buf_in = bytes;
        let mut flush_on_eof = is_eof;
        let mut buf_out = vec![0u8; bytes.len().max(1)];
        let mut buf_out_pos = 0usize;
        let mut field_ends = vec![0usize; MAX_CSV_COLUMNS];
        let mut n_end = 0usize;

        loop {
            let input = if !buf_in.is_empty() {
                buf_in
            } else if flush_on_eof {
                flush_on_eof = false;
                &[]
            } else {
                break;
            };

            let (result, n_in, n_out, new_ends) =
                reader.read_record(input, &mut buf_out[buf_out_pos..], &mut field_ends[n_end..]);
            n_end += new_ends;

            match result {
                ReadRecordResult::InputEmpty => {
                    if input.is_empty() {
                        return Err(Some(ArrowError::CsvError("unexpected eof".to_string())));
                    }
                    buf_out_pos += n_out;
                }
                ReadRecordResult::OutputFull => {
                    return Err(Some(ArrowError::CsvError(
                        "csv output buffer full when inferring schema".to_string(),
                    )));
                }
                ReadRecordResult::OutputEndsFull => {
                    return Err(Some(ArrowError::CsvError(
                        "csv field buffer full when inferring schema".to_string(),
                    )));
                }
                ReadRecordResult::Record => {
                    let row_end = buf_out_pos + n_out;
                    let counted = Self::process_csv_record(
                        params,
                        &buf_out[..row_end],
                        &field_ends[..n_end],
                        &mut headers,
                        &mut column_types,
                        &mut expected_num_fields,
                    )
                    .map_err(Some)?;
                    buf_out_pos = 0;
                    n_end = 0;
                    if counted {
                        rows_seen += 1;
                    }

                    if rows_seen >= max_records {
                        break;
                    }
                }
                ReadRecordResult::End => {
                    if !input.is_empty() {
                        return Err(Some(ArrowError::CsvError("unexpected eof".to_string())));
                    }
                    buf_out_pos += n_out;
                }
            }

            if !input.is_empty() {
                buf_in = &buf_in[n_in..];
            }
        }

        if (!buf_in.is_empty() || buf_out_pos != 0 || n_end != 0) && rows_seen < max_records {
            return Err(Some(ArrowError::CsvError("unexpected eof".to_string())));
        }

        let Some(headers) = headers else {
            return Err(None);
        };

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
        let result = match &self.file_format_params {
            FileFormatParams::Csv(params) => {
                Self::infer_csv_schema(file_bytes, params, batch.is_eof, self.max_records)
            }
            FileFormatParams::Text(params) => {
                Self::infer_tsv_schema(file_bytes, params, batch.is_eof, self.max_records)
            }
            FileFormatParams::NdJson(_) => {
                let mut records = ValueIter::new(Cursor::new(file_bytes), self.max_records);
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
                    "InferSchemaSeparator is currently limited to format CSV, TEXT and NDJSON",
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

fn trim_ascii_space(data: &[u8]) -> &[u8] {
    data.trim_ascii()
}

#[cfg(test)]
mod tests {
    use databend_common_meta_app::principal::CsvFileFormatParams;
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

    #[test]
    fn test_infer_csv_schema_trim_space_numbers() {
        let params = CsvFileFormatParams {
            trim_space: true,
            ..CsvFileFormatParams::default()
        };
        let schema =
            InferSchemaSeparator::infer_csv_schema(b" 42 , 123 \n", &params, true, None).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_infer_csv_schema_trim_space_header_names() {
        let params = CsvFileFormatParams {
            headers: 1,
            trim_space: true,
            ..CsvFileFormatParams::default()
        };
        let schema = InferSchemaSeparator::infer_csv_schema(
            b"  id  ,  value  \n 42 , hello \n",
            &params,
            true,
            None,
        )
        .unwrap();

        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_infer_csv_schema_trim_space_quoted_values() {
        let params = CsvFileFormatParams {
            trim_space: true,
            ..CsvFileFormatParams::default()
        };
        let schema =
            InferSchemaSeparator::infer_csv_schema(b"\" 42 \",\" 3.14 \"\n", &params, true, None)
                .unwrap();

        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
    }
}
