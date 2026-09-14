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

use arrow_schema::ArrowError;
use arrow_schema::Field;
use arrow_schema::Fields;
use arrow_schema::Schema;
use csv_core::ReadRecordResult;
use databend_common_formats::RecordDelimiter;
use databend_common_meta_app::principal::CsvFileFormatParams;

use crate::table_functions::infer_schema::text::TextInferredDataType;

const MAX_CSV_COLUMNS: usize = 1024;

fn trim_ascii_space(data: &[u8]) -> &[u8] {
    data.trim_ascii()
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

pub(super) fn infer_csv_schema(
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
                let counted = process_csv_record(
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

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;
    use databend_common_meta_app::principal::CsvFileFormatParams;

    use super::infer_csv_schema;

    #[test]
    fn test_infer_csv_schema_trim_space_numbers() {
        let params = CsvFileFormatParams {
            trim_space: true,
            ..CsvFileFormatParams::default()
        };
        let schema = infer_csv_schema(b" 42 , 123 \n", &params, true, None).unwrap();

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
        let schema =
            infer_csv_schema(b"  id  ,  value  \n 42 , hello \n", &params, true, None).unwrap();

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
        let schema = infer_csv_schema(b"\" 42 \",\" 3.14 \"\n", &params, true, None).unwrap();

        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
    }
}
