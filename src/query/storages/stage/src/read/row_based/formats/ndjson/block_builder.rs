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

use std::collections::HashSet;
use std::sync::Arc;

use bstr::ByteSlice;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_formats::FieldJsonAstDecoder;
use databend_common_formats::json::JsonObjectLookup;
use databend_common_formats::json::JsonValue;
use databend_common_meta_app::principal::NullAs;
use databend_common_storage::FileParseError;

use crate::read::block_builder_state::BlockBuilderState;
use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::RowDecoder;
use crate::read::row_based::formats::ndjson::format::NdJsonInputFormat;
use crate::read::row_based::utils::truncate_column_data;

pub struct NdJsonDecoder {
    pub load_context: Arc<LoadContext>,
    pub fmt: NdJsonInputFormat,
    pub field_decoder: FieldJsonAstDecoder,
    pub schema_field_names: HashSet<String>,
}

impl NdJsonDecoder {
    pub fn create(fmt: NdJsonInputFormat, load_context: Arc<LoadContext>) -> Self {
        let field_decoder =
            FieldJsonAstDecoder::create(&load_context.settings, load_context.is_select);
        let schema_field_names = load_context
            .schema
            .fields()
            .iter()
            .map(|field| normalize_field_name(field.name(), field_decoder.ident_case_sensitive))
            .collect();
        Self {
            load_context,
            fmt,
            field_decoder,
            schema_field_names,
        }
    }
    fn read_row(
        &self,
        buf: &[u8],
        columns: &mut [ColumnBuilder],
        null_if: &[&str],
        file_path: &str,
    ) -> std::result::Result<(), FileParseError> {
        if self.load_context.settings.enable_sonic_json_parser
            && !self.field_decoder.is_select
            && let Some(json) = databend_common_formats::json::parse_sonic(buf)
        {
            return self.read_row_value(&json, columns, null_if, file_path);
        }
        // Keep serde for stage SELECT, unsupported rows and error diagnostics.
        let json: serde_json::Value =
            serde_json::from_slice(buf).map_err(|e| map_json_error(e, buf, file_path))?;
        self.read_row_value(&json, columns, null_if, file_path)
    }

    fn read_row_value(
        &self,
        json: &impl JsonValue,
        columns: &mut [ColumnBuilder],
        null_if: &[&str],
        file_path: &str,
    ) -> std::result::Result<(), FileParseError> {
        // Stage SELECT always comes through the Serde branch in read_row.
        if self.field_decoder.is_select {
            if let ColumnBuilder::Variant(column) = &mut columns[0] {
                json.write_jsonb(&mut column.data);
                column.commit_row();
            } else {
                return Err(FileParseError::InvalidRow {
                    format: "NDJSON".to_string(),
                    message: format!(
                        "Invalid NDJSON select schema: expect Variant column, but got {}",
                        columns[0].data_type()
                    ),
                });
            }
        } else {
            let object_values = JsonObjectLookup::for_row(
                json,
                self.field_decoder.ident_case_sensitive,
                columns.len(),
            );
            let object_keys = if self.load_context.schema_evolution {
                object_values.unique_len()
            } else {
                0
            };
            let mut used_keys = 0;

            for ((column_index, field), column) in self
                .load_context
                .schema
                .fields()
                .iter()
                .enumerate()
                .zip(columns.iter_mut())
            {
                let value = object_values.get(field.name());
                if value.is_some() {
                    used_keys += 1;
                }
                match value {
                    None => match self.fmt.params.missing_field_as {
                        NullAs::Error => {
                            let advice = self.build_column_missing_advice(
                                field.name(),
                                json,
                                "MISSING_FIELD_AS=ERROR",
                                None,
                            );
                            return Err(FileParseError::ColumnMissingError {
                                column_index,
                                column_name: field.name().to_owned(),
                                column_type: field.data_type.to_string(),
                                advice,
                            });
                        }
                        NullAs::Null => {
                            if field.is_nullable_or_null() {
                                column.push_default();
                            } else {
                                let advice = self.build_column_missing_advice(
                                    field.name(),
                                    json,
                                    "MISSING_FIELD_AS=NULL",
                                    Some("the column is not nullable"),
                                );
                                return Err(FileParseError::ColumnMissingError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                    advice,
                                });
                            }
                        }
                        NullAs::FieldDefault => {
                            self.load_context.push_default_value(column, column_index)?;
                        }
                    },
                    Some(value) if value.is_null() => match self.fmt.params.null_field_as {
                        NullAs::Error => unreachable!("null_field_as should be error"),
                        NullAs::Null => {
                            if field.is_nullable_or_null() {
                                column.push_default();
                            } else {
                                return Err(FileParseError::ColumnDecodeError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                    decode_error: "null value is not allowed for non-nullable field, when NULL_FIELDS_AS=NULL".to_owned(),
                                    column_data: "null".to_owned(),
                                });
                            }
                        }
                        NullAs::FieldDefault => {
                            self.load_context.push_default_value(column, column_index)?;
                        }
                    },
                    Some(value) => {
                        if !null_if.is_empty()
                            && matches!(column, ColumnBuilder::Nullable(_))
                            && value.as_str().is_some_and(|s| null_if.contains(&s))
                        {
                            column.push_default();
                        } else {
                            self.field_decoder
                                .read_field_with_data_type(column, value, field.data_type())
                                .map_err(|e| FileParseError::ColumnDecodeError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                    decode_error: e.to_string(),
                                    column_data: truncate_column_data(value.diagnostic_text()),
                                })?;
                        }
                    }
                }
            }

            if self.load_context.schema_evolution && object_keys != used_keys {
                let extra_columns = extra_object_keys(
                    json,
                    &self.schema_field_names,
                    self.field_decoder.ident_case_sensitive,
                );
                if !extra_columns.is_empty() {
                    return Err(FileParseError::InvalidRow {
                        format: "NDJSON".to_string(),
                        message: format!(
                            "schema evolution sample did not include all columns for File '{}'. Extra columns: {}. Please adjust SCHEMA_EVOLUTION sample options such as SAMPLE_FILES, SAMPLE_RECORDS_PER_FILE, or SAMPLE_TOTAL_RECORDS",
                            file_path,
                            extra_columns.join(", "),
                        ),
                    });
                }
            }
        }
        Ok(())
    }

    fn build_column_missing_advice(
        &self,
        column_name: &str,
        json: &impl JsonValue,
        current_setting: &str,
        extra_reason: Option<&str>,
    ) -> String {
        let mut hints = Vec::new();

        // Hint 1: current MISSING_FIELD_AS setting
        hints.push(format!("current FILE_FORMAT option: {current_setting}"));

        // Hint 2: if there's an extra reason (e.g., column not nullable)
        if let Some(reason) = extra_reason {
            hints.push(reason.to_string());
        }

        // Hint 3: if case_sensitive, try case-insensitive match
        if self.field_decoder.ident_case_sensitive {
            if let Some(mut object) = json.object_iter() {
                let lower_column = column_name.to_lowercase();
                let matched_key =
                    object.find_map(|(k, _)| (k.to_lowercase() == lower_column).then_some(k));
                if let Some(key) = matched_key {
                    hints.push(format!(
                        "found field '{}' with different case; consider using COPY with `COLUMN_MATCH_MODE=CASE_INSENSITIVE`",
                        key
                    ));
                }
            }
        }

        // Hint 4: if target table has a single Variant column, suggest select $1
        let fields = self.load_context.schema.fields();
        if fields.len() == 1
            && matches!(
                fields[0].data_type.remove_nullable(),
                databend_common_expression::TableDataType::Variant
            )
        {
            hints.push(
                "the target table has a single VARIANT column; consider using `COPY INTO <table> FROM (SELECT $1 FROM @<stage>)` to load raw JSON"
                    .to_string(),
            );
        }

        hints.join(". ")
    }
}

fn normalize_field_name(name: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        name.to_string()
    } else {
        name.to_lowercase()
    }
}

fn extra_object_keys(
    json: &impl JsonValue,
    schema_field_names: &HashSet<String>,
    case_sensitive: bool,
) -> Vec<String> {
    let Some(object) = json.object_iter() else {
        return vec![];
    };
    let mut extra_columns = object
        .map(|(key, _)| key)
        .filter(|key| !schema_field_names.contains(&normalize_field_name(key, case_sensitive)))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    extra_columns.sort();
    extra_columns.dedup();
    extra_columns
}

impl RowDecoder for NdJsonDecoder {
    fn add(&self, state: &mut BlockBuilderState, batch: RowBatchWithPosition) -> Result<()> {
        let data = batch.data.into_nd_json().unwrap();
        let null_if = self
            .fmt
            .params
            .null_if
            .iter()
            .map(|x| x.as_str())
            .collect::<Vec<_>>();
        for (row_id, row) in data.iter().enumerate() {
            let columns = &mut state.column_builders;
            let row = row.trim();
            let row_id = batch.start_pos.rows + row_id;
            if !row.is_empty() {
                if let Err(e) = self.read_row(row, columns, &null_if, &state.file_path) {
                    self.load_context.error_handler.on_error(
                        e.with_row(row_id),
                        Some((columns, state.num_rows)),
                        &mut state.file_status,
                        &batch.start_pos.path,
                    )?
                } else {
                    state.add_row(row_id);
                }
            }
        }
        Ok(())
    }
}

// The origin JSON error format "{} at line {} column {}" is misleading for NDJSON.
// - rename `column {}` to `pos {}`, 1-based to 0 based
// - add info for size and next byte
//
// Use test in case of changes of serde_json.
fn map_json_error(err: serde_json::Error, data: &[u8], file_path: &str) -> FileParseError {
    let pos = if err.column() > 0 {
        err.column() - 1
    } else {
        err.column()
    };
    let len = data.len();

    let mut message = err.to_string();
    if let Some(p) = message.rfind(" column") {
        message = message[..p].to_string()
    }

    message = format!("{message}, position {pos} of size {len} for File '{file_path}'");
    if err.column() < len {
        message = format!("{message}, next byte is '{}'", data[pos] as char)
    }
    FileParseError::InvalidRow {
        format: "NDJSON".to_string(),
        message,
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::FileParseError;
    use super::JsonObjectLookup;
    use super::extra_object_keys;
    use super::map_json_error;

    fn decode_err(data: &str) -> String {
        serde_json::from_slice::<serde_json::Value>(data.as_bytes())
            .map_err(|e| {
                let e = map_json_error(e, data.as_bytes(), "mock_file");
                if let FileParseError::InvalidRow { message, .. } = e {
                    message
                } else {
                    unreachable!()
                }
            })
            .err()
            .unwrap()
    }

    #[test]
    fn test_json_decode_error() {
        assert_eq!(
            decode_err("{").as_str(),
            "EOF while parsing an object at line 1, position 0 of size 1 for File 'mock_file'"
        );
        assert_eq!(
            decode_err("").as_str(),
            "EOF while parsing a value at line 1, position 0 of size 0 for File 'mock_file'"
        );
        assert_eq!(
            decode_err("{\"k\"-}").as_str(),
            "expected `:` at line 1, position 4 of size 6 for File 'mock_file', next byte is '-'"
        );
    }

    #[test]
    fn test_sonic_object_matching() {
        use databend_common_formats::json::JsonValue;
        use databend_common_formats::json::parse_sonic;

        for text in [
            r#"{"A":1,"a":2,"Å":3,"extra":4}"#,
            r#"{"A":1,"a":2,"A":3,"extra":4,"extra":5}"#,
            r#"{"id":1,"d":165.6263,"nested":{"a":-0.0003}}"#,
        ] {
            let serde: serde_json::Value = serde_json::from_str(text).unwrap();
            let sonic = parse_sonic(text.as_bytes()).unwrap();
            for case_sensitive in [false, true] {
                let expected_map = JsonObjectLookup::for_row(&serde, case_sensitive, 8);
                let actual_map = JsonObjectLookup::for_row(&sonic, case_sensitive, 8);
                assert_eq!(actual_map.unique_len(), expected_map.unique_len());
                for key in ["A", "a", "Å", "å", "id", "d", "nested", "missing"] {
                    let expected = expected_map.get(key);
                    let actual = actual_map.get(key);
                    assert_eq!(
                        actual.map(JsonValue::diagnostic_text),
                        expected.map(JsonValue::diagnostic_text)
                    );
                }
                let schema = HashSet::from(["a".to_string(), "id".to_string()]);
                assert_eq!(
                    extra_object_keys(&sonic, &schema, case_sensitive),
                    extra_object_keys(&serde, &schema, case_sensitive)
                );
            }
        }
    }

    #[test]
    fn test_ndjson_extra_object_keys_case_insensitive() {
        let json: serde_json::Value = serde_json::from_str(r#"{"A":1,"B":2,"D":3}"#).unwrap();
        let schema_field_names = HashSet::from(["a".to_string(), "b".to_string()]);
        let object_values = JsonObjectLookup::for_row(&json, false, 2);

        assert_eq!(
            object_values.get("a"),
            Some(&serde_json::Value::Number(1.into()))
        );
        assert_eq!(extra_object_keys(&json, &schema_field_names, false), vec![
            "D".to_string()
        ]);
    }
}
