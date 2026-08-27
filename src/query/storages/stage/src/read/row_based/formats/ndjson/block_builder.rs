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
use std::collections::HashSet;
use std::sync::Arc;

use bstr::ByteSlice;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_formats::FieldJsonAstDecoder;
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
        // Use from_slice instead of from_reader: from_reader wraps the slice
        // in an IoRead adapter that reads byte-by-byte and disables serde_json's
        // slice fast path. Benchmarks on ~890 MiB of real NDJSON show a 2.5x
        // speedup from this change alone.
        let json: serde_json::Value =
            serde_json::from_slice(buf).map_err(|e| map_json_error(e, buf, file_path))?;
        // todo: this is temporary
        if self.field_decoder.is_select {
            if let ColumnBuilder::Variant(column) = &mut columns[0] {
                let value = jsonb::Value::from(&json);
                value.write_to_vec(&mut column.data);
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
            let object_keys = json.as_object().map(|object| object.len()).unwrap_or(0);
            let object_values =
                object_values_by_key(&json, self.field_decoder.ident_case_sensitive);
            let mut used_keys = 0;

            for ((column_index, field), column) in self
                .load_context
                .schema
                .fields()
                .iter()
                .enumerate()
                .zip(columns.iter_mut())
            {
                let field_name =
                    normalize_field_name(field.name(), self.field_decoder.ident_case_sensitive);
                let value = get_object_value(
                    &json,
                    object_values.as_ref(),
                    &field_name,
                    self.field_decoder.ident_case_sensitive,
                );
                if value.is_some() {
                    used_keys += 1;
                }
                match value {
                    None => match self.fmt.params.missing_field_as {
                        NullAs::Error => {
                            let advice = self.build_column_missing_advice(
                                field.name(),
                                &json,
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
                                    &json,
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
                    Some(serde_json::Value::Null) => match self.fmt.params.null_field_as {
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
                            && value.is_string()
                            && null_if.contains(&value.as_str().unwrap())
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
                                    column_data: truncate_column_data(value.to_string()),
                                })?;
                        }
                    }
                }
            }

            if self.load_context.schema_evolution && object_keys != used_keys {
                let extra_columns = extra_object_keys(
                    &json,
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
        json: &serde_json::Value,
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
            if let Some(object) = json.as_object() {
                let lower_column = column_name.to_lowercase();
                let matched_key = object.keys().find(|k| k.to_lowercase() == lower_column);
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

fn get_object_value<'a>(
    json: &'a serde_json::Value,
    object_values: Option<&HashMap<String, &'a serde_json::Value>>,
    field_name: &str,
    case_sensitive: bool,
) -> Option<&'a serde_json::Value> {
    let serde_json::Value::Object(object) = json else {
        return None;
    };
    if case_sensitive {
        object.get(field_name)
    } else {
        object_values.and_then(|values| values.get(field_name).copied())
    }
}

fn object_values_by_key(
    json: &serde_json::Value,
    case_sensitive: bool,
) -> Option<HashMap<String, &serde_json::Value>> {
    if case_sensitive {
        return None;
    }
    let serde_json::Value::Object(object) = json else {
        return None;
    };
    Some(
        object
            .iter()
            .map(|(key, value)| (key.to_lowercase(), value))
            .collect(),
    )
}

fn extra_object_keys(
    json: &serde_json::Value,
    schema_field_names: &HashSet<String>,
    case_sensitive: bool,
) -> Vec<String> {
    let serde_json::Value::Object(object) = json else {
        return vec![];
    };
    let mut extra_columns = object
        .keys()
        .filter(|key| !schema_field_names.contains(&normalize_field_name(key, case_sensitive)))
        .cloned()
        .collect::<Vec<_>>();
    extra_columns.sort();
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
    use super::extra_object_keys;
    use super::get_object_value;
    use super::map_json_error;
    use super::object_values_by_key;

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
    fn test_ndjson_extra_object_keys_case_insensitive() {
        let json: serde_json::Value = serde_json::from_str(r#"{"A":1,"B":2,"D":3}"#).unwrap();
        let schema_field_names = HashSet::from(["a".to_string(), "b".to_string()]);
        let object_values = object_values_by_key(&json, false);

        assert_eq!(
            get_object_value(&json, object_values.as_ref(), "a", false),
            Some(&serde_json::Value::Number(1.into()))
        );
        assert_eq!(extra_object_keys(&json, &schema_field_names, false), vec![
            "D".to_string()
        ]);
    }
}
