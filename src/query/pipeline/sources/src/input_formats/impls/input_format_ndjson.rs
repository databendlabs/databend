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

use std::sync::Arc;

use bstr::ByteSlice;
use common_exception::Result;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_expression::TableSchemaRef;
use common_formats::FieldDecoder;
use common_formats::FieldJsonAstDecoder;
use common_formats::FileFormatOptionsExt;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::StageFileFormatType;
use common_storage::FileParseError;

use crate::input_formats::error_utils::truncate_column_data;
use crate::input_formats::AligningStateRowDelimiter;
use crate::input_formats::BlockBuilder;
use crate::input_formats::InputContext;
use crate::input_formats::InputFormatTextBase;
use crate::input_formats::RowBatch;
use crate::input_formats::SplitInfo;

pub struct InputFormatNDJson {}

impl InputFormatNDJson {
    pub fn create() -> Self {
        Self {}
    }
    fn read_row(
        field_decoder: &FieldJsonAstDecoder,
        buf: &[u8],
        columns: &mut [ColumnBuilder],
        schema: &TableSchemaRef,
        default_values: &Option<Vec<Scalar>>,
    ) -> std::result::Result<(), FileParseError> {
        let mut json: serde_json::Value =
            serde_json::from_reader(buf).map_err(|e| FileParseError::InvalidNDJsonRow {
                message: e.to_string(),
            })?;
        // todo: this is temporary
        if field_decoder.is_select {
            field_decoder
                .read_field(&mut columns[0], &json)
                .map_err(|e| FileParseError::InvalidNDJsonRow {
                    message: e.to_string(),
                })?;
        } else {
            // if it's not case_sensitive, we convert to lowercase
            if !field_decoder.ident_case_sensitive {
                if let serde_json::Value::Object(x) = json {
                    let y = x.into_iter().map(|(k, v)| (k.to_lowercase(), v)).collect();
                    json = serde_json::Value::Object(y);
                }
            }

            for ((column_index, field), column) in
                schema.fields().iter().enumerate().zip(columns.iter_mut())
            {
                let field_name = if field_decoder.ident_case_sensitive {
                    field.name().to_owned()
                } else {
                    field.name().to_lowercase()
                };
                let value = &json[field_name];
                if value == &serde_json::Value::Null {
                    match default_values {
                        None => {
                            column.push_default();
                        }
                        Some(values) => {
                            column.push(values[column_index].as_ref());
                        }
                    }
                } else {
                    field_decoder.read_field(column, value).map_err(|e| {
                        FileParseError::ColumnDecodeError {
                            column_index,
                            column_name: field.name().to_owned(),
                            column_type: field.data_type.to_string(),
                            decode_error: e.to_string(),
                            column_data: truncate_column_data(value.to_string()),
                        }
                    })?;
                }
            }
        }
        Ok(())
    }
}

impl InputFormatTextBase for InputFormatNDJson {
    type AligningState = AligningStateRowDelimiter;

    fn format_type() -> StageFileFormatType {
        StageFileFormatType::NdJson
    }

    fn is_splittable() -> bool {
        true
    }

    fn try_create_align_state(
        ctx: &Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<Self::AligningState> {
        AligningStateRowDelimiter::try_create(ctx, split_info, b'\n', true, 0)
    }

    fn create_field_decoder(
        _params: &FileFormatParams,
        options: &FileFormatOptionsExt,
    ) -> Arc<dyn FieldDecoder> {
        Arc::new(FieldJsonAstDecoder::create(options))
    }

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()> {
        let field_decoder = builder
            .field_decoder
            .as_any()
            .downcast_ref::<FieldJsonAstDecoder>()
            .expect("must success");

        let columns = &mut builder.mutable_columns;
        let mut start = 0usize;
        for (i, end) in batch.row_ends.iter().enumerate() {
            let buf = &batch.data[start..*end];
            let buf = buf.trim();
            if !buf.is_empty() {
                if let Err(e) = Self::read_row(
                    field_decoder,
                    buf,
                    columns,
                    &builder.ctx.schema,
                    &builder.ctx.default_values,
                ) {
                    builder.ctx.on_error(
                        e,
                        Some((columns, builder.num_rows)),
                        &mut builder.file_status,
                        &batch.split_info.file.path,
                        batch.start_row_in_split + i,
                    )?
                } else {
                    builder.num_rows += 1;
                    builder.file_status.num_rows_loaded += 1;
                }
            }
            start = *end;
        }
        Ok(())
    }
}
