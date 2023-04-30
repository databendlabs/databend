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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnBuilder;
use common_expression::TableSchemaRef;
use common_formats::FieldDecoder;
use common_formats::FieldDecoderRowBased;
use common_formats::FieldDecoderTSV;
use common_formats::FileFormatOptionsExt;
use common_io::cursor_ext::*;
use common_io::format_diagnostic::verbose_string;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::OnErrorMode;
use common_meta_app::principal::StageFileFormatType;
use common_meta_app::principal::TsvFileFormatParams;
use common_pipeline_core::InputError;

use crate::input_formats::AligningStateRowDelimiter;
use crate::input_formats::BlockBuilder;
use crate::input_formats::InputContext;
use crate::input_formats::InputFormatTextBase;
use crate::input_formats::RowBatch;
use crate::input_formats::SplitInfo;

pub struct InputFormatTSV {}

impl InputFormatTSV {
    pub fn create() -> Self {
        Self {}
    }
    fn read_row(
        field_delimiter: u8,
        field_decoder: &FieldDecoderTSV,
        buf: &[u8],
        columns: &mut Vec<ColumnBuilder>,
        schema: &TableSchemaRef,
    ) -> Result<()> {
        let num_columns = columns.len();
        let mut column_index = 0;
        let mut field_start = 0;
        let mut pos = 0;
        let mut err_msg = None;
        let buf_len = buf.len();
        while pos <= buf_len && column_index < num_columns {
            if pos == buf_len || buf[pos] == field_delimiter {
                let col_data = &buf[field_start..pos];
                if col_data.is_empty() {
                    columns[column_index].push_default();
                } else {
                    let mut reader = Cursor::new(col_data);
                    if let Err(e) =
                        field_decoder.read_field(&mut columns[column_index], &mut reader, true)
                    {
                        err_msg = Some(format_column_error(
                            schema,
                            column_index,
                            col_data,
                            &e.message(),
                        ));
                        break;
                    };
                    reader.ignore_white_spaces();
                    if reader.must_eof().is_err() {
                        err_msg = Some(format_column_error(
                            schema,
                            column_index,
                            col_data,
                            "bad field end",
                        ));
                        break;
                    }
                }
                column_index += 1;
                field_start = pos + 1;
                if column_index > num_columns {
                    err_msg = Some("too many columns".to_string());
                    break;
                }
            }
            pos += 1;
        }
        if err_msg.is_none() {
            if column_index < num_columns {
                err_msg = Some(format!(
                    "need {} columns, find {} only",
                    num_columns, column_index
                ));
            } else if pos < buf_len {
                err_msg = Some("too many columns".to_string());
            }
        }

        if let Some(m) = err_msg {
            let mut msg = format!("{}, row data: ", m);
            verbose_string(buf, &mut msg);
            Err(ErrorCode::BadBytes(msg))
        } else {
            Ok(())
        }
    }
}

impl InputFormatTextBase for InputFormatTSV {
    type AligningState = AligningStateRowDelimiter;

    fn format_type() -> StageFileFormatType {
        StageFileFormatType::Tsv
    }

    fn is_splittable() -> bool {
        true
    }

    fn create_field_decoder(
        params: &FileFormatParams,
        options: &FileFormatOptionsExt,
    ) -> Arc<dyn FieldDecoder> {
        let tsv_params = TsvFileFormatParams::downcast_unchecked(params);
        Arc::new(FieldDecoderTSV::create(tsv_params, options))
    }

    fn try_create_align_state(
        ctx: &Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<Self::AligningState> {
        let tsv_params = TsvFileFormatParams::downcast_unchecked(&ctx.file_format_params);
        AligningStateRowDelimiter::try_create(
            ctx,
            split_info,
            tsv_params.record_delimiter.as_bytes()[0],
            tsv_params.headers as usize,
        )
    }

    fn deserialize(
        builder: &mut BlockBuilder<Self>,
        batch: RowBatch,
    ) -> Result<HashMap<u16, InputError>> {
        tracing::debug!(
            "tsv deserializing row batch {}, id={}, start_row={:?}, offset={}",
            batch.split_info.file.path,
            batch.batch_id,
            batch.start_row_in_split,
            batch.start_offset_in_split
        );
        let field_decoder = builder
            .field_decoder
            .as_any()
            .downcast_ref::<FieldDecoderTSV>()
            .expect("must success");
        let field_delimiter =
            TsvFileFormatParams::downcast_unchecked(&builder.ctx.file_format_params)
                .field_delimiter
                .as_bytes()[0];
        let schema = &builder.ctx.schema;
        let columns = &mut builder.mutable_columns;
        let mut start = 0usize;
        let mut num_rows = 0usize;
        let mut error_map: HashMap<u16, InputError> = HashMap::new();
        for (i, end) in batch.row_ends.iter().enumerate() {
            let buf = &batch.data[start..*end]; // include \n
            if let Err(e) = Self::read_row(field_delimiter, field_decoder, buf, columns, schema) {
                match builder.ctx.on_error_mode {
                    OnErrorMode::Continue => {
                        Self::on_error_continue(columns, num_rows, e.clone(), &mut error_map);
                        start = *end;
                        continue;
                    }
                    OnErrorMode::AbortNum(n) => {
                        Self::on_error_abort(columns, num_rows, n, &builder.ctx.on_error_count, e)
                            .map_err(|e| batch.error(&e.message(), &builder.ctx, start, i))?;
                        start = *end;
                        continue;
                    }
                    _ => return Err(batch.error(&e.message(), &builder.ctx, start, i)),
                }
            }
            start = *end;
            num_rows += 1;
        }
        Ok(error_map)
    }
}

pub fn format_column_error(
    schema: &TableSchemaRef,
    column_index: usize,
    col_data: &[u8],
    msg: &str,
) -> String {
    let mut data = String::new();
    verbose_string(col_data, &mut data);
    let field = &schema.fields()[column_index];
    format!(
        "fail to decode column {} ({} {}): {}, [column_data]=[{}]",
        column_index,
        field.name(),
        field.data_type(),
        msg,
        data
    )
}
