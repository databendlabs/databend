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

use std::io::Cursor;
use std::sync::Arc;

use common_exception::Result;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_expression::TableSchemaRef;
use common_formats::FieldDecoder;
use common_formats::FieldDecoderRowBased;
use common_formats::FieldDecoderTSV;
use common_formats::FileFormatOptionsExt;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::StageFileFormatType;
use common_meta_app::principal::TsvFileFormatParams;
use common_storage::FileParseError;
use log::debug;

use crate::input_formats::error_utils::check_column_end;
use crate::input_formats::error_utils::get_decode_error_by_pos;
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

    fn read_column(
        builder: &mut ColumnBuilder,
        field_decoder: &FieldDecoderTSV,
        col_data: &[u8],
        column_index: usize,
        schema: &TableSchemaRef,
        default_values: &Option<Vec<Scalar>>,
    ) -> std::result::Result<(), FileParseError> {
        if col_data.is_empty() {
            match default_values {
                None => {
                    builder.push_default();
                }
                Some(values) => {
                    builder.push(values[column_index].as_ref());
                }
            }
            Ok(())
        } else {
            let mut reader = Cursor::new(col_data);
            if let Err(e) = field_decoder.read_field(builder, &mut reader, true) {
                return Err(get_decode_error_by_pos(
                    column_index,
                    schema,
                    &e.message(),
                    col_data,
                ));
            };
            check_column_end(&mut reader, schema, column_index)
        }
    }

    fn read_row(
        field_delimiter: u8,
        field_decoder: &FieldDecoderTSV,
        buf: &[u8],
        columns: &mut Vec<ColumnBuilder>,
        schema: &TableSchemaRef,
        columns_to_read: &Option<Vec<usize>>,
        default_values: &Option<Vec<Scalar>>,
    ) -> std::result::Result<(), FileParseError> {
        let num_columns = columns.len();
        let mut column_index = 0;
        let mut field_start = 0;
        let mut field_end = 0;
        let mut error = None;
        let buf_len = buf.len();
        let mut last_is_delimiter = false;
        if let Some(columns_to_read) = columns_to_read {
            while field_end <= buf_len && column_index < num_columns {
                if field_end == buf_len || (buf[field_end] == field_delimiter && !last_is_delimiter)
                {
                    if columns_to_read.contains(&column_index) {
                        if let Err(e) = Self::read_column(
                            &mut columns[column_index],
                            field_decoder,
                            &buf[field_start..field_end],
                            column_index,
                            schema,
                            default_values,
                        ) {
                            error = Some(e);
                            break;
                        }
                    }
                    column_index += 1;
                    field_start = field_end + 1;
                }
                if field_end < buf_len {
                    last_is_delimiter = (buf[field_end] == b'\\') && !last_is_delimiter
                }
                field_end += 1;
            }
            if error.is_none() {
                while column_index < num_columns {
                    columns[column_index].push_default();
                    column_index += 1;
                }
            }
        } else {
            while field_end <= buf_len && column_index < num_columns {
                if field_end == buf_len || (buf[field_end] == field_delimiter && !last_is_delimiter)
                {
                    if let Err(err) = Self::read_column(
                        &mut columns[column_index],
                        field_decoder,
                        &buf[field_start..field_end],
                        column_index,
                        schema,
                        default_values,
                    ) {
                        error = Some(err);
                        break;
                    }
                    column_index += 1;
                    field_start = field_end + 1;
                }
                if field_end < buf_len {
                    last_is_delimiter = (buf[field_end] == b'\\') && !last_is_delimiter
                }
                field_end += 1;
            }
            if error.is_none() {
                // expect: field_end > buf_len && column_index == num_columns
                if column_index < num_columns {
                    error = Some(FileParseError::NumberOfColumnsMismatch {
                        table: num_columns,
                        file: column_index,
                    });
                } else if field_end <= buf_len {
                    error = Some(FileParseError::NumberOfColumnsMismatch {
                        table: num_columns,
                        file: num_columns + 1,
                    });
                }
            }
        }
        if let Some(e) = error { Err(e) } else { Ok(()) }
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
            false,
            tsv_params.headers as usize,
        )
    }

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()> {
        debug!(
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
        for (i, end) in batch.row_ends.iter().enumerate() {
            let buf = &batch.data[start..*end]; // include \n
            if let Err(e) = Self::read_row(
                field_delimiter,
                field_decoder,
                buf,
                columns,
                schema,
                &builder.projection,
                &builder.ctx.default_values,
            ) {
                builder.ctx.on_error(
                    e,
                    Some((columns, builder.num_rows)),
                    &mut builder.file_status,
                    &batch.split_info.file.path,
                    i + batch.start_row_in_split,
                )?
            } else {
                builder.num_rows += 1;
                builder.file_status.num_rows_loaded += 1;
            }
            start = *end;
        }
        Ok(())
    }
}
