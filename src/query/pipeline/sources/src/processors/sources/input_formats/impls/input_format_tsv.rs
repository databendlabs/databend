//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::io::Cursor;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_datavalues::TypeDeserializer;
use common_exception::ErrorCode;
use common_exception::Result;
use common_formats::FieldDecoder;
use common_formats::FieldDecoderRowBased;
use common_formats::FieldDecoderTSV;
use common_formats::FileFormatOptionsExt;
use common_io::cursor_ext::*;
use common_io::format_diagnostic::verbose_string;
use common_meta_types::StageFileFormatType;

use crate::processors::sources::input_formats::input_format_text::AligningState;
use crate::processors::sources::input_formats::input_format_text::BlockBuilder;
use crate::processors::sources::input_formats::input_format_text::InputFormatTextBase;
use crate::processors::sources::input_formats::input_format_text::RowBatch;

pub struct InputFormatTSV {}

impl InputFormatTSV {
    #[allow(clippy::too_many_arguments)]
    fn read_row(
        field_delimiter: u8,
        field_decoder: &FieldDecoderTSV,
        buf: &[u8],
        deserializers: &mut Vec<common_datavalues::TypeDeserializerImpl>,
        schema: &DataSchemaRef,
        path: &str,
        batch_id: usize,
        offset: usize,
        row_index: Option<usize>,
    ) -> Result<()> {
        let num_columns = deserializers.len();
        let mut column_index = 0;
        let mut field_start = 0;
        let mut pos = 0;
        let mut err_msg = None;
        let buf_len = buf.len();
        while pos <= buf_len {
            if pos == buf_len || buf[pos] == field_delimiter {
                let col_data = &buf[field_start..pos];
                if col_data.is_empty() {
                    deserializers[column_index].de_default();
                } else {
                    let mut reader = Cursor::new(col_data);
                    reader.ignores(|c: u8| c == b' ');
                    if let Err(e) = field_decoder.read_field(
                        &mut deserializers[column_index],
                        &mut reader,
                        true,
                    ) {
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
        if err_msg.is_none() && column_index < num_columns {
            // todo(youngsofun): allow it optionally (set default)
            err_msg = Some(format!(
                "need {} columns, find {} only",
                num_columns, column_index
            ));
        }

        if let Some(m) = err_msg {
            let row_info = if let Some(r) = row_index {
                format!("at row {},", r)
            } else {
                String::new()
            };
            let mut msg = format!(
                "fail to parse tsv {} batch {} at offset {}, {} reason={}, row data: ",
                path,
                batch_id,
                offset + pos,
                row_info,
                m
            );
            verbose_string(buf, &mut msg);
            Err(ErrorCode::BadBytes(msg))
        } else {
            Ok(())
        }
    }
}

impl InputFormatTextBase for InputFormatTSV {
    fn format_type() -> StageFileFormatType {
        StageFileFormatType::Tsv
    }

    fn is_splittable() -> bool {
        true
    }

    fn create_field_decoder(options: &FileFormatOptionsExt) -> Arc<dyn FieldDecoder> {
        Arc::new(FieldDecoderTSV::create(options))
    }

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()> {
        tracing::debug!(
            "tsv deserializing row batch {}, id={}, start_row={:?}, offset={}",
            batch.path,
            batch.batch_id,
            batch.start_row,
            batch.offset
        );
        let field_decoder = builder
            .field_decoder
            .as_any()
            .downcast_ref::<FieldDecoderTSV>()
            .expect("must success");
        let schema = &builder.ctx.schema;
        let columns = &mut builder.mutable_columns;
        let mut start = 0usize;
        let start_row = batch.start_row;
        for (i, end) in batch.row_ends.iter().enumerate() {
            let buf = &batch.data[start..*end]; // include \n
            Self::read_row(
                builder.ctx.field_delimiter,
                field_decoder,
                buf,
                columns,
                schema,
                &batch.path,
                batch.batch_id,
                batch.offset + start,
                start_row.map(|n| n + i),
            )?;
            start = *end;
        }
        Ok(())
    }

    fn align(state: &mut AligningState<Self>, buf: &[u8]) -> Result<Vec<RowBatch>> {
        Ok(state.align_by_record_delimiter(buf))
    }
}

pub fn format_column_error(
    schema: &DataSchemaRef,
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
