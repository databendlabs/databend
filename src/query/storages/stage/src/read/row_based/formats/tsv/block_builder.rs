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

use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_formats::SeparatedTextDecoder;
use databend_common_io::cursor_ext::BufferReadStringExt;
use databend_common_storage::FileParseError;

use crate::read::block_builder_state::BlockBuilderState;
use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::RowDecoder;
use crate::read::row_based::formats::tsv::format::TsvInputFormat;
use crate::read::row_based::utils::get_decode_error_by_pos;

pub struct TsvDecoder {
    pub load_context: Arc<LoadContext>,
    pub field_decoder: SeparatedTextDecoder,

    pub field_delimiter: u8,

    pub record_delimiter: u8,
    pub trim_cr: bool,
}

impl TsvDecoder {
    pub fn create(fmt: TsvInputFormat, load_context: Arc<LoadContext>) -> Self {
        let field_decoder =
            SeparatedTextDecoder::create_tsv(&fmt.params, &load_context.file_format_options_ext);
        let field_delimiter = fmt.params.field_delimiter.as_bytes()[0];

        // we only accept \r\n when len > 1
        let trim_cr = fmt.params.field_delimiter.len() > 1;
        // safe to unwrap, params are checked
        let record_delimiter = *fmt.params.record_delimiter.as_bytes().last().unwrap();
        Self {
            load_context,
            field_decoder,
            field_delimiter,
            record_delimiter,
            trim_cr,
        }
    }

    fn trim_record_delimiter<'a>(&self, mut row: &'a [u8]) -> &'a [u8] {
        let len = row.len();
        if len > 0 && row[len - 1] == self.record_delimiter {
            row = &row[..(len - 1)];
            if self.trim_cr && len > 2 && row[len - 2] == b'\r' {
                row = &row[..(len - 2)];
            }
        }
        row
    }

    fn read_column(
        &self,
        builder: &mut ColumnBuilder,
        col_data: &[u8],
        column_index: usize,
    ) -> std::result::Result<(), FileParseError> {
        if col_data.is_empty() {
            self.load_context
                .push_default_value(builder, column_index, false)
        } else {
            // todo(youngsofun): optimize this later after refactor.
            let mut cursor = Cursor::new(col_data);
            let mut data = vec![];
            cursor.read_escaped_string_text(&mut data).map_err(|e| {
                get_decode_error_by_pos(
                    column_index,
                    &self.load_context.schema,
                    &e.to_string(),
                    col_data,
                )
            })?;
            if let Err(e) = self.field_decoder.read_field(builder, &data) {
                return Err(get_decode_error_by_pos(
                    column_index,
                    &self.load_context.schema,
                    &e.message(),
                    col_data,
                ));
            }
            Ok(())
        }
    }

    fn read_row(
        &self,
        buf: &[u8],
        columns: &mut [ColumnBuilder],
    ) -> std::result::Result<(), FileParseError> {
        let num_columns = columns.len();
        let mut column_index = 0;
        let mut field_start = 0;
        let mut field_end = 0;
        let mut error = None;
        let buf_len = buf.len();
        let mut last_is_delimiter = false;
        if let Some(columns_to_read) = &self.load_context.pos_projection {
            while field_end <= buf_len && column_index < num_columns {
                if field_end == buf_len
                    || (buf[field_end] == self.field_delimiter && !last_is_delimiter)
                {
                    if columns_to_read.contains(&column_index) {
                        if let Err(e) = self.read_column(
                            &mut columns[column_index],
                            &buf[field_start..field_end],
                            column_index,
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
                if field_end == buf_len
                    || (buf[field_end] == self.field_delimiter && !last_is_delimiter)
                {
                    if let Err(err) = self.read_column(
                        &mut columns[column_index],
                        &buf[field_start..field_end],
                        column_index,
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
        if let Some(e) = error {
            Err(e)
        } else {
            Ok(())
        }
    }
}

impl RowDecoder for TsvDecoder {
    fn add(&self, state: &mut BlockBuilderState, batch: RowBatchWithPosition) -> Result<()> {
        let data = batch.data.into_nd_json().unwrap();

        for (row_id, mut row) in data.iter().enumerate() {
            // trim the record delimiter
            let columns = &mut state.column_builders;
            row = self.trim_record_delimiter(row);
            let row_id = batch.start_pos.rows + row_id;
            if !row.is_empty() {
                if let Err(e) = self.read_row(row, columns) {
                    self.load_context.error_handler.on_error(
                        e,
                        Some((columns, state.num_rows)),
                        &mut state.file_status,
                        &batch.start_pos.path,
                        row_id,
                    )?
                } else {
                    state.add_row(row_id);
                }
            }
        }
        Ok(())
    }
}
