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

use std::mem;
use std::sync::Arc;

use csv_core::ReadRecordResult;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_common_formats::FieldDecoder;
use databend_common_formats::FileFormatOptionsExt;
use databend_common_formats::RecordDelimiter;
use databend_common_formats::SeparatedTextDecoder;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::EmptyFieldAs;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_storage::FileParseError;
use databend_common_storage::FileStatus;
use log::debug;

use crate::input_formats::error_utils::get_decode_error_by_pos;
use crate::input_formats::AligningStateCommon;
use crate::input_formats::AligningStateTextBased;
use crate::input_formats::BlockBuilder;
use crate::input_formats::InputContext;
use crate::input_formats::InputFormatTextBase;
use crate::input_formats::RowBatch;
use crate::input_formats::SplitInfo;

const MAX_CSV_COLUMNS: usize = 1000;

pub struct InputFormatCSV {}

impl InputFormatCSV {
    pub fn create() -> Self {
        Self {}
    }

    fn read_column(
        input_ctx: &Arc<InputContext>,
        builder: &mut ColumnBuilder,
        field_decoder: &SeparatedTextDecoder,
        col_data: &[u8],
        column_index: usize,
        schema: &TableSchemaRef,
        default_values: &Option<Vec<RemoteExpr>>,
        empty_filed_as: &EmptyFieldAs,
    ) -> std::result::Result<(), FileParseError> {
        if col_data.is_empty() {
            match default_values {
                None => {
                    // query
                    builder.push_default();
                }
                Some(values) => {
                    let field = &schema.fields()[column_index];
                    // copy
                    match empty_filed_as {
                        EmptyFieldAs::FieldDefault => {
                            input_ctx.push_default_value(values, builder, column_index)?;
                        }
                        EmptyFieldAs::Null => {
                            if !matches!(field.data_type, TableDataType::Nullable(_)) {
                                return Err(FileParseError::ColumnEmptyError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                    empty_field_as: empty_filed_as.to_string(),
                                    remedy: format!(
                                        "one of the following options: 1. Modify the `{}` column to allow NULL values. 2. Set EMPTY_FIELD_AS to FIELD_DEFAULT.",
                                        field.name()
                                    ),
                                });
                            }
                            builder.push_default();
                        }
                        EmptyFieldAs::String => match builder {
                            ColumnBuilder::String(b) => {
                                b.put_str("");
                                b.commit_row();
                            }
                            ColumnBuilder::Nullable(box NullableColumnBuilder {
                                builder: ColumnBuilder::String(b),
                                validity,
                            }) => {
                                b.put_str("");
                                b.commit_row();
                                validity.push(true);
                            }
                            _ => {
                                let field = &schema.fields()[column_index];
                                return Err(FileParseError::ColumnEmptyError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                    empty_field_as: empty_filed_as.to_string(),
                                    remedy: "Set EMPTY_FIELD_AS to FIELD_DEFAULT or NULL."
                                        .to_string(),
                                });
                            }
                        },
                    }
                }
            }
            return Ok(());
        }
        field_decoder
            .read_field(builder, col_data)
            .map_err(|e| get_decode_error_by_pos(column_index, schema, &e.message(), col_data))
    }

    fn read_row(
        input_ctx: &Arc<InputContext>,
        field_decoder: &SeparatedTextDecoder,
        buf: &[u8],
        columns: &mut [ColumnBuilder],
        schema: &TableSchemaRef,
        field_ends: &[usize],
        columns_to_read: &Option<Vec<usize>>,
        default_values: &Option<Vec<RemoteExpr>>,
        empty_filed_as: &EmptyFieldAs,
    ) -> std::result::Result<(), FileParseError> {
        if let Some(columns_to_read) = columns_to_read {
            for c in columns_to_read {
                if *c >= field_ends.len() {
                    columns[*c].push_default();
                } else {
                    let field_start = if *c == 0 { 0 } else { field_ends[c - 1] };
                    let field_end = field_ends[*c];
                    let col_data = &buf[field_start..field_end];
                    Self::read_column(
                        input_ctx,
                        &mut columns[*c],
                        field_decoder,
                        col_data,
                        *c,
                        schema,
                        default_values,
                        empty_filed_as,
                    )?;
                }
            }
        } else {
            let mut field_start = 0;
            for (c, column) in columns.iter_mut().enumerate() {
                let field_end = field_ends[c];
                let col_data = &buf[field_start..field_end];
                Self::read_column(
                    input_ctx,
                    column,
                    field_decoder,
                    col_data,
                    c,
                    schema,
                    default_values,
                    empty_filed_as,
                )?;
                field_start = field_end;
            }
        }
        Ok(())
    }
}

impl InputFormatTextBase for InputFormatCSV {
    type AligningState = CsvReaderState;

    fn format_type() -> StageFileFormatType {
        StageFileFormatType::Csv
    }

    fn create_field_decoder(
        params: &FileFormatParams,
        options: &FileFormatOptionsExt,
    ) -> Arc<dyn FieldDecoder> {
        let csv_params = CsvFileFormatParams::downcast_unchecked(params);
        Arc::new(SeparatedTextDecoder::create_csv(csv_params, options))
    }

    fn try_create_align_state(
        ctx: &Arc<InputContext>,
        split_info: &Arc<SplitInfo>,
    ) -> Result<Self::AligningState> {
        let csv_params = CsvFileFormatParams::downcast_unchecked(&ctx.file_format_params);

        let escape = if csv_params.escape.is_empty() {
            None
        } else {
            Some(csv_params.escape.as_bytes()[0])
        };
        let reader = csv_core::ReaderBuilder::new()
            .delimiter(csv_params.field_delimiter.as_bytes()[0])
            .quote(csv_params.quote.as_bytes()[0])
            .escape(escape)
            .terminator(match csv_params.record_delimiter.as_str().try_into()? {
                RecordDelimiter::Crlf => csv_core::Terminator::CRLF,
                RecordDelimiter::Any(v) => csv_core::Terminator::Any(v),
            })
            .build();
        let projection = ctx.projection.clone();
        let max_fields = match &projection {
            Some(p) => p.iter().copied().max().unwrap_or(1),
            None => ctx.schema.num_fields(),
        } + MAX_CSV_COLUMNS;
        Ok(CsvReaderState {
            common: AligningStateCommon::create(split_info, false, csv_params.headers as usize),
            error_on_column_count_mismatch: csv_params.error_on_column_count_mismatch,
            ctx: ctx.clone(),
            split_info: split_info.clone(),
            reader,
            out: vec![],
            field_ends: vec![0; max_fields],
            n_end: 0,
            num_fields: ctx.schema.num_fields(),
            projection,
        })
    }

    fn deserialize(builder: &mut BlockBuilder<Self>, batch: RowBatch) -> Result<()> {
        let columns = &mut builder.mutable_columns;
        let mut start = 0usize;
        let mut field_end_idx = 0;
        let field_decoder = builder
            .field_decoder
            .as_any()
            .downcast_ref::<SeparatedTextDecoder>()
            .expect("must success");
        let format_params = match builder.ctx.file_format_params {
            FileFormatParams::Csv(ref p) => p,
            _ => unreachable!(),
        };

        for (i, end) in batch.row_ends.iter().enumerate() {
            let num_fields = batch.num_fields[i];
            let buf = &batch.data[start..*end];
            if let Err(e) = Self::read_row(
                &builder.ctx,
                field_decoder,
                buf,
                columns,
                &builder.ctx.schema,
                &batch.field_ends[field_end_idx..field_end_idx + num_fields],
                &builder.projection,
                &builder.ctx.default_values,
                &format_params.empty_field_as,
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
            field_end_idx += num_fields;
        }
        Ok(())
    }
}

pub struct CsvReaderState {
    common: AligningStateCommon,
    error_on_column_count_mismatch: bool,
    #[allow(unused)]
    ctx: Arc<InputContext>,
    split_info: Arc<SplitInfo>,
    pub reader: csv_core::Reader,

    // remain from last read batch
    pub out: Vec<u8>,

    // field_end[..n_end] store the output of each call to reader.read_record()
    // it may belong to part of a row.
    // flush to RowBatch when a complete row is read
    pub field_ends: Vec<usize>,
    pub n_end: usize,

    num_fields: usize,
    projection: Option<Vec<usize>>,
}

enum ReadRecordOutput {
    Record { num_fields: usize, bytes: usize },
    RecordSkipped,
    PartialRecord { bytes: usize },
}

impl CsvReaderState {
    fn read_record(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        file_status: &mut FileStatus,
    ) -> Result<(ReadRecordOutput, usize)> {
        let (result, n_in, n_out, n_end) =
            self.reader
                .read_record(input, output, &mut self.field_ends[self.n_end..]);
        self.n_end += n_end;
        // shadow the n_end return from reader to avoid misuse
        let n_end = self.n_end;

        match result {
            ReadRecordResult::InputEmpty => {
                if input.is_empty() {
                    Err(ErrorCode::BadBytes("unexpected eof"))
                } else {
                    self.common.offset += n_in;
                    Ok((ReadRecordOutput::PartialRecord { bytes: n_out }, n_in))
                }
            }
            ReadRecordResult::OutputFull => Err(self.error_output_full()),
            ReadRecordResult::OutputEndsFull => Err(self.error_output_ends_full()),
            ReadRecordResult::Record => {
                let output = {
                    if self.projection.is_some() {
                        // select $1, $2, $3 ..  from csv, not check num of fields here

                        ReadRecordOutput::Record {
                            num_fields: n_end,
                            bytes: n_out,
                        }
                    } else {
                        // copy
                        if !self.error_on_column_count_mismatch {
                            // cut or patch to num_fields

                            if self.n_end < self.num_fields {
                                // support we expect 4 fields but got row with only 2 columns : "1,2\n"
                                // here we pretend we read "1,2,,\n"
                                debug_assert!(self.n_end > 0);
                                let end = self.field_ends[n_end - 1];
                                for i in n_end..self.num_fields {
                                    self.field_ends[i] = end;
                                }
                            }
                            ReadRecordOutput::Record {
                                num_fields: self.num_fields,
                                bytes: n_out,
                            }
                        } else {
                            // check num of fields strictly

                            if let Err(e) = self.check_num_field() {
                                self.ctx.on_error(
                                    e,
                                    None,
                                    file_status,
                                    &self.split_info.file.path,
                                    self.common.rows,
                                )?;
                                ReadRecordOutput::RecordSkipped
                            } else {
                                ReadRecordOutput::Record {
                                    num_fields: self.num_fields,
                                    bytes: n_out,
                                }
                            }
                        }
                    }
                };
                self.common.offset += n_in;
                self.common.rows += 1;
                self.n_end = 0;
                Ok((output, n_in))
            }
            ReadRecordResult::End => {
                if !input.is_empty() {
                    Err(ErrorCode::BadBytes("unexpected eof"))
                } else {
                    Ok((ReadRecordOutput::PartialRecord { bytes: n_out }, n_in))
                }
            }
        }
    }
}

impl AligningStateTextBased for CsvReaderState {
    fn align(&mut self, mut buf_in: &[u8]) -> Result<Vec<RowBatch>> {
        let size_in = buf_in.len();
        let mut file_status = FileStatus::default();
        let mut buf_out = vec![0u8; buf_in.len()];
        while self.common.rows_to_skip > 0 && !buf_in.is_empty() {
            let (res, n_in) = self.read_record(buf_in, &mut buf_out, &mut file_status)?;
            buf_in = &buf_in[n_in..];
            if matches!(res, ReadRecordOutput::Record { .. }) {
                self.common.rows_to_skip -= 1;
            }
        }

        if buf_in.is_empty() {
            return Ok(vec![]);
        }

        let mut buf_out_pos = 0usize;
        let mut buf_out_row_end: usize = 0;

        let last_batch_remain_len = self.out.len();

        let mut row_batch = RowBatch {
            data: vec![],
            row_ends: vec![],
            field_ends: vec![],
            num_fields: vec![],
            split_info: self.split_info.clone(),
            batch_id: self.common.batch_id,
            start_offset_in_split: self.common.offset,
            start_row_in_split: self.common.rows,
            start_row_of_split: Some(0),
        };

        while !buf_in.is_empty() {
            let (res, n_in) =
                self.read_record(buf_in, &mut buf_out[buf_out_pos..], &mut file_status)?;
            buf_in = &buf_in[n_in..];
            match res {
                ReadRecordOutput::Record { num_fields, bytes } => {
                    buf_out_pos += bytes;
                    row_batch.num_fields.push(num_fields);
                    row_batch
                        .field_ends
                        .extend_from_slice(&self.field_ends[..num_fields]);
                    row_batch.row_ends.push(last_batch_remain_len + buf_out_pos);
                    buf_out_row_end = buf_out_pos;
                }
                ReadRecordOutput::PartialRecord { bytes } => {
                    buf_out_pos += bytes;
                }
                _ => {}
            }
        }

        buf_out.truncate(buf_out_pos);
        if file_status.error.is_some() {
            self.ctx
                .table_context
                .get_copy_status()
                .add_chunk(&self.split_info.file.path, file_status);
        }

        if row_batch.row_ends.is_empty() {
            debug!(
                "csv aligner: {} + {} bytes => 0 rows",
                self.out.len(),
                size_in,
            );
            self.out.extend_from_slice(&buf_out);
            Ok(vec![])
        } else {
            let last_remain = mem::take(&mut self.out);

            self.common.batch_id += 1;
            self.out.extend_from_slice(&buf_out[buf_out_row_end..]);
            debug!(
                "csv aligner: {} + {} bytes => {} rows + {} bytes remain",
                last_remain.len(),
                size_in,
                row_batch.row_ends.len(),
                self.out.len()
            );

            buf_out.truncate(buf_out_row_end);
            row_batch.data = if last_remain.is_empty() {
                buf_out
            } else {
                [last_remain, buf_out].concat()
            };

            Ok(vec![row_batch])
        }
    }

    fn align_flush(&mut self) -> Result<Vec<RowBatch>> {
        let mut res = vec![];
        let in_tmp = Vec::new();
        let mut out_tmp = vec![0u8; 1];

        let mut file_status = FileStatus::default();
        if self.common.rows_to_skip > 0 {
            self.read_record(&in_tmp, &mut out_tmp, &mut file_status)?;
        } else {
            let last_batch_remain_len = self.out.len();
            let rows = self.common.rows;
            let (out, _n_in) = self.read_record(&in_tmp, &mut out_tmp, &mut file_status)?;
            if let ReadRecordOutput::Record { num_fields, bytes } = out {
                let data = mem::take(&mut self.out);

                let row_batch = RowBatch {
                    data,
                    row_ends: vec![last_batch_remain_len + bytes],
                    field_ends: self.field_ends[..num_fields].to_vec(),
                    num_fields: vec![num_fields],
                    split_info: self.split_info.clone(),
                    batch_id: self.common.batch_id,
                    start_offset_in_split: self.common.offset,
                    start_row_in_split: rows,
                    start_row_of_split: Some(0),
                };
                res.push(row_batch);

                self.common.batch_id += 1;
                debug!(
                    "csv aligner flush last row of {} bytes",
                    last_batch_remain_len,
                );
            }
        }
        if file_status.error.is_some() {
            self.ctx
                .table_context
                .get_copy_status()
                .add_chunk(&self.split_info.file.path, file_status);
        }
        Ok(res)
    }
}

impl CsvReaderState {
    fn check_num_field(&self) -> std::result::Result<(), FileParseError> {
        let expected = self.num_fields;
        let found = self.n_end;
        if found < expected
            || found > expected + 1
            || (found == expected + 1 && self.field_ends[expected] != self.field_ends[expected - 1])
        {
            Err(FileParseError::NumberOfColumnsMismatch {
                table: expected,
                file: found,
            })
        } else {
            Ok(())
        }
    }

    fn error_output_full(&self) -> ErrorCode {
        ErrorCode::BadBytes("Bug: CSV Reader return output longer then input.")
    }

    fn error_output_ends_full(&self) -> ErrorCode {
        if self.projection.is_some() {
            ErrorCode::BadBytes(format!(
                "too many fields, expect {}, got more than {}",
                self.num_fields,
                self.field_ends.len()
            ))
        } else {
            ErrorCode::BadBytes(format!(
                "select from CSV allow at most {} fields",
                MAX_CSV_COLUMNS
            ))
        }
    }
}
