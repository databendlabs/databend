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
use std::io::Read;
use std::mem;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnBuilder;
use common_expression::TableSchemaRef;
use common_formats::FieldDecoder;
use common_formats::FieldDecoderCSV;
use common_formats::FieldDecoderRowBased;
use common_formats::FileFormatOptionsExt;
use common_formats::RecordDelimiter;
use common_io::cursor_ext::*;
use common_io::format_diagnostic::verbose_char;
use common_meta_app::principal::CsvFileFormatParams;
use common_meta_app::principal::FileFormatParams;
use common_meta_app::principal::StageFileFormatType;
use common_pipeline_core::InputError;
use csv_core::ReadRecordResult;

use crate::input_formats::impls::input_format_tsv::format_column_error;
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
        builder: &mut ColumnBuilder,
        field_decoder: &FieldDecoderCSV,
        col_data: &[u8],
        column_index: usize,
        schema: &TableSchemaRef,
    ) -> Result<()> {
        let mut reader = Cursor::new(col_data);
        if reader.eof() {
            builder.push_default();
            return Ok(());
        }
        if let Err(e) = field_decoder.read_field(builder, &mut reader, true) {
            let err_msg = format_column_error(schema, column_index, col_data, &e.message());
            return Err(ErrorCode::BadBytes(err_msg));
        };
        let mut next = [0u8; 1];
        let readn = reader.read(&mut next[..])?;
        if readn > 0 {
            let remaining = col_data.len() - reader.position() as usize + 1;
            let err_msg = format!(
                "bad field end, remain {} bytes, next char is {}",
                remaining,
                verbose_char(next[0])
            );

            let err_msg = format_column_error(schema, column_index, col_data, &err_msg);
            return Err(ErrorCode::BadBytes(err_msg));
        }
        Ok(())
    }

    fn read_row(
        field_decoder: &FieldDecoderCSV,
        buf: &[u8],
        columns: &mut [ColumnBuilder],
        schema: &TableSchemaRef,
        field_ends: &[usize],
        columns_to_read: &Option<Vec<usize>>,
    ) -> Result<()> {
        if let Some(columns_to_read) = columns_to_read {
            for c in columns_to_read {
                if *c >= field_ends.len() {
                    columns[*c].push_default();
                } else {
                    let field_start = if *c == 0 { 0 } else { field_ends[c - 1] };
                    let field_end = field_ends[*c];
                    let col_data = &buf[field_start..field_end];
                    Self::read_column(&mut columns[*c], field_decoder, col_data, *c, schema)?;
                }
            }
        } else {
            let mut field_start = 0;
            for (c, column) in columns.iter_mut().enumerate() {
                let field_end = field_ends[c];
                let col_data = &buf[field_start..field_end];
                Self::read_column(column, field_decoder, col_data, c, schema)?;
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
        Arc::new(FieldDecoderCSV::create(csv_params, options))
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
            ctx: ctx.clone(),
            split_info: split_info.clone(),
            reader,
            out: vec![],
            field_ends: vec![0; max_fields],
            n_end: 0,
            num_fields: ctx.schema.num_fields(),
            projection,
            need_skip_first_line: split_info.offset > 0,
            has_read_one_more_line: false,
            received_bytes_offset: split_info.offset,
        })
    }

    fn deserialize(
        builder: &mut BlockBuilder<Self>,
        batch: RowBatch,
    ) -> Result<HashMap<u16, InputError>> {
        let columns = &mut builder.mutable_columns;
        let mut start = 0usize;
        let mut error_map: HashMap<u16, InputError> = HashMap::new();
        let mut field_end_idx = 0;
        let field_decoder = builder
            .field_decoder
            .as_any()
            .downcast_ref::<FieldDecoderCSV>()
            .expect("must success");
        for (i, end) in batch.row_ends.iter().enumerate() {
            let num_fields = batch.num_fields[i];
            let buf = &batch.data[start..*end];
            if let Err(e) = Self::read_row(
                field_decoder,
                buf,
                columns,
                &builder.ctx.schema,
                &batch.field_ends[field_end_idx..field_end_idx + num_fields],
                &builder.projection,
            ) {
                builder
                    .ctx
                    .on_error(e, Some((columns, builder.num_rows)), Some(&mut error_map))
                    .map_err(|e| batch.error(&e.message(), &builder.ctx, start, i))?;
            } else {
                builder.num_rows += 1;
            }
            start = *end;
            field_end_idx += num_fields;
        }
        Ok(error_map)
    }
}

pub struct CsvReaderState {
    common: AligningStateCommon,
    #[allow(unused)]
    ctx: Arc<InputContext>,
    split_info: Arc<SplitInfo>,
    pub reader: csv_core::Reader,

    // remain from last read batch
    pub out: Vec<u8>,
    pub field_ends: Vec<usize>,
    pub n_end: usize,

    num_fields: usize,
    projection: Option<Vec<usize>>,
    // used to skip the first line
    need_skip_first_line: bool,
    has_read_one_more_line: bool,
    received_bytes_offset: usize,
}

impl CsvReaderState {
    fn read_record(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<(Option<usize>, usize, usize)> {
        let (result, n_in, n_out, n_end) =
            self.reader
                .read_record(input, output, &mut self.field_ends[self.n_end..]);
        self.n_end += n_end;

        match result {
            ReadRecordResult::InputEmpty => {
                if input.is_empty() {
                    Err(self.csv_error("unexpected eof"))
                } else {
                    Ok((None, n_in, n_out))
                }
            }
            ReadRecordResult::OutputFull => Err(self.error_output_full()),
            ReadRecordResult::OutputEndsFull => Err(self.error_output_ends_full()),
            ReadRecordResult::Record => {
                if self.projection.is_none() {
                    if !self.need_skip_first_line && !self.has_read_one_more_line {
                        if let Err(e) = self.check_num_field() {
                            self.ctx.on_error(e, None, None)?;
                            self.common.rows += 1;
                            self.common.offset += n_in;
                            self.n_end = 0;
                            return Ok((Some(0), n_in, n_out));
                        }
                    }
                    self.need_skip_first_line = false;
                }

                self.common.rows += 1;
                self.common.offset += n_in;
                let n_end = self.n_end;
                self.n_end = 0;
                Ok((Some(n_end), n_in, n_out))
            }
            ReadRecordResult::End => {
                if !input.is_empty() {
                    Err(self.csv_error("unexpected eof"))
                } else {
                    Ok((None, n_in, n_out))
                }
            }
        }
    }
}

impl AligningStateTextBased for CsvReaderState {
    fn align(&mut self, buf_in: &[u8]) -> Result<Vec<RowBatch>> {
        let mut out_tmp = vec![0u8; buf_in.len()];
        let mut buf = buf_in;
        self.received_bytes_offset += buf.len();
        while self.common.rows_to_skip > 0 {
            let (_, n_in, _) = self.read_record(buf, &mut out_tmp)?;
            buf = &buf[n_in..];
            self.common.rows_to_skip -= 1;
        }

        let mut out_pos = 0usize;
        let mut row_batch_end: usize = 0;

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

        while !buf.is_empty() {
            let (num_fields, n_in, n_out) = self.read_record(buf, &mut out_tmp[out_pos..])?;
            buf = &buf[n_in..];
            out_pos += n_out;
            if let Some(num_fields) = num_fields {
                if num_fields == 0 {
                    out_pos = row_batch_end;
                } else {
                    row_batch
                        .field_ends
                        .extend_from_slice(&self.field_ends[..num_fields]);
                    row_batch.num_fields.push(num_fields);
                    row_batch.row_ends.push(last_batch_remain_len + out_pos);
                    row_batch_end = out_pos;
                }
            }
        }

        out_tmp.truncate(out_pos);
        if row_batch.row_ends.is_empty() {
            tracing::debug!(
                "csv aligner: {} + {} bytes => 0 rows",
                self.out.len(),
                buf_in.len(),
            );
            self.out.extend_from_slice(&out_tmp);
            self.judge_read_one_more_line();
            Ok(vec![])
        } else {
            let last_remain = mem::take(&mut self.out);

            self.common.batch_id += 1;
            self.out.extend_from_slice(&out_tmp[row_batch_end..]);
            self.judge_read_one_more_line();
            tracing::debug!(
                "csv aligner: {} + {} bytes => {} rows + {} bytes remain",
                last_remain.len(),
                buf_in.len(),
                row_batch.row_ends.len(),
                self.out.len()
            );

            out_tmp.truncate(row_batch_end);
            row_batch.data = if last_remain.is_empty() {
                out_tmp
            } else {
                vec![last_remain, out_tmp].concat()
            };
            Ok(vec![row_batch])
        }
    }

    fn align_flush(&mut self) -> Result<Vec<RowBatch>> {
        let mut res = vec![];
        let in_tmp = Vec::new();
        let mut out_tmp = vec![0u8; 1];

        if self.common.rows_to_skip > 0 {
            let _ = self.read_record(&in_tmp, &mut out_tmp)?;
        } else {
            let last_batch_remain_len = self.out.len();
            let (num_fields, _, n_out) = self.read_record(&in_tmp, &mut out_tmp)?;
            if let Some(num_fields) = num_fields {
                let data = mem::take(&mut self.out);

                let row_batch = RowBatch {
                    data,
                    row_ends: vec![last_batch_remain_len + n_out],
                    field_ends: self.field_ends[..num_fields].to_vec(),
                    num_fields: vec![num_fields],
                    split_info: self.split_info.clone(),
                    batch_id: self.common.batch_id,
                    start_offset_in_split: self.common.offset,
                    start_row_in_split: self.common.rows,
                    start_row_of_split: Some(0),
                };
                res.push(row_batch);

                self.common.batch_id += 1;
                tracing::debug!(
                    "csv aligner flush last row of {} bytes",
                    last_batch_remain_len,
                );
            }
        }
        Ok(res)
    }
}

impl CsvReaderState {
    fn judge_read_one_more_line(&mut self) {
        if self.received_bytes_offset - self.out.len()
            > self.split_info.offset + self.split_info.size
        {
            self.has_read_one_more_line = true;
        }
    }

    fn check_num_field(&self) -> Result<()> {
        let expect = self.num_fields;
        let actual = self.n_end;
        if actual < expect {
            Err(self.csv_error(&format!("expect {} fields, only found {} ", expect, actual)))
        } else if actual > expect + 1
            || (actual == expect + 1 && self.field_ends[expect] != self.field_ends[expect - 1])
        {
            Err(self.csv_error(&format!(
                "too many fields, expect {}, got {}",
                expect, actual
            )))
        } else {
            Ok(())
        }
    }

    fn error_output_full(&self) -> ErrorCode {
        self.csv_error("Bug: CSV Reader return output longer then input.")
    }

    fn error_output_ends_full(&self) -> ErrorCode {
        if self.projection.is_some() {
            self.csv_error(&format!(
                "too many fields, expect {}, got more than {}",
                self.num_fields,
                self.field_ends.len()
            ))
        } else {
            self.csv_error(&format!(
                "select from CSV allow at most {} fields",
                MAX_CSV_COLUMNS
            ))
        }
    }

    fn csv_error(&self, msg: &str) -> ErrorCode {
        self.ctx.parse_error_row_based(
            msg,
            &self.split_info,
            self.common.offset,
            self.common.rows,
            self.split_info.start_row_text(),
        )
    }
}
