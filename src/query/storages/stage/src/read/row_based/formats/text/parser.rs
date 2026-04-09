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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::cursor_ext::BufferReadStringExt;
use databend_common_meta_app::principal::TextFileFormatParams;

use crate::read::row_based::batch::BytesBatch;
use crate::read::row_based::batch::NdJsonRowBatchIter;
use crate::read::row_based::batch::NdjsonRowBatch;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::SeparatorState;
use crate::read::row_based::formats::text::separator::TextRowSeparator;
use crate::read::row_based::utils::trim_ascii_space;

pub struct TextFieldReader<'a> {
    row: &'a [u8],
    field_delimiter: u8,
    field_start: usize,
    field_end: usize,
    in_escape: bool,
}

impl<'a> TextFieldReader<'a> {
    pub fn new(row: &'a [u8], field_delimiter: u8) -> Self {
        Self {
            row,
            field_delimiter,
            field_start: 0,
            field_end: 0,
            in_escape: false,
        }
    }
}

impl<'a> Iterator for TextFieldReader<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        while self.field_end <= self.row.len() {
            if self.field_end == self.row.len()
                || (self.row[self.field_end] == self.field_delimiter && !self.in_escape)
            {
                let field = &self.row[self.field_start..self.field_end];
                self.field_start = self.field_end + 1;
                self.field_end += 1;
                return Some(field);
            }
            self.in_escape = (self.row[self.field_end] == b'\\') && !self.in_escape;
            self.field_end += 1;
        }
        None
    }
}

pub fn decode_tsv_field(col_data: &[u8], output: &mut Vec<u8>) -> Result<()> {
    let mut cursor = Cursor::new(col_data);
    output.clear();
    cursor
        .read_escaped_string_text(output)
        .map_err(|e| ErrorCode::BadBytes(e.to_string()))
}

struct InferSchemaIter {
    batches: std::vec::IntoIter<RowBatchWithPosition>,
    current_rows: Option<NdJsonRowBatchIter<'static>>,
    current_data: Option<Box<NdjsonRowBatch>>,
    current_fields: Option<std::iter::Enumerate<TextFieldReader<'static>>>,
    field_buf: Vec<u8>,
    record_delimiter: u8,
    field_delimiter: u8,
    trim_cr: bool,
    trim_space: bool,
}

impl Drop for InferSchemaIter {
    fn drop(&mut self) {
        self.current_rows = None;
        self.current_data = None;
    }
}

impl Iterator for InferSchemaIter {
    type Item = Result<(usize, String)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(fields) = &mut self.current_fields {
                if let Some((i, field)) = fields.next() {
                    let field = if self.trim_space {
                        trim_ascii_space(field)
                    } else {
                        field
                    };
                    return Some(
                        decode_tsv_field(field, &mut self.field_buf)
                            .map(|_| (i, String::from_utf8_lossy(&self.field_buf).to_string())),
                    );
                } else {
                    self.current_fields = None;
                }
            }

            if let Some(rows) = &mut self.current_rows {
                if let Some(row) = rows.next() {
                    let row = trim_record_delimiter(row, self.record_delimiter, self.trim_cr);

                    let reader = TextFieldReader::new(row, self.field_delimiter).enumerate();

                    self.current_fields = Some(reader);
                    continue;
                } else {
                    self.current_rows = None;
                }
            }

            let batch = self.batches.next()?;
            match batch.data.into_nd_json() {
                Ok(data) => {
                    let boxed = Box::new(data);
                    let ptr: *const NdjsonRowBatch = &*boxed;
                    let iter = unsafe { (*ptr).iter() };
                    let iter_static: NdJsonRowBatchIter<'static> =
                        unsafe { std::mem::transmute(iter) };

                    self.current_data = Some(boxed);
                    self.current_rows = Some(iter_static);
                    continue;
                }
                Err(_) => {
                    return Some(Err(ErrorCode::BadBytes(
                        "expected NDJson row batch for TEXT".to_string(),
                    )));
                }
            }
        }
    }
}

pub fn parse_tsv_records_for_infer_schema(
    input: &[u8],
    params: &TextFileFormatParams,
    is_eof: bool,
) -> Result<impl Iterator<Item = Result<(usize, String)>>> {
    let record_delimiter = params.record_delimiter.as_bytes();
    let record_delimiter_byte = *record_delimiter.last().ok_or_else(|| {
        ErrorCode::BadBytes("empty TEXT record delimiter when infer schema".to_string())
    })?;
    let field_delimiter = params
        .field_delimiter
        .as_bytes()
        .first()
        .copied()
        .ok_or_else(|| {
            ErrorCode::BadBytes("empty TEXT field delimiter when infer schema".to_string())
        })?;

    let mut separator = TextRowSeparator::try_create("infer_schema", record_delimiter, 0)?;
    let batch = BytesBatch {
        data: input.to_vec(),
        path: "infer_schema".to_string(),
        offset: 0,
        is_eof,
    };
    let (batches, _) = separator.append(batch)?;

    let trim_cr: bool = params.record_delimiter.len() > 1;
    let iter = InferSchemaIter {
        batches: batches.into_iter(),

        current_rows: None,
        current_data: None,
        current_fields: None,

        field_buf: Vec::new(),

        record_delimiter: record_delimiter_byte,
        field_delimiter,
        trim_cr,
        trim_space: params.trim_space,
    };
    Ok(iter)
}

fn trim_record_delimiter(mut row: &[u8], record_delimiter: u8, trim_cr: bool) -> &[u8] {
    let len = row.len();
    if len > 0 && row[len - 1] == record_delimiter {
        row = &row[..(len - 1)];
        if trim_cr && len > 2 && row[len - 2] == b'\r' {
            row = &row[..(len - 2)];
        }
    }
    row
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tsv_field_reader() {
        let row = b"a\\tb\tc\\\\\td\t";
        let fields: Vec<&[u8]> = TextFieldReader::new(row, b'\t').collect();
        assert_eq!(fields, vec![
            b"a\\tb".as_slice(),
            b"c\\\\".as_slice(),
            b"d".as_slice(),
            b""
        ]);
    }

    #[test]
    fn test_parse_tsv_records_for_infer_schema() -> Result<()> {
        let params = TextFileFormatParams::default();
        let records = parse_tsv_records_for_infer_schema(b"1\\t2\t3\\n4\n5\t6\n", &params, true)?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(records, vec![
            (0, "1\t2".to_string()),
            (1, "3\n4".to_string()),
            (0, "5".to_string()),
            (1, "6".to_string()),
        ]);
        Ok(())
    }

    #[test]
    fn test_parse_tsv_records_for_infer_schema_trim_space() -> Result<()> {
        let params = TextFileFormatParams {
            trim_space: true,
            ..TextFileFormatParams::default()
        };
        let records =
            parse_tsv_records_for_infer_schema(b"  42  \t  hello  \t  \\N  \n", &params, true)?
                .collect::<Result<Vec<_>>>()?;
        assert_eq!(records, vec![
            (0, "42".to_string()),
            (1, "hello".to_string()),
            (2, "\\N".to_string()),
        ]);
        Ok(())
    }
}
