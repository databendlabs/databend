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
use databend_common_meta_app::principal::TsvFileFormatParams;

use crate::read::row_based::batch::BytesBatch;
use crate::read::row_based::format::SeparatorState;
use crate::read::row_based::formats::tsv::separator::TsvRowSeparator;

pub struct TsvFieldReader<'a> {
    row: &'a [u8],
    field_delimiter: u8,
    field_start: usize,
    field_end: usize,
    in_escape: bool,
}

impl<'a> TsvFieldReader<'a> {
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

impl<'a> Iterator for TsvFieldReader<'a> {
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

pub fn normalize_tsv_for_infer_schema(
    input: &[u8],
    params: &TsvFileFormatParams,
    is_eof: bool,
) -> Result<Vec<u8>> {
    let record_delimiter = *params.record_delimiter.as_bytes().last().ok_or_else(|| {
        ErrorCode::BadBytes("empty TSV record delimiter when infer schema".to_string())
    })?;
    let field_delimiter = params
        .field_delimiter
        .as_bytes()
        .first()
        .copied()
        .ok_or_else(|| {
            ErrorCode::BadBytes("empty TSV field delimiter when infer schema".to_string())
        })?;

    let mut separator = TsvRowSeparator::try_create("infer_schema", record_delimiter, 0)?;
    let batch = BytesBatch {
        data: input.to_vec(),
        path: "infer_schema".to_string(),
        offset: 0,
        is_eof,
    };
    let (batches, _) = separator.append(batch)?;

    let mut normalized = Vec::with_capacity(input.len());
    let mut field_buffer = Vec::new();
    let trim_cr = params.record_delimiter.len() > 1;
    for row_batch in batches {
        let data = row_batch
            .data
            .into_nd_json()
            .map_err(|_| ErrorCode::BadBytes("expected NDJson row batch for TSV".to_string()))?;
        for mut row in data.iter() {
            row = trim_record_delimiter(row, record_delimiter, trim_cr);
            if row.is_empty() {
                continue;
            }
            normalize_row(row, field_delimiter, &mut normalized, &mut field_buffer)?;
        }
    }
    Ok(normalized)
}

pub fn parse_tsv_records_for_infer_schema(
    input: &[u8],
    params: &TsvFileFormatParams,
    is_eof: bool,
) -> Result<Vec<Vec<Vec<u8>>>> {
    let record_delimiter = *params.record_delimiter.as_bytes().last().ok_or_else(|| {
        ErrorCode::BadBytes("empty TSV record delimiter when infer schema".to_string())
    })?;
    let field_delimiter = params
        .field_delimiter
        .as_bytes()
        .first()
        .copied()
        .ok_or_else(|| {
            ErrorCode::BadBytes("empty TSV field delimiter when infer schema".to_string())
        })?;

    let mut separator = TsvRowSeparator::try_create("infer_schema", record_delimiter, 0)?;
    let batch = BytesBatch {
        data: input.to_vec(),
        path: "infer_schema".to_string(),
        offset: 0,
        is_eof,
    };
    let (batches, _) = separator.append(batch)?;

    let trim_cr = params.record_delimiter.len() > 1;
    let mut field_buffer = Vec::new();
    let mut records = Vec::new();
    for row_batch in batches {
        let data = row_batch
            .data
            .into_nd_json()
            .map_err(|_| ErrorCode::BadBytes("expected NDJson row batch for TSV".to_string()))?;
        for mut row in data.iter() {
            row = trim_record_delimiter(row, record_delimiter, trim_cr);
            if row.is_empty() {
                continue;
            }
            let mut columns = Vec::new();
            for field in TsvFieldReader::new(row, field_delimiter) {
                decode_tsv_field(field, &mut field_buffer)?;
                columns.push(field_buffer.clone());
            }
            records.push(columns);
        }
    }
    Ok(records)
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

fn normalize_row(
    row: &[u8],
    field_delimiter: u8,
    output: &mut Vec<u8>,
    field_buffer: &mut Vec<u8>,
) -> Result<()> {
    let mut is_first = true;

    for field in TsvFieldReader::new(row, field_delimiter) {
        if !is_first {
            output.push(b',');
        }
        append_field_as_csv(field, output, field_buffer)?;
        is_first = false;
    }

    output.push(b'\n');
    Ok(())
}

fn append_field_as_csv(
    field: &[u8],
    output: &mut Vec<u8>,
    field_buffer: &mut Vec<u8>,
) -> Result<()> {
    decode_tsv_field(field, field_buffer)?;

    output.push(b'"');
    for b in field_buffer.iter() {
        if *b == b'"' {
            output.extend_from_slice(b"\"\"");
        } else {
            output.push(*b);
        }
    }
    output.push(b'"');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_tsv_for_infer_schema() -> Result<()> {
        let params = TsvFileFormatParams::default();
        let normalized = normalize_tsv_for_infer_schema(b"1\\t2\t3\\n4\n", &params, true)?;
        assert_eq!(normalized, b"\"1\t2\",\"3\n4\"\n");
        Ok(())
    }

    #[test]
    fn test_normalize_tsv_for_infer_schema_non_eof() -> Result<()> {
        let params = TsvFileFormatParams::default();
        let normalized = normalize_tsv_for_infer_schema(b"1\t2\n3\t4", &params, false)?;
        assert_eq!(normalized, b"\"1\",\"2\"\n");
        Ok(())
    }

    #[test]
    fn test_tsv_field_reader() {
        let row = b"a\\tb\tc\\\\\td\t";
        let fields: Vec<&[u8]> = TsvFieldReader::new(row, b'\t').collect();
        assert_eq!(fields, vec![
            b"a\\tb".as_slice(),
            b"c\\\\".as_slice(),
            b"d".as_slice(),
            b""
        ]);
    }

    #[test]
    fn test_parse_tsv_records_for_infer_schema() -> Result<()> {
        let params = TsvFileFormatParams::default();
        let records = parse_tsv_records_for_infer_schema(b"1\\t2\t3\\n4\n5\t6\n", &params, true)?;
        assert_eq!(records, vec![
            vec![b"1\t2".to_vec(), b"3\n4".to_vec()],
            vec![b"5".to_vec(), b"6".to_vec()]
        ]);
        Ok(())
    }
}
