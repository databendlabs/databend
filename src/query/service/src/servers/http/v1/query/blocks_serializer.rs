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

use std::cell::RefCell;
use std::ops::DerefMut;

use databend_common_expression::Column;
use databend_common_formats::field_encoder::FieldEncoderValues;
use databend_common_io::prelude::FormatSettings;
use serde::ser::SerializeSeq;

fn data_is_null(column: &Column, row_index: usize) -> bool {
    match column {
        Column::Null { .. } => true,
        Column::Nullable(box inner) => !inner.validity.get_bit(row_index),
        _ => false,
    }
}

#[derive(Debug, Clone)]
pub struct BlocksSerializer {
    // Vec<Column> for a Block
    columns: Vec<(Vec<Column>, usize)>,
    pub(crate) format: Option<FormatSettings>,
}

impl BlocksSerializer {
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            format: None,
        }
    }

    pub fn new(format: Option<FormatSettings>) -> Self {
        Self {
            columns: vec![],
            format,
        }
    }

    pub fn has_format(&self) -> bool {
        self.format.is_some()
    }

    pub fn set_format(&mut self, format: FormatSettings) {
        self.format = Some(format);
    }

    pub fn append(&mut self, columns: Vec<Column>, num_rows: usize) {
        self.columns.push((columns, num_rows));
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn num_rows(&self) -> usize {
        self.columns.iter().map(|(_, num_rows)| *num_rows).sum()
    }
}

impl serde::Serialize for BlocksSerializer {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut serialize_seq = serializer.serialize_seq(Some(self.num_rows()))?;
        if let Some(format) = &self.format {
            let mut buf = RefCell::new(Vec::new());
            let encoder = FieldEncoderValues::create_for_http_handler(
                format.jiff_timezone.clone(),
                format.timezone,
                format.geometry_format,
            );
            for (columns, num_rows) in self.columns.iter() {
                for i in 0..*num_rows {
                    serialize_seq.serialize_element(&RowSerializer {
                        format,
                        data_block: columns,
                        encodeer: &encoder,
                        buf: &mut buf,
                        row_index: i,
                    })?
                }
            }
        }
        serialize_seq.end()
    }
}

struct RowSerializer<'a> {
    format: &'a FormatSettings,
    data_block: &'a [Column],
    encodeer: &'a FieldEncoderValues,
    buf: &'a RefCell<Vec<u8>>,
    row_index: usize,
}

impl<'a> serde::Serialize for RowSerializer<'a> {
    fn serialize<S>(&self, serializer: S) -> core::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut serialize_seq = serializer.serialize_seq(Some(self.data_block.len()))?;

        for column in self.data_block.iter() {
            if !self.format.format_null_as_str && data_is_null(column, self.row_index) {
                serialize_seq.serialize_element(&None::<String>)?;
                continue;
            }
            let string = self
                .encodeer
                .try_direct_as_string(&column, self.row_index, false)
                .unwrap_or_else(|| {
                    let mut buf = self.buf.borrow_mut();
                    buf.clear();
                    self.encodeer
                        .write_field(column, self.row_index, buf.deref_mut(), false);
                    String::from_utf8_lossy(buf.deref_mut()).into_owned()
                });
            serialize_seq.serialize_element(&Some(string))?;
        }
        serialize_seq.end()
    }
}
