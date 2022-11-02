// Copyright 2022 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;
use common_io::prelude::FormatSettings;

use crate::types::DataType;
use crate::Column;
use crate::TypeSerializer;

pub struct NullableSerializer {
    pub validity: Bitmap,
    pub inner: Box<dyn TypeSerializer>,
}

impl NullableSerializer {
    pub fn try_create(col: Column, inner_ty: &DataType) -> Result<Self, String> {
        let column = col
            .into_nullable()
            .map_err(|_| "unable to get nullable column".to_string())?;

        let inner = inner_ty.create_serializer(column.column)?;
        Ok(Self {
            validity: column.validity,
            inner,
        })
    }
}

impl TypeSerializer for NullableSerializer {
    fn need_quote(&self) -> bool {
        self.inner.need_quote()
    }

    fn write_field(&self, row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        if !self.validity.get_bit(row_index) {
            buf.extend_from_slice(&format.null_bytes);
        } else {
            self.inner.write_field(row_index, buf, format)
        }
    }

    fn write_field_escaped(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        quote: u8,
    ) {
        if !self.validity.get_bit(row_index) {
            buf.extend_from_slice(&format.null_bytes);
        } else {
            self.inner
                .write_field_escaped(row_index, buf, format, quote)
        }
    }

    fn write_field_quoted(
        &self,
        row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        quote: u8,
    ) {
        if !self.validity.get_bit(row_index) {
            buf.extend_from_slice(&format.null_bytes);
        } else {
            self.inner.write_field_quoted(row_index, buf, format, quote)
        }
    }
}
