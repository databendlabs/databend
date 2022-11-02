// Copyright 2021 Datafuse Labs.
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
use common_exception::Result;
use common_io::prelude::FormatSettings;
use serde_json::Value;

use crate::prelude::*;

#[derive(Clone)]
pub struct ConstSerializer<'a> {
    pub size: usize,
    pub inner: Box<TypeSerializerImpl<'a>>,
}

impl<'a> ConstSerializer<'a> {
    fn repeat<T: Clone>(&self, one: Vec<T>) -> Vec<T> {
        let mut res = Vec::with_capacity(self.size);
        for _ in 0..self.size {
            res.push(one[0].clone())
        }
        res
    }
}
impl<'a> TypeSerializer<'a> for ConstSerializer<'a> {
    fn write_field_values(
        &self,
        _row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        in_nested: bool,
    ) {
        self.inner.write_field_values(0, buf, format, in_nested)
    }

    fn write_field_tsv(&self, _row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        self.inner.write_field_tsv(0, buf, format)
    }

    fn write_field_csv(&self, _row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        self.inner.write_field_csv(0, buf, format)
    }

    fn write_field_json(
        &self,
        _row_index: usize,
        buf: &mut Vec<u8>,
        format: &FormatSettings,
        quote: bool,
    ) {
        self.inner.write_field_json(0, buf, format, quote)
    }

    fn serialize_json_values(&self, format: &FormatSettings) -> Result<Vec<Value>> {
        Ok(self.repeat(self.inner.serialize_json_values(format)?))
    }

    fn serialize_json_object(
        &self,
        valids: Option<&Bitmap>,
        format: &FormatSettings,
    ) -> Result<Vec<Value>> {
        Ok(self.repeat(self.inner.serialize_json_object(valids, format)?))
    }

    fn serialize_json_object_suppress_error(
        &self,
        format: &FormatSettings,
    ) -> Result<Vec<Option<Value>>> {
        Ok(self.repeat(self.inner.serialize_json_object_suppress_error(format)?))
    }
}
