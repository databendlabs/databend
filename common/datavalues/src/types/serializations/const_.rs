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
    fn need_quote(&self) -> bool {
        self.inner.need_quote()
    }

    fn write_field(&self, _row_index: usize, buf: &mut Vec<u8>, format: &FormatSettings) {
        self.inner.write_field(0, buf, format)
    }

    fn serialize_json(&self, format: &FormatSettings) -> Result<Vec<Value>> {
        Ok(self.repeat(self.inner.serialize_json(format)?))
    }

    fn serialize_clickhouse_column(
        &self,
        format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        self.inner.serialize_clickhouse_const(format, self.size)
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
