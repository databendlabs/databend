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
use common_exception::ErrorCode;
use common_exception::Result;
use opensrv_clickhouse::types::column::ArcColumnData;
use serde_json::Value;

use crate::prelude::*;
mod array;
mod boolean;
mod date;
mod null;
mod nullable;
mod number;
mod string;
mod struct_;
mod timestamp;
mod variant;

pub use array::*;
pub use boolean::*;
pub use date::*;
pub use null::*;
pub use nullable::*;
pub use number::*;
pub use string::*;
pub use struct_::*;
pub use timestamp::*;
pub use variant::*;

pub trait TypeSerializer: Send + Sync {
    fn serialize_value(&self, value: &DataValue) -> Result<String>;
    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>>;
    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>>;
    fn serialize_clickhouse_format(&self, column: &ColumnRef) -> Result<ArcColumnData>;

    fn serialize_json_object(
        &self,
        _column: &ColumnRef,
        _valids: Option<&Bitmap>,
    ) -> Result<Vec<Value>> {
        Err(ErrorCode::BadDataValueType(
            "Error parsing JSON: unsupported data type",
        ))
    }

    fn serialize_json_object_suppress_error(
        &self,
        _column: &ColumnRef,
    ) -> Result<Vec<Option<Value>>> {
        Err(ErrorCode::BadDataValueType(
            "Error parsing JSON: unsupported data type",
        ))
    }
}
