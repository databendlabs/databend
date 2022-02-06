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

use common_clickhouse_srv::types::column::ArcColumnData;
use common_exception::Result;
use serde_json::Value;

use crate::prelude::*;
mod array;
mod boolean;
mod date;
mod date_time;
mod null;
mod nullable;
mod number;
mod string;
mod struct_;

pub use array::*;
pub use boolean::*;
pub use date::*;
pub use date_time::*;
pub use null::*;
pub use nullable::*;
pub use number::*;
pub use string::*;
pub use struct_::*;

pub trait TypeSerializer: Send + Sync {
    fn serialize_value(&self, value: &DataValue) -> Result<String>;
    fn serialize_json(&self, column: &ColumnRef) -> Result<Vec<Value>>;
    fn serialize_column(&self, column: &ColumnRef) -> Result<Vec<String>>;
    fn serialize_clickhouse_format(&self, column: &ColumnRef) -> Result<ArcColumnData>;
}
