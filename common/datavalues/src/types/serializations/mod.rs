// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

use crate::prelude::DataColumn;
use crate::DataType;

mod boolean;
mod date;
mod date_time;
mod null;
mod number;
mod string;

pub use boolean::*;
pub use date::*;
pub use date_time::*;
pub use null::*;
pub use number::*;
pub use string::*;

pub trait TypeSerializer {
    fn serialize_strings(&self, column: &DataColumn) -> Result<Vec<String>>;
}

impl DataType {
    pub fn get_serializer(&self) -> Box<dyn TypeSerializer> {
        match self {
            DataType::Null => Box::new(NullSerializer {}),
            DataType::Boolean => Box::new(BooleanSerializer {}),
            DataType::UInt8 => Box::new(NumberSerializer::<u8>::default()),
            DataType::UInt16 => Box::new(NumberSerializer::<u16>::default()),
            DataType::UInt32 => Box::new(NumberSerializer::<u32>::default()),
            DataType::UInt64 => Box::new(NumberSerializer::<u64>::default()),
            DataType::Int8 => Box::new(NumberSerializer::<i8>::default()),
            DataType::Int16 => Box::new(NumberSerializer::<i16>::default()),
            DataType::Int32 => Box::new(NumberSerializer::<i32>::default()),
            DataType::Int64 => Box::new(NumberSerializer::<i64>::default()),
            DataType::Float32 => Box::new(NumberSerializer::<f32>::default()),
            DataType::Float64 => Box::new(NumberSerializer::<f64>::default()),
            DataType::Date16 => Box::new(DateSerializer::<u16>::default()),
            DataType::Date32 => Box::new(DateSerializer::<u32>::default()),
            DataType::DateTime32 => Box::new(DateTimeSerializer::<u32>::default()),
            DataType::String => Box::new(StringSerializer {}),
            DataType::List(_) => todo!(),
            DataType::Struct(_) => todo!(),
        }
    }
}
