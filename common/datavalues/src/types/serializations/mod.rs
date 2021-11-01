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

use chrono_tz::Tz;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
use crate::DataType;

mod boolean;
mod date;
mod date_time;
mod number;
mod string;

pub use boolean::*;
pub use date::*;
pub use date_time::*;
pub use number::*;
pub use string::*;

// capacity.
pub trait TypeSerializer {
    fn serialize_strings(&self, column: &DataColumn) -> Result<Vec<String>>;

    fn de(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn de_batch(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()>;
    /// If error occurrs, append a null by default
    fn de_text(&mut self, reader: &[u8]) -> Result<()>;
    fn de_null(&mut self);
    fn finish_to_series(&mut self) -> Series;
}

impl DataType {
    pub fn create_serializer(&self, capacity: usize) -> Result<Box<dyn TypeSerializer>> {
        let data_type = self.clone();

        with_match_primitive_type!(data_type, |$T| {
                Ok(Box::new(NumberSerializer::<$T> {
                    builder: PrimitiveArrayBuilder::<$T>::with_capacity( capacity ),
                }))
            },

            {match data_type {
                DataType::Boolean => Ok(Box::new(BooleanSerializer {
                    builder: BooleanArrayBuilder::with_capacity(capacity),
                })),
                DataType::Date16 => Ok(Box::new(DateSerializer::<u16> {
                    builder: PrimitiveArrayBuilder::<u16>::with_capacity(capacity),
                })),
                DataType::Date32 => Ok(Box::new(DateSerializer::<i32> {
                    builder: PrimitiveArrayBuilder::<i32>::with_capacity(capacity),
                })),
                DataType::DateTime32(tz) => {
                    let tz = tz.unwrap_or_else(|| "UTC".to_string());
                    Ok(Box::new(DateTimeSerializer::<u32> {
                        builder: PrimitiveArrayBuilder::<u32>::with_capacity(capacity),
                        tz: tz.parse::<Tz>().unwrap(),
                    }))
                }
                DataType::String => Ok(Box::new(StringSerializer {
                    builder: StringArrayBuilder::with_capacity(capacity),
                })),
                DataType::Interval(_) => Ok(Box::new(DateSerializer::<i64> {
                    builder: PrimitiveArrayBuilder::<i64>::with_capacity(capacity),
                })),
                other => Err(ErrorCode::BadDataValueType(format!(
                    "create_serializer does not support type '{:?}'",
                    other
                ))),
            }
        })
    }
}
