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

use std::any::Any;

use common_arrow::arrow::bitmap::MutableBitmap;

use super::MutableBooleanArrayBuilder;
use super::MutablePrimitiveArrayBuilder;
use super::MutableStringArrayBuilder;
use crate::series::Series;
use crate::DataType;

pub trait MutableArrayBuilder {
    fn data_type(&self) -> &DataType;
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn as_series(&mut self) -> Series;
    fn push_null(&mut self);
    fn validity(&self) -> Option<&MutableBitmap>;
}

pub fn create_mutable_array(datatype: DataType) -> Box<dyn MutableArrayBuilder> {
    match datatype {
        DataType::Boolean => Box::new(MutableBooleanArrayBuilder::<true>::default()),
        DataType::UInt8 => Box::new(MutablePrimitiveArrayBuilder::<u8, true>::default()),
        DataType::UInt16 => Box::new(MutablePrimitiveArrayBuilder::<u16, true>::default()),
        DataType::UInt32 => Box::new(MutablePrimitiveArrayBuilder::<u32, true>::default()),
        DataType::UInt64 => Box::new(MutablePrimitiveArrayBuilder::<u64, true>::default()),
        DataType::Int8 => Box::new(MutablePrimitiveArrayBuilder::<i8, true>::default()),
        DataType::Int16 => Box::new(MutablePrimitiveArrayBuilder::<i16, true>::default()),
        DataType::Int32 => Box::new(MutablePrimitiveArrayBuilder::<i32, true>::default()),
        DataType::Int64 => Box::new(MutablePrimitiveArrayBuilder::<i64, true>::default()),
        DataType::Float32 => Box::new(MutablePrimitiveArrayBuilder::<f32, true>::default()),
        DataType::Float64 => Box::new(MutablePrimitiveArrayBuilder::<f64, true>::default()),
        DataType::String => Box::new(MutableStringArrayBuilder::<true>::default()),
        _ => {
            todo!()
        }
    }
}
