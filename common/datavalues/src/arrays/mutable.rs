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

use std::{any::Any, sync::Arc};

use super::MutableBooleanArrayBuilder;
use super::MutablePrimitiveArrayBuilder;
use super::MutableStringArrayBuilder;
use common_arrow::arrow::array::Array;
use crate::DataType;

pub trait MutableArrayBuilder {
    fn data_type(&self) -> DataType;
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn as_arc(&mut self) -> Arc<dyn Array>;
}

pub fn create_mutable_array(datatype: DataType) -> Box<dyn MutableArrayBuilder> {
    match datatype {
        DataType::Boolean => Box::new(MutableBooleanArrayBuilder::new()),
        DataType::UInt8 => Box::new(MutablePrimitiveArrayBuilder::<u8>::new()),
        DataType::UInt16 => Box::new(MutablePrimitiveArrayBuilder::<u16>::new()),
        DataType::UInt32 => Box::new(MutablePrimitiveArrayBuilder::<u32>::new()),
        DataType::UInt64 => Box::new(MutablePrimitiveArrayBuilder::<u64>::new()),
        DataType::Int8 => Box::new(MutablePrimitiveArrayBuilder::<i8>::new()),
        DataType::Int16 => Box::new(MutablePrimitiveArrayBuilder::<i16>::new()),
        DataType::Int32 => Box::new(MutablePrimitiveArrayBuilder::<i32>::new()),
        DataType::Int64 => Box::new(MutablePrimitiveArrayBuilder::<i64>::new()),
        DataType::Float32 => Box::new(MutablePrimitiveArrayBuilder::<f32>::new()),
        DataType::Float64 => Box::new(MutablePrimitiveArrayBuilder::<f64>::new()),
        DataType::String => Box::new(MutableStringArrayBuilder::new()),
        _ => {
            todo!()
        }
    }
}
