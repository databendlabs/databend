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

use crate::types::DataTypePtr;
use crate::ColumnRef;

pub trait MutableColumn {
    fn data_type(&self) -> DataTypePtr;
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
    fn as_column(&mut self) -> ColumnRef;

    fn append_default(&mut self);

    fn append_null(&mut self) -> bool {
        false
    }
    fn validity(&self) -> Option<&MutableBitmap> {
        None
    }
    fn shrink_to_fit(&mut self);
}

pub fn create_mutable_array(datatype: &DataTypePtr) -> Box<dyn MutableColumn> {
    match datatype {
        // TODO
        // DataType::Boolean => Box::new(MutableBooleanColumn::<true>::default()),
        // DataType::UInt8 => Box::new(MutablePrimitiveColumn::<u8, true>::default()),
        // DataType::UInt16 => Box::new(MutablePrimitiveColumn::<u16, true>::default()),
        // DataType::UInt32 => Box::new(MutablePrimitiveColumn::<u32, true>::default()),
        // DataType::UInt64 => Box::new(MutablePrimitiveColumn::<u64, true>::default()),
        // DataType::Int8 => Box::new(MutablePrimitiveColumn::<i8, true>::default()),
        // DataType::Int16 => Box::new(MutablePrimitiveColumn::<i16, true>::default()),
        // DataType::Int32 => Box::new(MutablePrimitiveColumn::<i32, true>::default()),
        // DataType::Int64 => Box::new(MutablePrimitiveColumn::<i64, true>::default()),
        // DataType::Float32 => Box::new(MutablePrimitiveColumn::<f32, true>::default()),
        // DataType::Float64 => Box::new(MutablePrimitiveColumn::<f64, true>::default()),
        // DataType::String => Box::new(MutableStringColumn::<true>::default()),
        _ => {
            todo!()
        }
    }
}
