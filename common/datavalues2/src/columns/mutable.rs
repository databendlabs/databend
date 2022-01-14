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

use crate::prelude::*;
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

// pub fn create_mutable_array(datatype: &DataTypePtr) -> Box<dyn MutableColumn> {
//     match datatype.data_type_id() {
//         // TODO
//         TypeID::Boolean => Box::new(MutableBooleanColumn::default()),
//         TypeID::UInt8 => Box::new(MutablePrimitiveColumn::<u8>::default()),
//         TypeID::UInt16 => Box::new(MutablePrimitiveColumn::<u16>::default()),
//         TypeID::UInt32 => Box::new(MutablePrimitiveColumn::<u32>::default()),
//         TypeID::UInt64 => Box::new(MutablePrimitiveColumn::<u64>::default()),
//         TypeID::Int8 => Box::new(MutablePrimitiveColumn::<i8>::default()),
//         TypeID::Int16 => Box::new(MutablePrimitiveColumn::<i16>::default()),
//         TypeID::Int32 => Box::new(MutablePrimitiveColumn::<i32>::default()),
//         TypeID::Int64 => Box::new(MutablePrimitiveColumn::<i64>::default()),
//         TypeID::Float32 => Box::new(MutablePrimitiveColumn::<f32>::default()),
//         TypeID::Float64 => Box::new(MutablePrimitiveColumn::<f64>::default()),
//         TypeID::String => Box::new(MutableStringColumn::default()),
//         TypeID::Null => Box::new(MutableNullColumn::default()),
//         TypeID::Nullable => {
//             let ty: &DataTypeNullable = datatype.as_any().downcast_ref().unwrap();
//             let mutable_inner = create_mutable_array(ty.inner_type());
//             Box::new(MutableNullableColumn::new(mutable_inner))
//         }
//         _ => {
//             todo!()
//         }
//     }
// }
