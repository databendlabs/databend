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

use std::marker::PhantomData;

use common_arrow::arrow::datatypes::DataType as ArrowType;

use super::data_type::IDataType;
use super::type_id::TypeID;
use crate::DFDataType;
use crate::DFPrimitiveType;

pub struct DataTypeNumeric<T> {
    _t: PhantomData<T>,
}

impl<T> IDataType for DataTypeNumeric<T>
where T: DFPrimitiveType + DFDataType
{
    fn type_id(&self) -> TypeID {
        TypeID::Int16
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::Int16
    }
}

pub type DataTypeInt8 = DataTypeNumeric<i8>;
pub type DataTypeInt16 = DataTypeNumeric<i16>;
pub type DataTypeInt32 = DataTypeNumeric<i32>;
pub type DataTypeInt64 = DataTypeNumeric<i64>;
pub type DataTypeUInt8 = DataTypeNumeric<u8>;
pub type DataTypeUInt16 = DataTypeNumeric<u16>;
pub type DataTypeUInt32 = DataTypeNumeric<u32>;
pub type DataTypeUInt64 = DataTypeNumeric<u64>;
pub type DataTypeFloat32 = DataTypeNumeric<f32>;
pub type DataTypeFloat64 = DataTypeNumeric<f64>;
