// Copyright [2021] [Jorge C Leitao]
// Copyright 2021 Datafuse Labs
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

use parquet_format_safe::thrift::protocol::TCompactInputProtocol;
use parquet_format_safe::ColumnIndex;

use crate::error::Error;
use crate::indexes::BooleanIndex;
use crate::indexes::ByteIndex;
use crate::indexes::FixedLenByteIndex;
use crate::indexes::Index;
use crate::indexes::NativeIndex;
use crate::schema::types::PhysicalType;
use crate::schema::types::PrimitiveType;

pub fn deserialize(data: &[u8], primitive_type: PrimitiveType) -> Result<Box<dyn Index>, Error> {
    let mut prot = TCompactInputProtocol::new(data, data.len() * 2 + 1024);

    let index = ColumnIndex::read_from_in_protocol(&mut prot)?;

    let index = match primitive_type.physical_type {
        PhysicalType::Boolean => Box::new(BooleanIndex::try_new(index)?) as Box<dyn Index>,
        PhysicalType::Int32 => Box::new(NativeIndex::<i32>::try_new(index, primitive_type)?),
        PhysicalType::Int64 => Box::new(NativeIndex::<i64>::try_new(index, primitive_type)?),
        PhysicalType::Int96 => Box::new(NativeIndex::<[u32; 3]>::try_new(index, primitive_type)?),
        PhysicalType::Float => Box::new(NativeIndex::<f32>::try_new(index, primitive_type)?),
        PhysicalType::Double => Box::new(NativeIndex::<f64>::try_new(index, primitive_type)?),
        PhysicalType::ByteArray => Box::new(ByteIndex::try_new(index, primitive_type)?),
        PhysicalType::FixedLenByteArray(_) => {
            Box::new(FixedLenByteIndex::try_new(index, primitive_type)?)
        }
    };

    Ok(index)
}
