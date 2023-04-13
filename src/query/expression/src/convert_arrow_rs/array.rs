// Copyright 2023 Datafuse Labs.
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

use std::sync::Arc;

use arrow_array::make_array;
use arrow_array::Array;
use arrow_array::ArrowPrimitiveType;
use arrow_array::BooleanArray;
use arrow_array::LargeBinaryArray;
use arrow_array::NullArray;
use arrow_array::PrimitiveArray;
use arrow_array::StructArray;
use arrow_buffer::buffer::BooleanBuffer;
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::ArrowNativeType;
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::TimeUnit;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer as Buffer2;
use common_arrow::arrow::types::NativeType;
use ordered_float::OrderedFloat;

use crate::types::decimal::DecimalColumn;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::F32;
use crate::types::F64;
use crate::Column;
use crate::DataField;
use crate::ARROW_EXT_TYPE_EMPTY_ARRAY;
use crate::ARROW_EXT_TYPE_EMPTY_MAP;
use crate::ARROW_EXT_TYPE_VARIANT;
use crate::EXTENSION_KEY;

impl Column {
    pub fn into_arrow_rs(self) -> Result<Arc<dyn Array>, ArrowError> {
        let arrow2_array = self.as_arrow();
        let arrow_array: Arc<dyn Array> = arrow2_array.into();
        Ok(arrow_array)
    }

    pub fn from_arrow_rs(array: Arc<dyn Array>, field: &Field) -> Result<Self, ArrowError> {
        let field = DataField::try_from(field)?;
        let arrow2_array: Box<dyn common_arrow::arrow::array::Array> = array.into();

        Ok(Column::from_arrow(
            arrow2_array.as_ref(),
            &field.data_type(),
        ))
    }
}
