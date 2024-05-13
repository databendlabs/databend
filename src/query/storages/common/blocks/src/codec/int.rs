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

use std::sync::Arc;

use arrow_array::types::ArrowPrimitiveType;
use arrow_array::types::Date32Type;
use arrow_array::types::Decimal128Type;
use arrow_array::types::Decimal256Type;
use arrow_array::types::Int32Type;
use arrow_array::types::Int64Type;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::types::UInt32Type;
use arrow_array::types::UInt64Type;
use arrow_array::Array;
use arrow_array::ArrowNativeTypeOp;
use arrow_array::PrimitiveArray;
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::TimeUnit;
use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use parquet_rs::basic::Encoding;
use parquet_rs::file::properties::WriterPropertiesBuilder;
use parquet_rs::schema::types::ColumnPath;

const MAX_WIDTH_THRESHOLD: i64 = 3;

pub fn choose_int_encoding(
    mut props: WriterPropertiesBuilder,
    _stat: Option<&ColumnStatistics>,
    array: Arc<dyn Array>,
    column_name: &str,
) -> Result<WriterPropertiesBuilder> {
    if array.is_empty() {
        return Ok(props);
    }
    let col_path = ColumnPath::new(vec![column_name.to_string()]);
    let data_type = array.data_type();
    let apply_delta = match data_type {
        ArrowDataType::Int32 => can_apply_delta_binary_pack::<Int32Type>(&array)?,
        ArrowDataType::Int64 => can_apply_delta_binary_pack::<Int64Type>(&array)?,
        ArrowDataType::UInt32 => can_apply_delta_binary_pack::<UInt32Type>(&array)?,
        ArrowDataType::UInt64 => can_apply_delta_binary_pack::<UInt64Type>(&array)?,
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            can_apply_delta_binary_pack::<TimestampMicrosecondType>(&array)?
        }
        ArrowDataType::Date32 => can_apply_delta_binary_pack::<Date32Type>(&array)?,
        ArrowDataType::Decimal128(_, _) => can_apply_delta_binary_pack::<Decimal128Type>(&array)?,
        ArrowDataType::Decimal256(_, _) => can_apply_delta_binary_pack::<Decimal256Type>(&array)?,
        _ => false,
    };
    if apply_delta {
        props = props.set_column_encoding(col_path, Encoding::DELTA_BINARY_PACKED);
    }
    Ok(props)
}

fn can_apply_delta_binary_pack<T: ArrowPrimitiveType>(array: &dyn Array) -> Result<bool> {
    let mut max_delta = T::Native::MIN_TOTAL_ORDER;
    let mut min_delta = T::Native::MAX_TOTAL_ORDER;
    let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    for i in 1..array.len() {
        let delta = array.value(i).sub_wrapping(array.value(i - 1));
        if delta.is_gt(max_delta) {
            max_delta = delta;
        }
        if delta.is_lt(min_delta) {
            min_delta = delta;
        }
    }
    let x = max_delta.sub_wrapping(min_delta).as_usize();
    Ok(x <= (1 << MAX_WIDTH_THRESHOLD))
}
