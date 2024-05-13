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

use arrow_array::types::ByteArrayType;
use arrow_array::types::LargeBinaryType;
use arrow_array::types::LargeUtf8Type;
use arrow_array::Array;
use arrow_array::GenericByteArray;
use arrow_schema::DataType as ArrowDataType;
use databend_common_exception::Result;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use parquet_rs::basic::Encoding;
use parquet_rs::file::properties::WriterPropertiesBuilder;
use parquet_rs::schema::types::ColumnPath;

const ROWS_PER_DISTINCT_THRESHOLD: f64 = 10.0;
const SAMPLE_ROWS: usize = 1000;
const AVERAGE_PREFIX_LEN_THRESHOLD: f64 = 8.0;

pub fn choose_byte_array_encoding(
    mut props: WriterPropertiesBuilder,
    stat: Option<&ColumnStatistics>,
    array: Arc<dyn Array>,
    column_name: &str,
) -> Result<WriterPropertiesBuilder> {
    if array.is_empty() {
        return Ok(props);
    }
    let col_path = ColumnPath::new(vec![column_name.to_string()]);
    let ndv = stat.as_ref().and_then(|s| s.distinct_of_values);
    let num_rows = array.len();
    if let Some(ndv) = ndv {
        if num_rows as f64 / ndv as f64 > ROWS_PER_DISTINCT_THRESHOLD {
            props = props.set_column_dictionary_enabled(col_path, true);
            return Ok(props);
        }
    }
    let data_type = array.data_type();
    match data_type {
        ArrowDataType::LargeBinary => {
            if can_apply_delta_byte_array::<LargeBinaryType>(&array)? {
                props = props.set_column_encoding(col_path, Encoding::DELTA_BYTE_ARRAY);
                return Ok(props);
            }
        }
        ArrowDataType::LargeUtf8 => {
            if can_apply_delta_byte_array::<LargeUtf8Type>(&array)? {
                props = props.set_column_encoding(col_path, Encoding::DELTA_BYTE_ARRAY);
                return Ok(props);
            }
        }
        _ => {}
    };
    props = props.set_column_encoding(col_path, Encoding::DELTA_LENGTH_BYTE_ARRAY);
    Ok(props)
}

fn can_apply_delta_byte_array<T: ByteArrayType>(array: &dyn Array) -> Result<bool> {
    let num_rows = array.len();
    let array = array
        .as_any()
        .downcast_ref::<GenericByteArray<T>>()
        .unwrap();
    let mut sum_prefix_len = 0;
    for i in 1..num_rows.min(SAMPLE_ROWS) {
        let last: &[u8] = array.value(i - 1).as_ref();
        let cur: &[u8] = array.value(i).as_ref();
        let prefix_len = last
            .iter()
            .zip(cur.iter())
            .take_while(|(a, b)| a == b)
            .count();
        sum_prefix_len += prefix_len;
    }
    let avg_prefix_len = sum_prefix_len as f64 / num_rows as f64;
    Ok(avg_prefix_len > AVERAGE_PREFIX_LEN_THRESHOLD)
}
