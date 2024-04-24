// Copyright 2022 Datafuse Labs.
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

use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::OwnedMemoryUsageSize;
use databend_common_base::runtime::ThreadTracker;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;

#[test]
fn test_data_types_owned_memory_usage() {
    fn test_data_type<F: Fn() -> DataType>(f: F) {
        let mem_stat = MemStat::create("TEST".to_string());
        let mut payload = ThreadTracker::new_tracking_payload();

        payload.mem_stat = Some(mem_stat.clone());

        let _guard = ThreadTracker::tracking(payload);

        let mut data_type = f();

        drop(_guard);
        assert_eq!(
            mem_stat.get_memory_usage(),
            data_type.owned_memory_usage() as i64
        );
    }

    test_data_type(|| DataType::Null);
    test_data_type(|| DataType::EmptyArray);
    test_data_type(|| DataType::EmptyMap);
    test_data_type(|| DataType::Number(NumberDataType::Int8));
    test_data_type(|| DataType::Number(NumberDataType::Int16));
    test_data_type(|| DataType::Number(NumberDataType::Int32));
    test_data_type(|| DataType::Number(NumberDataType::Int64));
    test_data_type(|| DataType::Number(NumberDataType::UInt8));
    test_data_type(|| DataType::Number(NumberDataType::UInt16));
    test_data_type(|| DataType::Number(NumberDataType::UInt32));
    test_data_type(|| DataType::Number(NumberDataType::UInt64));
    test_data_type(|| DataType::Number(NumberDataType::Float32));
    test_data_type(|| DataType::Number(NumberDataType::Float64));
    test_data_type(|| {
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 0,
            scale: 0,
        }))
    });
    test_data_type(|| {
        DataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
            precision: 0,
            scale: 0,
        }))
    });
    test_data_type(|| DataType::Timestamp);
    test_data_type(|| DataType::Date);
    test_data_type(|| DataType::Boolean);
    test_data_type(|| DataType::Binary);
    test_data_type(|| DataType::String);
    test_data_type(|| DataType::Array(Box::new(DataType::Binary)));
    test_data_type(|| DataType::Map(Box::new(DataType::String)));
    test_data_type(|| DataType::Bitmap);
    test_data_type(|| {
        DataType::Tuple({
            let mut data_types = Vec::with_capacity(1000);
            data_types.push(DataType::Null);
            data_types.push(DataType::String);
            data_types.push(DataType::Array(Box::new(DataType::Binary)));
            data_types
        })
    });
    test_data_type(|| DataType::Variant);
    test_data_type(|| DataType::Geometry);
    test_data_type(|| DataType::Generic(1));
}
