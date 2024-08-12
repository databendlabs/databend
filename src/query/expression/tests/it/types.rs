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

use chrono_tz::Tz;
use databend_common_expression::types::number::NumberDataType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Scalar;
use databend_common_expression::Value;

#[test]
fn test_timestamp_to_string_formats() {
    // Unix timestamp for "2024-01-01 01:02:03" UTC
    let ts = 1_704_070_923_000_000;
    let tz = Tz::UTC;

    assert_eq!(
        timestamp_to_string(ts, tz).to_string(),
        "2024-01-01 01:02:03.000000"
    );
}

#[test]
fn test_block_entry_memory_size() {
    let scalar = Scalar::Number(NumberScalar::UInt8(1));
    let entry = BlockEntry {
        data_type: DataType::Number(NumberDataType::UInt8),
        value: Value::<AnyType>::Scalar(scalar.clone()),
    };
    let scalar_size = std::mem::size_of_val(&scalar);

    assert_eq!(144, scalar_size);
    assert_eq!(144, entry.memory_size());
}
