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

use arrow_schema::Schema;
use chrono_tz::Tz;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;

use crate::get_all_test_data_types;
use crate::rand_block_for_all_types;

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
fn test_convert_types() {
    let all_types = get_all_test_data_types();
    let all_fields = all_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| DataField::new(&format!("column_{idx}"), data_type.clone()))
        .collect::<Vec<_>>();

    let schema = DataSchema::new(all_fields);
    let arrow_schema = Schema::from(&schema);
    let schema2 = DataSchema::try_from(&arrow_schema).unwrap();
    assert_eq!(schema, schema2);

    let random_block = rand_block_for_all_types(1024);
    for (idx, c) in random_block.columns().iter().enumerate() {
        let c = c.value.as_column().unwrap().clone();

        let data = serialize_column(&c);
        let c2 = deserialize_column(&data).unwrap();
        assert_eq!(c, c2, "in {idx} | datatype: {}", c.data_type());
    }
}
