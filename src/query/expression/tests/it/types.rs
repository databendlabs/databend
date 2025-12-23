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
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::types::timestamp::timestamp_to_string;
use jiff::fmt::strtime::BrokenDownTime;
use jiff::tz;
use jiff::tz::TimeZone;

use crate::DataTypeFilter;
use crate::get_all_test_data_types;
use crate::rand_block_for_all_types;

#[test]
fn test_timestamp_to_string_formats() {
    // Unix timestamp for "2024-01-01 01:02:03" UTC
    let ts = 1_704_070_923_000_000;
    let tz = TimeZone::UTC;

    assert_eq!(
        timestamp_to_string(ts, &tz).to_string(),
        "2024-01-01 01:02:03.000000"
    );
}

#[test]
fn test_parse_jiff() {
    let (mut tm, offset) = BrokenDownTime::parse_prefix(
        "%Y年%m月%d日，%H时%M分%S秒[America/New_York]Y",
        "2022年02月04日，8时58分59秒[America/New_York]Yxxxxxxxxxxx",
    )
    .unwrap();

    tm.set_offset(Some(tz::offset(0 as _)));
    let ts = tm.to_timestamp().unwrap();
    assert_eq!(ts.to_string(), "2022-02-04T08:58:59Z");
    assert_eq!(ts.as_microsecond(), 1643965139000000);
    assert_eq!(offset, 53);

    assert_eq!(
        "2022年02月04日，8时58分59秒[America/New_York]Y".len(),
        offset
    );

    // Jiff 0.2.16 requires a full civil date to build a datetime.  For inputs
    // that only specify a Unix timestamp (`%s`), verify via `to_timestamp`.
    let (mut tm, _) = BrokenDownTime::parse_prefix("%s", "200").unwrap();
    tm.set_offset(Some(tz::offset(0 as _)));
    assert_eq!(
        "1970-01-01T00:03:20Z",
        tm.to_timestamp().unwrap().to_string()
    );
}

#[test]
fn test_convert_types() {
    let all_types = get_all_test_data_types(DataTypeFilter::All);
    let all_fields = all_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| DataField::new(&format!("column_{idx}"), data_type.clone()))
        .collect::<Vec<_>>();

    let schema = DataSchema::new(all_fields);
    let arrow_schema = Schema::from(&schema);
    let schema2 = DataSchema::try_from(&arrow_schema).unwrap();
    assert_eq!(schema, schema2);

    let random_block = rand_block_for_all_types(1024, DataTypeFilter::All);
    for (idx, c) in random_block.columns().iter().enumerate() {
        let c = c.as_column().unwrap().clone();

        let data = serialize_column(&c);
        let c2 = deserialize_column(&data).unwrap();
        assert_eq!(c, c2, "in {idx} | datatype: {}", c.data_type());
    }
}
