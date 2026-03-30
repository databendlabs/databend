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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::FromData;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::TimestampTzType;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::number::Float64Type;
use databend_common_expression::types::number::Int32Type;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_io::prelude::BinaryDisplayFormat;
use databend_common_io::prelude::HttpHandlerDataFormat;
use databend_common_io::prelude::OutputFormatSettings;
use databend_query::servers::http::v1::BlocksCollector;
use pretty_assertions::assert_eq;

fn test_data_block(is_nullable: bool) -> Result<()> {
    let mut columns = vec![
        Int32Type::from_data(vec![1, 2, 3]),
        StringType::from_data(vec!["a", "b", "c"]),
        BooleanType::from_data(vec![true, true, false]),
        Float64Type::from_data(vec![1.1, 2.2, 3.3]),
        DateType::from_data(vec![1_i32, 2_i32, 3_i32]),
    ];

    if is_nullable {
        columns = columns
            .iter()
            .map(|c| NullableColumn::new_column(c.clone(), Bitmap::new_constant(true, c.len())))
            .collect();
    }

    let format = OutputFormatSettings::default();
    let mut collector = BlocksCollector::new();
    collector.append_columns(columns, 3);
    let serializer = collector.into_serializer(format);
    let expect = [
        vec!["1", "a", "1", "1.1", "1970-01-02"],
        vec!["2", "b", "1", "2.2", "1970-01-03"],
        vec!["3", "c", "0", "3.3", "1970-01-04"],
    ]
    .iter()
    .map(|r| r.iter().map(|v| v.to_string()).collect::<Vec<_>>())
    .collect::<Vec<_>>();

    assert_eq!(
        serde_json::to_string(&serializer)?,
        serde_json::to_string(&expect)?
    );
    Ok(())
}

#[test]
fn test_data_block_nullable() -> anyhow::Result<()> {
    test_data_block(true)?;
    Ok(())
}

#[test]
fn test_data_block_not_nullable() -> anyhow::Result<()> {
    test_data_block(false)?;
    Ok(())
}

#[test]
fn test_empty_block() -> anyhow::Result<()> {
    let format = OutputFormatSettings::default();
    let collector = BlocksCollector::new();
    let serializer = collector.into_serializer(format);
    assert!(serializer.is_empty());
    Ok(())
}

#[test]
fn test_driver_mode_data_block() -> anyhow::Result<()> {
    let ts_tz = timestamp_tz::new(1_000_000, 8 * 3600);
    let columns = vec![
        DateType::from_data(vec![1_i32]),
        TimestampType::from_data(vec![1_000_000_i64]),
        TimestampTzType::from_data(vec![ts_tz]),
        BinaryType::from_data(vec![b"x".as_slice()]),
    ];

    let display = OutputFormatSettings {
        binary_format: BinaryDisplayFormat::Base64,
        ..Default::default()
    };

    let mut driver = display.clone();
    driver.http_json_result_mode = HttpHandlerDataFormat::Driver;

    let mut display_collector = BlocksCollector::new();
    display_collector.append_columns(columns.clone(), 1);
    let display_serializer = display_collector.into_serializer(display.clone());

    let mut driver_collector = BlocksCollector::new();
    driver_collector.append_columns(columns, 1);
    let driver_serializer = driver_collector.into_serializer(driver);

    assert_eq!(
        serde_json::to_value(&display_serializer)?,
        serde_json::json!([[
            date_to_string(1, &display.jiff_timezone).to_string(),
            timestamp_to_string(1_000_000, &display.jiff_timezone).to_string(),
            ts_tz.to_string(),
            "eA=="
        ]])
    );
    assert_eq!(
        serde_json::to_value(&driver_serializer)?,
        serde_json::json!([[
            "1",
            "1000000",
            format!("{} {}", ts_tz.timestamp(), ts_tz.hours_offset()),
            "78"
        ]])
    );
    Ok(())
}

#[test]
fn test_nested_string_data_block() -> anyhow::Result<()> {
    let format = OutputFormatSettings::default();

    let array = Column::Array(Box::new(ArrayColumn::new(
        StringType::from_data(vec!["x", "y\"z"]),
        vec![0_u64, 2].into(),
    )));
    let binary_array = Column::Array(Box::new(ArrayColumn::new(
        BinaryType::from_data(vec![b"x".as_slice(), b"y\"z".as_slice()]),
        vec![0_u64, 2].into(),
    )));
    let tuple = Column::Tuple(vec![
        Int32Type::from_data(vec![7]),
        StringType::from_data(vec!["p\"q"]),
    ]);
    let map = Column::Map(Box::new(ArrayColumn::new(
        Column::Tuple(vec![
            StringType::from_data(vec!["k\"1"]),
            StringType::from_data(vec!["v\"2"]),
        ]),
        vec![0_u64, 1].into(),
    )));

    let mut collector = BlocksCollector::new();
    collector.append_columns(vec![array, binary_array, tuple, map], 1);
    let serializer = collector.into_serializer(format);

    assert_eq!(
        serde_json::to_value(&serializer)?,
        serde_json::json!([[
            "[\"x\",\"y\\\"z\"]",
            "[78,79227A]",
            "(7,\"p\\\"q\")",
            "{\"k\\\"1\":\"v\\\"2\"}"
        ]])
    );
    Ok(())
}
