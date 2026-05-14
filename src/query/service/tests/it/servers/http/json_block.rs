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

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::DataType as ArrowDataType;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::types::timestamp_tz;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FromData;
use databend_common_expression::converts::arrow::ARROW_EXT_TYPE_GEOGRAPHY;
use databend_common_expression::converts::arrow::ARROW_EXT_TYPE_GEOMETRY;
use databend_common_expression::converts::arrow::ARROW_EXT_TYPE_VARIANT;
use databend_common_expression::converts::arrow::EXTENSION_KEY;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::GeometryType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::TimestampTzType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::date::date_to_string;
use databend_common_expression::types::decimal::Decimal64Type;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::geography::GeographyColumn;
use databend_common_expression::types::geography::GeographyRef;
use databend_common_expression::types::geography::GeographyType;
use databend_common_expression::types::geometry::extract_geo_and_srid;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::number::Float64Type;
use databend_common_expression::types::number::Int32Type;
use databend_common_expression::types::timestamp::timestamp_to_string;
use databend_common_io::GEOGRAPHY_SRID;
use databend_common_io::GeometryDataType;
use databend_common_io::geo_to_ewkb;
use databend_common_io::geo_to_ewkt;
use databend_common_io::geo_to_wkb;
use databend_common_io::geometry::geometry_from_str;
use databend_common_io::prelude::BinaryDisplayFormat;
use databend_common_io::prelude::HttpHandlerDataFormat;
use databend_common_io::prelude::OutputFormatSettings;
use databend_query::servers::http::v1::BlocksCollector;
use jsonb::RawJsonb;
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
    let ts_tz = timestamp_tz::new(1_000_000, 5 * 3600 + 45 * 60);
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
            format!("{} {}", ts_tz.timestamp(), ts_tz.seconds_offset()),
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

fn geography_column(values: &[Vec<u8>]) -> Column {
    let mut builder =
        BinaryColumnBuilder::with_capacity(values.len(), values.iter().map(|v| v.len()).sum());
    for value in values {
        builder.put_slice(value);
        builder.commit_row();
    }
    Column::Geography(GeographyColumn(builder.build()))
}

fn geometry_to_wkb(value: &[u8]) -> Result<Vec<u8>> {
    let (geo, _) =
        extract_geo_and_srid(databend_common_expression::ScalarRef::Geometry(value))?.unwrap();
    geo_to_wkb(geo)
}

fn geography_to_wkb(value: &[u8]) -> Result<Vec<u8>> {
    let (geo, _) = extract_geo_and_srid(databend_common_expression::ScalarRef::Geography(
        GeographyRef(value),
    ))?
    .unwrap();
    geo_to_wkb(geo)
}

fn geometry_to_ewkt(value: &[u8]) -> Result<Vec<u8>> {
    let (geo, srid) =
        extract_geo_and_srid(databend_common_expression::ScalarRef::Geometry(value))?.unwrap();
    Ok(geo_to_ewkt(geo, Some(srid))?.into_bytes())
}

fn geometry_to_ewkb(value: &[u8]) -> Result<Vec<u8>> {
    let (geo, srid) =
        extract_geo_and_srid(databend_common_expression::ScalarRef::Geometry(value))?.unwrap();
    geo_to_ewkb(geo, Some(srid))
}

fn geography_to_ewkt(value: &[u8]) -> Result<Vec<u8>> {
    let (geo, _) = extract_geo_and_srid(databend_common_expression::ScalarRef::Geography(
        GeographyRef(value),
    ))?
    .unwrap();
    Ok(geo_to_ewkt(geo, Some(GEOGRAPHY_SRID))?.into_bytes())
}

fn variant_to_json_string(value: &[u8]) -> Vec<u8> {
    RawJsonb::new(value).to_string().into_bytes()
}

fn read_first_arrow_batch(
    buf: Vec<u8>,
) -> anyhow::Result<(Arc<arrow_schema::Schema>, RecordBatch)> {
    let mut reader = StreamReader::try_new(Cursor::new(buf), None)?;
    let schema = reader.schema();
    let batch = reader
        .next()
        .transpose()?
        .expect("expected one record batch in arrow stream");
    assert!(reader.next().transpose()?.is_none());
    Ok((schema, batch))
}

#[test]
fn test_arrow_ipc_geo_text_payloads() -> anyhow::Result<()> {
    let geom = geometry_from_str("SRID=4326;POINT(1 2)", None)?;
    let geog = GeographyType::point(3.0, 4.0).0;

    let schema = DataSchema::new(vec![
        DataField::new("geom", DataType::Geometry),
        DataField::new("geog", DataType::Geography),
    ]);
    let format = OutputFormatSettings {
        geometry_format: GeometryDataType::EWKT,
        ..Default::default()
    };

    let mut collector = BlocksCollector::new();
    collector.append_columns(
        vec![
            GeometryType::from_data(vec![geom.clone()]),
            geography_column(std::slice::from_ref(&geog)),
        ],
        1,
    );

    let buf = collector
        .into_serializer(format)
        .to_arrow_ipc(&schema, vec![])?;
    let (arrow_schema, batch) = read_first_arrow_batch(buf)?;

    assert_eq!(
        arrow_schema
            .field(0)
            .metadata()
            .get(EXTENSION_KEY)
            .map(String::as_str),
        Some(ARROW_EXT_TYPE_GEOMETRY)
    );
    assert_eq!(
        arrow_schema
            .field(1)
            .metadata()
            .get(EXTENSION_KEY)
            .map(String::as_str),
        Some(ARROW_EXT_TYPE_GEOGRAPHY)
    );

    match Column::from_arrow_rs(batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Geometry(column) => {
            assert_eq!(column.index(0).unwrap(), geometry_to_ewkt(&geom)?)
        }
        other => panic!("expected geometry column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(1).clone(), schema.field(1).data_type())? {
        Column::Geography(column) => {
            assert_eq!(column.index(0).unwrap().0, geography_to_ewkt(&geog)?)
        }
        other => panic!("expected geography column, got {other:?}"),
    }

    Ok(())
}

#[test]
fn test_arrow_ipc_variant_json_string_payloads() -> anyhow::Result<()> {
    let value1 = jsonb::parse_value(r#"{"a":1,"b":[true,null,"x"]}"#.as_bytes())?.to_vec();
    let value2 = jsonb::parse_value(r#""plain string""#.as_bytes())?.to_vec();

    let schema = DataSchema::new(vec![DataField::new("v", DataType::Variant)]);
    let mut collector = BlocksCollector::new();
    collector.append_columns(
        vec![VariantType::from_data(vec![value1.clone(), value2.clone()])],
        2,
    );

    let buf = collector
        .into_serializer(OutputFormatSettings::default())
        .to_arrow_ipc(&schema, vec![])?;
    let (arrow_schema, batch) = read_first_arrow_batch(buf)?;

    assert_eq!(
        arrow_schema
            .field(0)
            .metadata()
            .get(EXTENSION_KEY)
            .map(String::as_str),
        Some(ARROW_EXT_TYPE_VARIANT)
    );
    assert_eq!(arrow_schema.field(0).data_type(), &ArrowDataType::LargeUtf8);

    match Column::from_arrow_rs(batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Variant(column) => {
            assert_eq!(column.index(0).unwrap(), variant_to_json_string(&value1));
            assert_eq!(column.index(1).unwrap(), variant_to_json_string(&value2));
        }
        other => panic!("expected variant column, got {other:?}"),
    }

    Ok(())
}

#[test]
fn test_arrow_ipc_nested_variant_json_string_payloads() -> anyhow::Result<()> {
    let value1 = jsonb::parse_value(r#"{"k":[1,2,3]}"#.as_bytes())?.to_vec();
    let value2 = jsonb::parse_value(r#"{"nested":{"x":"y"}}"#.as_bytes())?.to_vec();
    let value3 = jsonb::parse_value(r#"[1,{"z":false}]"#.as_bytes())?.to_vec();
    let value4 = jsonb::parse_value(r#"{"m":null}"#.as_bytes())?.to_vec();
    let value5 = jsonb::parse_value(r#"123"#.as_bytes())?.to_vec();

    let schema = DataSchema::new(vec![
        DataField::new(
            "array_variant",
            DataType::Array(Box::new(DataType::Variant)),
        ),
        DataField::new(
            "tuple_variant",
            DataType::Tuple(vec![DataType::Variant, DataType::String]),
        ),
        DataField::new(
            "map_variant",
            DataType::Map(Box::new(DataType::Tuple(vec![
                DataType::String,
                DataType::Variant,
            ]))),
        ),
        DataField::new(
            "nullable_variant",
            DataType::Nullable(Box::new(DataType::Variant)),
        ),
    ]);

    let mut collector = BlocksCollector::new();
    collector.append_columns(
        vec![
            Column::Array(Box::new(ArrayColumn::new(
                VariantType::from_data(vec![value1.clone(), value2.clone()]),
                vec![0_u64, 2].into(),
            ))),
            Column::Tuple(vec![
                VariantType::from_data(vec![value3.clone()]),
                StringType::from_data(vec!["s"]),
            ]),
            Column::Map(Box::new(ArrayColumn::new(
                Column::Tuple(vec![
                    StringType::from_data(vec!["k"]),
                    VariantType::from_data(vec![value4.clone()]),
                ]),
                vec![0_u64, 1].into(),
            ))),
            NullableColumn::new_column(
                VariantType::from_data(vec![value5.clone()]),
                Bitmap::new_constant(true, 1),
            ),
        ],
        1,
    );

    let buf = collector
        .into_serializer(OutputFormatSettings::default())
        .to_arrow_ipc(&schema, vec![])?;
    let (arrow_schema, batch) = read_first_arrow_batch(buf)?;

    match arrow_schema.field(0).data_type() {
        ArrowDataType::LargeList(field) => {
            assert_eq!(field.data_type(), &ArrowDataType::LargeUtf8);
        }
        other => panic!("expected large list arrow type, got {other:?}"),
    }
    match arrow_schema.field(1).data_type() {
        ArrowDataType::Struct(fields) => {
            assert_eq!(fields[0].data_type(), &ArrowDataType::LargeUtf8);
        }
        other => panic!("expected struct arrow type, got {other:?}"),
    }
    match arrow_schema.field(2).data_type() {
        ArrowDataType::Map(field, _) => match field.data_type() {
            ArrowDataType::Struct(fields) => {
                assert_eq!(fields[1].data_type(), &ArrowDataType::LargeUtf8);
            }
            other => panic!("expected map entry struct arrow type, got {other:?}"),
        },
        other => panic!("expected map arrow type, got {other:?}"),
    }
    assert_eq!(arrow_schema.field(3).data_type(), &ArrowDataType::LargeUtf8);

    match Column::from_arrow_rs(batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Array(column) => match column.values() {
            Column::Variant(values) => {
                assert_eq!(values.index(0).unwrap(), variant_to_json_string(&value1));
                assert_eq!(values.index(1).unwrap(), variant_to_json_string(&value2));
            }
            other => panic!("expected variant array values, got {other:?}"),
        },
        other => panic!("expected array column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(1).clone(), schema.field(1).data_type())? {
        Column::Tuple(fields) => match &fields[0] {
            Column::Variant(column) => {
                assert_eq!(column.index(0).unwrap(), variant_to_json_string(&value3))
            }
            other => panic!("expected tuple variant field, got {other:?}"),
        },
        other => panic!("expected tuple column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(2).clone(), schema.field(2).data_type())? {
        Column::Map(column) => match column.values() {
            Column::Tuple(fields) => match &fields[1] {
                Column::Variant(column) => {
                    assert_eq!(column.index(0).unwrap(), variant_to_json_string(&value4))
                }
                other => panic!("expected map variant value field, got {other:?}"),
            },
            other => panic!("expected map tuple values, got {other:?}"),
        },
        other => panic!("expected map column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(3).clone(), schema.field(3).data_type())? {
        Column::Nullable(column) => match &column.column {
            Column::Variant(column) => {
                assert_eq!(column.index(0).unwrap(), variant_to_json_string(&value5))
            }
            other => panic!("expected nullable variant inner column, got {other:?}"),
        },
        other => panic!("expected nullable column, got {other:?}"),
    }

    Ok(())
}

#[test]
fn test_arrow_ipc_nullable_variant_distinguishes_sql_null_and_json_null() -> anyhow::Result<()> {
    let json_null = jsonb::parse_value(b"null")?.to_vec();

    let schema = DataSchema::new(vec![DataField::new(
        "nullable_variant",
        DataType::Nullable(Box::new(DataType::Variant)),
    )]);
    let mut collector = BlocksCollector::new();
    collector.append_columns(
        vec![NullableColumn::new_column(
            VariantType::from_data(vec![Vec::new(), json_null.clone()]),
            Bitmap::from([false, true]),
        )],
        2,
    );

    let buf = collector
        .into_serializer(OutputFormatSettings::default())
        .to_arrow_ipc(&schema, vec![])?;
    let (_, batch) = read_first_arrow_batch(buf)?;

    match Column::from_arrow_rs(batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Nullable(column) => match &column.column {
            Column::Variant(values) => {
                assert!(!column.validity.get_bit(0));
                assert!(column.validity.get_bit(1));
                assert!(values.index(0).unwrap().is_empty());
                assert_eq!(values.index(1).unwrap(), b"null");
            }
            other => panic!("expected nullable variant inner column, got {other:?}"),
        },
        other => panic!("expected nullable column, got {other:?}"),
    }

    Ok(())
}

#[test]
fn test_arrow_ipc_nested_nullable_variant_distinguishes_sql_null_and_json_null()
-> anyhow::Result<()> {
    let json_null = jsonb::parse_value(b"null")?.to_vec();

    let schema = DataSchema::new(vec![DataField::new(
        "tuple_nullable_variant",
        DataType::Tuple(vec![
            DataType::Nullable(Box::new(DataType::Variant)),
            DataType::String,
        ]),
    )]);
    let mut collector = BlocksCollector::new();
    collector.append_columns(
        vec![Column::Tuple(vec![
            NullableColumn::new_column(
                VariantType::from_data(vec![Vec::new(), json_null.clone()]),
                Bitmap::from([false, true]),
            ),
            StringType::from_data(vec!["a", "b"]),
        ])],
        2,
    );

    let buf = collector
        .into_serializer(OutputFormatSettings::default())
        .to_arrow_ipc(&schema, vec![])?;
    let (_, batch) = read_first_arrow_batch(buf)?;

    match Column::from_arrow_rs(batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Tuple(fields) => match &fields[0] {
            Column::Nullable(column) => match &column.column {
                Column::Variant(values) => {
                    assert!(!column.validity.get_bit(0));
                    assert!(column.validity.get_bit(1));
                    assert!(values.index(0).unwrap().is_empty());
                    assert_eq!(values.index(1).unwrap(), b"null");
                }
                other => panic!("expected tuple nullable variant inner column, got {other:?}"),
            },
            other => panic!("expected tuple nullable variant field, got {other:?}"),
        },
        other => panic!("expected tuple column, got {other:?}"),
    }

    Ok(())
}

#[test]
fn test_arrow_ipc_variant_jsonb_payloads_for_legacy_bendsql_python() -> anyhow::Result<()> {
    let value1 = jsonb::parse_value(r#"{"a":1,"b":[true,null,"x"]}"#.as_bytes())?.to_vec();
    let value2 = jsonb::parse_value(r#""plain string""#.as_bytes())?.to_vec();

    let schema = DataSchema::new(vec![DataField::new("v", DataType::Variant)]);
    let mut collector = BlocksCollector::new();
    collector.append_columns(
        vec![VariantType::from_data(vec![value1.clone(), value2.clone()])],
        2,
    );

    let buf = collector
        .into_serializer(OutputFormatSettings {
            http_arrow_use_jsonb: true,
            ..Default::default()
        })
        .to_arrow_ipc(&schema, vec![])?;
    let (_, batch) = read_first_arrow_batch(buf)?;

    match Column::from_arrow_rs(batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Variant(column) => {
            assert_eq!(column.index(0).unwrap(), value1);
            assert_eq!(column.index(1).unwrap(), value2);
        }
        other => panic!("expected variant column, got {other:?}"),
    }

    Ok(())
}

#[test]
fn test_arrow_ipc_nested_variant_jsonb_payloads_for_legacy_bendsql_python() -> anyhow::Result<()> {
    let value1 = jsonb::parse_value(r#"{"k":[1,2,3]}"#.as_bytes())?.to_vec();
    let value2 = jsonb::parse_value(r#"{"nested":{"x":"y"}}"#.as_bytes())?.to_vec();
    let value3 = jsonb::parse_value(r#"[1,{"z":false}]"#.as_bytes())?.to_vec();
    let value4 = jsonb::parse_value(r#"{"m":null}"#.as_bytes())?.to_vec();
    let value5 = jsonb::parse_value(r#"123"#.as_bytes())?.to_vec();

    let schema = DataSchema::new(vec![
        DataField::new(
            "array_variant",
            DataType::Array(Box::new(DataType::Variant)),
        ),
        DataField::new(
            "tuple_variant",
            DataType::Tuple(vec![DataType::Variant, DataType::String]),
        ),
        DataField::new(
            "map_variant",
            DataType::Map(Box::new(DataType::Tuple(vec![
                DataType::String,
                DataType::Variant,
            ]))),
        ),
        DataField::new(
            "nullable_variant",
            DataType::Nullable(Box::new(DataType::Variant)),
        ),
    ]);

    let mut collector = BlocksCollector::new();
    collector.append_columns(
        vec![
            Column::Array(Box::new(ArrayColumn::new(
                VariantType::from_data(vec![value1.clone(), value2.clone()]),
                vec![0_u64, 2].into(),
            ))),
            Column::Tuple(vec![
                VariantType::from_data(vec![value3.clone()]),
                StringType::from_data(vec!["s"]),
            ]),
            Column::Map(Box::new(ArrayColumn::new(
                Column::Tuple(vec![
                    StringType::from_data(vec!["k"]),
                    VariantType::from_data(vec![value4.clone()]),
                ]),
                vec![0_u64, 1].into(),
            ))),
            NullableColumn::new_column(
                VariantType::from_data(vec![value5.clone()]),
                Bitmap::new_constant(true, 1),
            ),
        ],
        1,
    );

    let buf = collector
        .into_serializer(OutputFormatSettings {
            http_arrow_use_jsonb: true,
            ..Default::default()
        })
        .to_arrow_ipc(&schema, vec![])?;
    let (_, batch) = read_first_arrow_batch(buf)?;

    match Column::from_arrow_rs(batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Array(column) => match column.values() {
            Column::Variant(values) => {
                assert_eq!(values.index(0).unwrap(), value1);
                assert_eq!(values.index(1).unwrap(), value2);
            }
            other => panic!("expected variant array values, got {other:?}"),
        },
        other => panic!("expected array column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(1).clone(), schema.field(1).data_type())? {
        Column::Tuple(fields) => match &fields[0] {
            Column::Variant(column) => assert_eq!(column.index(0).unwrap(), value3),
            other => panic!("expected tuple variant field, got {other:?}"),
        },
        other => panic!("expected tuple column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(2).clone(), schema.field(2).data_type())? {
        Column::Map(column) => match column.values() {
            Column::Tuple(fields) => match &fields[1] {
                Column::Variant(column) => assert_eq!(column.index(0).unwrap(), value4),
                other => panic!("expected map variant value field, got {other:?}"),
            },
            other => panic!("expected map tuple values, got {other:?}"),
        },
        other => panic!("expected map column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(3).clone(), schema.field(3).data_type())? {
        Column::Nullable(column) => match &column.column {
            Column::Variant(column) => assert_eq!(column.index(0).unwrap(), value5),
            other => panic!("expected nullable variant inner column, got {other:?}"),
        },
        other => panic!("expected nullable column, got {other:?}"),
    }

    Ok(())
}

#[test]
fn test_arrow_ipc_decimal64_feature_toggle() -> anyhow::Result<()> {
    let decimal_size = DecimalSize::new_unchecked(10, 2);
    let schema = DataSchema::new(vec![DataField::new("d", DataType::Decimal(decimal_size))]);

    let mut collector = BlocksCollector::new();
    collector.append_columns(
        vec![Decimal64Type::from_data_with_size(
            vec![123_i64],
            Some(decimal_size),
        )],
        1,
    );

    let decimal64_buf = collector
        .clone()
        .into_serializer(OutputFormatSettings {
            http_arrow_use_decimal64: true,
            ..Default::default()
        })
        .to_arrow_ipc(&schema, vec![])?;
    let (decimal64_schema, _) = read_first_arrow_batch(decimal64_buf)?;
    assert_eq!(
        decimal64_schema.field(0).data_type(),
        &ArrowDataType::Decimal64(decimal_size.precision(), decimal_size.scale() as i8)
    );

    let decimal128_buf = collector
        .into_serializer(OutputFormatSettings {
            http_arrow_use_decimal64: false,
            ..Default::default()
        })
        .to_arrow_ipc(&schema, vec![])?;
    let (decimal128_schema, _) = read_first_arrow_batch(decimal128_buf)?;
    assert_eq!(
        decimal128_schema.field(0).data_type(),
        &ArrowDataType::Decimal128(decimal_size.precision(), decimal_size.scale() as i8)
    );

    Ok(())
}

#[test]
fn test_arrow_ipc_nested_geo_binary_payloads() -> anyhow::Result<()> {
    let geom1 = geometry_from_str("SRID=4326;POINT(1 2)", None)?;
    let geom2 = geometry_from_str("SRID=4326;POINT(3 4)", None)?;
    let geom3 = geometry_from_str("SRID=4326;POINT(5 6)", None)?;
    let geom4 = geometry_from_str("SRID=4326;POINT(7 8)", None)?;
    let geog1 = GeographyType::point(9.0, 10.0).0;
    let geog2 = GeographyType::point(11.0, 12.0).0;

    let schema = DataSchema::new(vec![
        DataField::new("array_geom", DataType::Array(Box::new(DataType::Geometry))),
        DataField::new(
            "tuple_geo",
            DataType::Tuple(vec![DataType::Geometry, DataType::Geography]),
        ),
        DataField::new(
            "map_geog",
            DataType::Map(Box::new(DataType::Tuple(vec![
                DataType::String,
                DataType::Geography,
            ]))),
        ),
        DataField::new(
            "nullable_geom",
            DataType::Nullable(Box::new(DataType::Geometry)),
        ),
    ]);
    let format = OutputFormatSettings {
        geometry_format: GeometryDataType::WKB,
        ..Default::default()
    };

    let mut collector = BlocksCollector::new();
    collector.append_columns(
        vec![
            Column::Array(Box::new(ArrayColumn::new(
                GeometryType::from_data(vec![geom1.clone(), geom2.clone()]),
                vec![0_u64, 2].into(),
            ))),
            Column::Tuple(vec![
                GeometryType::from_data(vec![geom3.clone()]),
                geography_column(std::slice::from_ref(&geog1)),
            ]),
            Column::Map(Box::new(ArrayColumn::new(
                Column::Tuple(vec![
                    StringType::from_data(vec!["k"]),
                    geography_column(std::slice::from_ref(&geog2)),
                ]),
                vec![0_u64, 1].into(),
            ))),
            NullableColumn::new_column(
                GeometryType::from_data(vec![geom4.clone()]),
                Bitmap::new_constant(true, 1),
            ),
        ],
        1,
    );

    let buf = collector
        .into_serializer(format)
        .to_arrow_ipc(&schema, vec![])?;
    let (_, batch) = read_first_arrow_batch(buf)?;

    match Column::from_arrow_rs(batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Array(column) => match column.values() {
            Column::Geometry(values) => {
                assert_eq!(values.index(0).unwrap(), geometry_to_wkb(&geom1)?);
                assert_eq!(values.index(1).unwrap(), geometry_to_wkb(&geom2)?);
            }
            other => panic!("expected geometry array values, got {other:?}"),
        },
        other => panic!("expected array column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(1).clone(), schema.field(1).data_type())? {
        Column::Tuple(fields) => {
            match &fields[0] {
                Column::Geometry(column) => {
                    assert_eq!(column.index(0).unwrap(), geometry_to_wkb(&geom3)?)
                }
                other => panic!("expected tuple geometry field, got {other:?}"),
            }
            match &fields[1] {
                Column::Geography(column) => {
                    assert_eq!(column.index(0).unwrap().0, geography_to_wkb(&geog1)?)
                }
                other => panic!("expected tuple geography field, got {other:?}"),
            }
        }
        other => panic!("expected tuple column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(2).clone(), schema.field(2).data_type())? {
        Column::Map(column) => match column.values() {
            Column::Tuple(fields) => match &fields[1] {
                Column::Geography(column) => {
                    assert_eq!(column.index(0).unwrap().0, geography_to_wkb(&geog2)?)
                }
                other => panic!("expected map geography value field, got {other:?}"),
            },
            other => panic!("expected map tuple values, got {other:?}"),
        },
        other => panic!("expected map column, got {other:?}"),
    }

    match Column::from_arrow_rs(batch.column(3).clone(), schema.field(3).data_type())? {
        Column::Nullable(column) => match &column.column {
            Column::Geometry(column) => {
                assert_eq!(column.index(0).unwrap(), geometry_to_wkb(&geom4)?)
            }
            other => panic!("expected nullable geometry inner column, got {other:?}"),
        },
        other => panic!("expected nullable column, got {other:?}"),
    }

    Ok(())
}

#[test]
fn test_arrow_ipc_geometry_preserves_implicit_srid_zero_for_ewk() -> anyhow::Result<()> {
    let geom = geometry_from_str("POINT(1 2)", None)?;
    let schema = DataSchema::new(vec![DataField::new("geom", DataType::Geometry)]);

    let mut collector = BlocksCollector::new();
    collector.append_columns(vec![GeometryType::from_data(vec![geom.clone()])], 1);

    let ewkt_buf = collector
        .clone()
        .into_serializer(OutputFormatSettings {
            geometry_format: GeometryDataType::EWKT,
            ..Default::default()
        })
        .to_arrow_ipc(&schema, vec![])?;
    let (_, ewkt_batch) = read_first_arrow_batch(ewkt_buf)?;

    match Column::from_arrow_rs(ewkt_batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Geometry(column) => {
            assert_eq!(column.index(0).unwrap(), geometry_to_ewkt(&geom)?);
        }
        other => panic!("expected geometry column, got {other:?}"),
    }

    let ewkb_buf = collector
        .into_serializer(OutputFormatSettings {
            geometry_format: GeometryDataType::EWKB,
            ..Default::default()
        })
        .to_arrow_ipc(&schema, vec![])?;
    let (_, ewkb_batch) = read_first_arrow_batch(ewkb_buf)?;

    match Column::from_arrow_rs(ewkb_batch.column(0).clone(), schema.field(0).data_type())? {
        Column::Geometry(column) => {
            assert_eq!(column.index(0).unwrap(), geometry_to_ewkb(&geom)?);
        }
        other => panic!("expected geometry column, got {other:?}"),
    }

    Ok(())
}
