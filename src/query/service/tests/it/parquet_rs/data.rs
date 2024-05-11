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

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::Date32Array;
use arrow_array::Date64Array;
use arrow_array::Decimal128Array;
use arrow_array::Float64Array;
use arrow_array::Int32Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_array::TimestampMicrosecondArray;
use arrow_array::TimestampMillisecondArray;
use arrow_array::TimestampNanosecondArray;
use arrow_array::TimestampSecondArray;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use chrono::Datelike;
use chrono::Duration;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::NamedTempFile;

// Test cases from apache/arrow-datafusion

/// What data to use
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum Scenario {
    Timestamp,
    Date,
    Int32,
    Float64,
    Decimal,
    DecimalLargePrecision,
    PeriodsInColumnNames,
}

/// Return record batch with a few rows of data for all of the supported timestamp types
/// values with the specified offset
///
/// Columns are named:
/// "nanos" --> TimestampNanosecondArray
/// "micros" --> TimestampMicrosecondArray
/// "millis" --> TimestampMillisecondArray
/// "seconds" --> TimestampSecondArray
/// "names" --> StringArray
fn make_timestamp_batch(offset: Duration) -> RecordBatch {
    let ts_strings = vec![
        Some("2020-01-01T01:01:01.0000000000001"),
        Some("2020-01-01T01:02:01.0000000000001"),
        Some("2020-01-01T02:01:01.0000000000001"),
        None,
        Some("2020-01-02T01:01:01.0000000000001"),
    ];

    let offset_nanos = offset.num_nanoseconds().expect("non overflow nanos");

    let ts_nanos = ts_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                offset_nanos
                    + t.parse::<chrono::NaiveDateTime>()
                        .unwrap()
                        .and_utc()
                        .timestamp_nanos_opt()
                        .unwrap()
            })
        })
        .collect::<Vec<_>>();

    let ts_micros = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000))
        .collect::<Vec<_>>();

    let ts_millis = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000000))
        .collect::<Vec<_>>();

    let ts_seconds = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000000000))
        .collect::<Vec<_>>();

    let names = ts_nanos
        .iter()
        .enumerate()
        .map(|(i, _)| format!("Row {i} + {offset}"))
        .collect::<Vec<_>>();

    let arr_nanos = TimestampNanosecondArray::from(ts_nanos);
    let arr_micros = TimestampMicrosecondArray::from(ts_micros);
    let arr_millis = TimestampMillisecondArray::from(ts_millis);
    let arr_seconds = TimestampSecondArray::from(ts_seconds);

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = StringArray::from(names);

    let schema = Schema::new(vec![
        Field::new("nanos", arr_nanos.data_type().clone(), true),
        Field::new("micros", arr_micros.data_type().clone(), true),
        Field::new("millis", arr_millis.data_type().clone(), true),
        Field::new("seconds", arr_seconds.data_type().clone(), true),
        Field::new("name", arr_names.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(schema, vec![
        Arc::new(arr_nanos),
        Arc::new(arr_micros),
        Arc::new(arr_millis),
        Arc::new(arr_seconds),
        Arc::new(arr_names),
    ])
    .unwrap()
}

/// Return record batch with i32 sequence
///
/// Columns are named
/// "i" -> Int32Array
fn make_int32_batch(start: i32, end: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let v: Vec<i32> = (start..end).collect();
    let array = Arc::new(Int32Array::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with f64 vector
///
/// Columns are named
/// "f" -> Float64Array
fn make_f64_batch(v: Vec<f64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, true)]));
    let array = Arc::new(Float64Array::from(v)) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with decimal vector
///
/// Columns are named
/// "decimal_col" -> DecimalArray
fn make_decimal_batch(v: Vec<i128>, precision: u8, scale: i8) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "decimal_col",
        DataType::Decimal128(precision, scale),
        true,
    )]));
    let array = Arc::new(
        Decimal128Array::from(v)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    ) as ArrayRef;
    RecordBatch::try_new(schema, vec![array.clone()]).unwrap()
}

/// Return record batch with a few rows of data for all of the supported date
/// types with the specified offset (in days)
///
/// Columns are named:
/// "date32" --> Date32Array
/// "date64" --> Date64Array
/// "names" --> StringArray
fn make_date_batch(offset: Duration) -> RecordBatch {
    let date_strings = vec![
        Some("2020-01-01"),
        Some("2020-01-02"),
        Some("2020-01-03"),
        None,
        Some("2020-01-04"),
    ];

    let names = date_strings
        .iter()
        .enumerate()
        .map(|(i, val)| format!("Row {i} + {offset}: {val:?}"))
        .collect::<Vec<_>>();

    // Copied from `cast.rs` cast kernel due to lack of temporal kernels
    // https://github.com/apache/arrow-rs/issues/527
    const EPOCH_DAYS_FROM_CE: i32 = 719_163;

    let date_seconds = date_strings
        .iter()
        .map(|t| {
            t.map(|t| {
                let t = t.parse::<chrono::NaiveDate>().unwrap();
                let t = t + offset;
                t.num_days_from_ce() - EPOCH_DAYS_FROM_CE
            })
        })
        .collect::<Vec<_>>();

    let date_millis = date_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                let t = t
                    .parse::<chrono::NaiveDate>()
                    .unwrap()
                    .and_time(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap());
                let t = t + offset;
                t.and_utc().timestamp_millis()
            })
        })
        .collect::<Vec<_>>();

    let arr_date32 = Date32Array::from(date_seconds);
    let arr_date64 = Date64Array::from(date_millis);

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = StringArray::from(names);

    let schema = Schema::new(vec![
        Field::new("date32", arr_date32.data_type().clone(), true),
        Field::new("date64", arr_date64.data_type().clone(), true),
        Field::new("name", arr_names.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(schema, vec![
        Arc::new(arr_date32),
        Arc::new(arr_date64),
        Arc::new(arr_names),
    ])
    .unwrap()
}

/// returns a batch with two columns (note "service.name" is the name
/// of the column. It is *not* a table named service.name
///
/// name | service.name
fn make_names_batch(name: &str, service_name_values: Vec<&str>) -> RecordBatch {
    let num_rows = service_name_values.len();
    let name: StringArray = std::iter::repeat(Some(name)).take(num_rows).collect();
    let service_name: StringArray = service_name_values.iter().map(Some).collect();

    let schema = Schema::new(vec![
        Field::new("name", name.data_type().clone(), true),
        // note the column name has a period in it!
        Field::new("service.name", service_name.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(schema, vec![Arc::new(name), Arc::new(service_name)]).unwrap()
}

fn create_data_batch(scenario: Scenario) -> Vec<RecordBatch> {
    match scenario {
        Scenario::Timestamp => {
            vec![
                make_timestamp_batch(Duration::seconds(0)),
                make_timestamp_batch(Duration::seconds(10)),
                make_timestamp_batch(Duration::minutes(10)),
                make_timestamp_batch(Duration::days(10)),
            ]
        }
        Scenario::Date => {
            vec![
                make_date_batch(Duration::days(0)),
                make_date_batch(Duration::days(10)),
                make_date_batch(Duration::days(300)),
                make_date_batch(Duration::days(3600)),
            ]
        }
        Scenario::Int32 => {
            vec![
                make_int32_batch(-5, 0),
                make_int32_batch(-4, 1),
                make_int32_batch(0, 5),
                make_int32_batch(5, 10),
            ]
        }
        Scenario::Float64 => {
            vec![
                make_f64_batch(vec![-5.0, -4.0, -3.0, -2.0, -1.0]),
                make_f64_batch(vec![-4.0, -3.0, -2.0, -1.0, 0.0]),
                make_f64_batch(vec![0.0, 1.0, 2.0, 3.0, 4.0]),
                make_f64_batch(vec![5.0, 6.0, 7.0, 8.0, 9.0]),
            ]
        }
        Scenario::Decimal => {
            // decimal record batch
            vec![
                make_decimal_batch(vec![100, 200, 300, 400, 600], 9, 2),
                make_decimal_batch(vec![-500, 100, 300, 400, 600], 9, 2),
                make_decimal_batch(vec![2000, 3000, 3000, 4000, 6000], 9, 2),
            ]
        }
        Scenario::DecimalLargePrecision => {
            // decimal record batch with large precision,
            // and the data will stored as FIXED_LENGTH_BYTE_ARRAY
            vec![
                make_decimal_batch(vec![100, 200, 300, 400, 600], 38, 2),
                make_decimal_batch(vec![-500, 100, 300, 400, 600], 38, 2),
                make_decimal_batch(vec![2000, 3000, 3000, 4000, 6000], 38, 2),
            ]
        }
        Scenario::PeriodsInColumnNames => {
            vec![
                // all frontend
                make_names_batch("HTTP GET / DISPATCH", vec![
                    "frontend", "frontend", "frontend", "frontend", "frontend",
                ]),
                // both frontend and backend
                make_names_batch("HTTP PUT / DISPATCH", vec![
                    "frontend", "frontend", "backend", "backend", "backend",
                ]),
                // all backend
                make_names_batch("HTTP GET / DISPATCH", vec![
                    "backend", "backend", "backend", "backend", "backend",
                ]),
            ]
        }
    }
}

/// Create a test parquet file with various data types
pub async fn make_test_file_rg(scenario: Scenario) -> (NamedTempFile, SchemaRef) {
    let mut output_file = tempfile::Builder::new()
        .prefix("parquet_pruning")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    let props = WriterProperties::builder()
        .set_max_row_group_size(5)
        .build();

    let batches = create_data_batch(scenario);

    let schema = batches[0].schema();

    let mut writer = ArrowWriter::try_new(&mut output_file, schema.clone(), Some(props)).unwrap();

    for batch in batches {
        writer.write(&batch).expect("writing batch");
    }

    writer.close().unwrap();

    (output_file, schema)
}

pub async fn make_test_file_page(scenario: Scenario) -> (NamedTempFile, SchemaRef) {
    let mut output_file = tempfile::Builder::new()
        .prefix("parquet_page_pruning")
        .suffix(".parquet")
        .tempfile()
        .expect("tempfile creation");

    // set row count to 5, should get same result as rowGroup
    let props = WriterProperties::builder()
        .set_data_page_row_count_limit(5)
        .set_write_batch_size(5)
        .build();

    let batches = create_data_batch(scenario);

    let schema = batches[0].schema();

    let mut writer = ArrowWriter::try_new(&mut output_file, schema.clone(), Some(props)).unwrap();

    for batch in batches {
        writer.write(&batch).expect("writing batch");
    }
    writer.close().unwrap();
    (output_file, schema)
}
