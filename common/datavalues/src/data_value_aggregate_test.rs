// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_data_value_arithmetic() {
    use pretty_assertions::assert_eq;

    use super::*;

    #[allow(dead_code)]
    struct ScalarTest {
        name: &'static str,
        args: Vec<Vec<DataValue>>,
        expect: Vec<DataValue>,
        error: Vec<&'static str>,
        op: DataValueAggregateOperator,
    }

    let tests = vec![
        ScalarTest {
            name: "min-passed",
            args: vec![
                vec![
                    DataValue::String(Some("xx".to_string())),
                    DataValue::Int8(Some(2)),
                ],
                vec![
                    DataValue::String(Some("x1".to_string())),
                    DataValue::String(Some("x2".to_string())),
                ],
                vec![DataValue::Int8(Some(1)), DataValue::Int8(Some(2))],
                vec![DataValue::Int16(Some(1)), DataValue::Int16(Some(2))],
                vec![DataValue::Int32(Some(1)), DataValue::Int32(Some(2))],
                vec![DataValue::Int64(Some(1)), DataValue::Int64(Some(2))],
                vec![DataValue::UInt8(Some(1)), DataValue::UInt8(Some(2))],
                vec![DataValue::UInt16(Some(1)), DataValue::UInt16(Some(2))],
                vec![DataValue::UInt32(Some(1)), DataValue::UInt32(Some(2))],
                vec![DataValue::UInt64(Some(1)), DataValue::UInt64(Some(2))],
                vec![DataValue::Float32(Some(1.0)), DataValue::Float32(Some(2.0))],
                vec![DataValue::Float64(Some(1.0)), DataValue::Float64(Some(2.0))],
            ],
            op: DataValueAggregateOperator::Min,
            expect: vec![
                DataValue::String(Some("xx".to_string())),
                DataValue::String(Some("x1".to_string())),
                DataValue::Int8(Some(1)),
                DataValue::Int16(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::Int64(Some(1)),
                DataValue::UInt8(Some(1)),
                DataValue::UInt16(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float64(Some(1.0)),
            ],
            error: vec![
                "DataValue Internal Error: Unsupported data_value_min for data type: left:Utf8, right:Int8",
            ],
        },
        ScalarTest {
            name: "max-passed",
            args: vec![
                vec![
                    DataValue::String(Some("xx".to_string())),
                    DataValue::Int8(Some(2)),
                ],
                vec![
                    DataValue::String(Some("x1".to_string())),
                    DataValue::String(Some("x2".to_string())),
                ],
                vec![DataValue::Int8(Some(1)), DataValue::Int8(Some(2))],
                vec![DataValue::Int16(Some(1)), DataValue::Int16(Some(2))],
                vec![DataValue::Int32(Some(1)), DataValue::Int32(Some(2))],
                vec![DataValue::Int64(Some(1)), DataValue::Int64(Some(2))],
                vec![DataValue::UInt8(Some(1)), DataValue::UInt8(Some(2))],
                vec![DataValue::UInt16(Some(1)), DataValue::UInt16(Some(2))],
                vec![DataValue::UInt32(Some(1)), DataValue::UInt32(Some(2))],
                vec![DataValue::UInt64(Some(1)), DataValue::UInt64(Some(2))],
                vec![DataValue::Float32(Some(1.0)), DataValue::Float32(Some(2.0))],
                vec![DataValue::Float64(Some(1.0)), DataValue::Float64(Some(2.0))],
            ],
            op: DataValueAggregateOperator::Max,
            expect: vec![
                DataValue::String(Some("xx".to_string())),
                DataValue::String(Some("x2".to_string())),
                DataValue::Int8(Some(2)),
                DataValue::Int16(Some(2)),
                DataValue::Int32(Some(2)),
                DataValue::Int64(Some(2)),
                DataValue::UInt8(Some(2)),
                DataValue::UInt16(Some(2)),
                DataValue::UInt32(Some(2)),
                DataValue::UInt64(Some(2)),
                DataValue::Float32(Some(2.0)),
                DataValue::Float64(Some(2.0)),
            ],
            error: vec![
                "DataValue Internal Error: Unsupported data_value_max for data type: left:Utf8, right:Int8",
            ],
        },
        ScalarTest {
            name: "sum-passed",
            args: vec![
                vec![
                    DataValue::String(Some("xx".to_string())),
                    DataValue::Int8(Some(2)),
                ],
                vec![
                    DataValue::String(Some("x1".to_string())),
                    DataValue::String(Some("x2".to_string())),
                ],
                vec![DataValue::Int8(Some(1)), DataValue::Int8(Some(2))],
                vec![DataValue::Int16(Some(1)), DataValue::Int16(Some(2))],
                vec![DataValue::Int32(Some(1)), DataValue::Int32(Some(2))],
                vec![DataValue::Int64(Some(1)), DataValue::Int64(Some(2))],
                vec![DataValue::UInt8(Some(1)), DataValue::UInt8(Some(2))],
                vec![DataValue::UInt16(Some(1)), DataValue::UInt16(Some(2))],
                vec![DataValue::UInt32(Some(1)), DataValue::UInt32(Some(2))],
                vec![DataValue::UInt64(Some(1)), DataValue::UInt64(Some(2))],
                vec![DataValue::Float32(Some(1.0)), DataValue::Float32(Some(2.0))],
                vec![DataValue::Float64(Some(1.0)), DataValue::Float64(Some(2.0))],
            ],
            op: DataValueAggregateOperator::Sum,
            expect: vec![
                DataValue::String(Some("xx".to_string())),
                DataValue::String(Some("x2".to_string())),
                DataValue::Int8(Some(3)),
                DataValue::Int16(Some(3)),
                DataValue::Int32(Some(3)),
                DataValue::Int64(Some(3)),
                DataValue::UInt8(Some(3)),
                DataValue::UInt16(Some(3)),
                DataValue::UInt32(Some(3)),
                DataValue::UInt64(Some(3)),
                DataValue::Float32(Some(3.0)),
                DataValue::Float64(Some(3.0)),
            ],
            error: vec![
                "DataValue Internal Error: Unsupported data_value_sum for data type: left:Utf8, right:Int8",
                "DataValue Internal Error: Unsupported data_value_sum for data type: left:Utf8, right:Utf8",
            ],
        },
        ScalarTest {
            name: "avg-failed",
            args: vec![
                vec![
                    DataValue::String(Some("xx".to_string())),
                    DataValue::Int8(Some(2)),
                ],
                vec![
                    DataValue::String(Some("x1".to_string())),
                    DataValue::String(Some("x2".to_string())),
                ],
                vec![DataValue::Int8(Some(1)), DataValue::Int8(Some(2))],
                vec![DataValue::Int16(Some(1)), DataValue::Int16(Some(2))],
                vec![DataValue::Int32(Some(1)), DataValue::Int32(Some(2))],
                vec![DataValue::Int64(Some(1)), DataValue::Int64(Some(2))],
                vec![DataValue::UInt8(Some(1)), DataValue::UInt8(Some(2))],
                vec![DataValue::UInt16(Some(1)), DataValue::UInt16(Some(2))],
                vec![DataValue::UInt32(Some(1)), DataValue::UInt32(Some(2))],
                vec![DataValue::UInt64(Some(1)), DataValue::UInt64(Some(2))],
                vec![DataValue::Float32(Some(1.0)), DataValue::Float32(Some(2.0))],
                vec![DataValue::Float64(Some(1.0)), DataValue::Float64(Some(2.0))],
            ],
            op: DataValueAggregateOperator::Avg,
            expect: vec![
            ],
            error: vec![
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:Utf8, right:Int8",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:Utf8, right:Utf8",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:Int8, right:Int8",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:Int16, right:Int16",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:Int32, right:Int32",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:Int64, right:Int64",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:UInt8, right:UInt8",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:UInt16, right:UInt16",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:UInt32, right:UInt32",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:UInt64, right:UInt64",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:Float32, right:Float32",
                "DataValue Internal Error: Unsupported data_value_avg for data type: left:Float64, right:Float64",
            ],
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = data_value_aggregate_op(t.op.clone(), args[0].clone(), args[1].clone());
            match result {
                Ok(v) => assert_eq!(v, t.expect[i]),
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}
