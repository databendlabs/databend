// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_data_value_arithmetic() {
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
                "Internal Error: Unsupported data_value_min for data type: left:Utf8, right:Int8",
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
                "Internal Error: Unsupported data_value_max for data type: left:Utf8, right:Int8",
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
                "Internal Error: Unsupported data_value_sum for data type: left:Utf8, right:Int8",
                "Internal Error: Unsupported data_value_sum for data type: left:Utf8, right:Utf8",
            ],
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = data_value_aggregate_op(t.op.clone(), args[0].clone(), args[1].clone());
            match result {
                Ok(ref v) => {
                    // Result check.
                    if *v != t.expect[i] {
                        println!("{}, expect:\n{:?} \nactual:\n{:?}", t.name, t.expect[i], v);
                        assert!(false);
                    }
                }
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}
