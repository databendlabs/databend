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
use std::io::Read;
use std::io::Seek;

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::*;
use common_arrow::arrow::error::Result;
use common_arrow::arrow::io::parquet::read as p_read;
use common_arrow::arrow::io::parquet::read::statistics::*;
use common_arrow::arrow::io::parquet::write::*;
use common_arrow::arrow::types::days_ms;
use common_arrow::arrow::types::i256;
use common_arrow::arrow::types::NativeType;
use ethnum::AsI256;

mod deserialize;
#[cfg(feature = "io_json_integration")]
mod integration;
mod read;
mod read_indexes;
mod write;
mod write_async;

#[cfg(feature = "io_parquet_sample_test")]
mod sample_tests;

type ArrayStats = (Box<dyn Array>, Statistics);

fn new_struct(
    arrays: Vec<Box<dyn Array>>,
    names: Vec<String>,
    validity: Option<Bitmap>,
) -> StructArray {
    let fields = names
        .into_iter()
        .zip(arrays.iter())
        .map(|(n, a)| Field::new(n, a.data_type().clone(), true))
        .collect();
    StructArray::new(DataType::Struct(fields), arrays, validity)
}

pub fn read_column<R: Read + Seek>(mut reader: R, column: &str) -> Result<ArrayStats> {
    let metadata = p_read::read_metadata(&mut reader)?;
    let schema = p_read::infer_schema(&metadata)?;

    let row_group = &metadata.row_groups[0];

    // verify that we can read indexes
    if p_read::indexes::has_indexes(row_group) {
        let _indexes = p_read::indexes::read_filtered_pages(
            &mut reader,
            row_group,
            &schema.fields,
            |_, _| vec![],
        )?;
    }

    let schema = schema.filter(|_, f| f.name == column);

    let field = &schema.fields[0];

    let statistics = deserialize(field, &metadata.row_groups)?;

    let mut reader = p_read::FileReader::new(reader, metadata.row_groups, schema, None, None, None);

    let array = reader.next().unwrap()?.into_arrays().pop().unwrap();

    Ok((array, statistics))
}

pub fn pyarrow_nested_edge(column: &str) -> Box<dyn Array> {
    match column {
        "simple" => {
            // [[0, 1]]
            let data = [Some(vec![Some(0), Some(1)])];
            let mut a = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        "null" => {
            // [None]
            let data = [None::<Vec<Option<i64>>>];
            let mut a = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        "empty" => {
            // [None]
            let data: [Option<Vec<Option<i64>>>; 0] = [];
            let mut a = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        "struct_list_nullable" => {
            // [
            //      {"f1": ["a", "b", None, "c"]}
            // ]
            let a = ListArray::<i32>::new(
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                vec![0, 4].try_into().unwrap(),
                Utf8Array::<i32>::from([Some("a"), Some("b"), None, Some("c")]).boxed(),
                None,
            );
            StructArray::new(
                DataType::Struct(vec![Field::new("f1", a.data_type().clone(), true)]),
                vec![a.boxed()],
                None,
            )
            .boxed()
        }
        "list_struct_list_nullable" => {
            let values = pyarrow_nested_edge("struct_list_nullable");
            ListArray::<i32>::new(
                DataType::List(Box::new(Field::new(
                    "item",
                    values.data_type().clone(),
                    true,
                ))),
                vec![0, 1].try_into().unwrap(),
                values,
                None,
            )
            .boxed()
        }
        _ => todo!(),
    }
}

pub fn pyarrow_nested_nullable(column: &str) -> Box<dyn Array> {
    let i64_values = &[
        Some(0),
        Some(1),
        Some(2),
        None,
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        Some(9),
        Some(10),
    ];
    let offsets = vec![0, 2, 2, 5, 8, 8, 11, 11, 12].try_into().unwrap();

    let values = match column {
        "list_int64" => {
            // [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
            PrimitiveArray::<i64>::from(i64_values).boxed()
        }
        "list_int64_required" | "list_int64_optional_required" | "list_int64_required_required" => {
            // [[0, 1], None, [2, 0, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]
            PrimitiveArray::<i64>::from(&[
                Some(0),
                Some(1),
                Some(2),
                Some(0),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
                Some(8),
                Some(9),
                Some(10),
            ])
            .boxed()
        }
        "list_int16" => PrimitiveArray::<i16>::from(&[
            Some(0),
            Some(1),
            Some(2),
            None,
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
        ])
        .boxed(),
        "list_bool" => BooleanArray::from(&[
            Some(false),
            Some(true),
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
        ])
        .boxed(),
        // string = [
        // ["Hello", "bbb"],
        // None,
        // ["aa", None, ""],
        // ["bbb", "aa", "ccc"],
        // [],
        // ["abc", "bbb", "bbb"],
        // None,
        // [""],
        // ]
        "list_utf8" => Utf8Array::<i32>::from([
            Some("Hello".to_string()),
            Some("bbb".to_string()),
            Some("aa".to_string()),
            None,
            Some("".to_string()),
            Some("bbb".to_string()),
            Some("aa".to_string()),
            Some("ccc".to_string()),
            Some("abc".to_string()),
            Some("bbb".to_string()),
            Some("bbb".to_string()),
            Some("".to_string()),
        ])
        .boxed(),
        "list_large_binary" => Box::new(BinaryArray::<i64>::from([
            Some(b"Hello".to_vec()),
            Some(b"bbb".to_vec()),
            Some(b"aa".to_vec()),
            None,
            Some(b"".to_vec()),
            Some(b"bbb".to_vec()),
            Some(b"aa".to_vec()),
            Some(b"ccc".to_vec()),
            Some(b"abc".to_vec()),
            Some(b"bbb".to_vec()),
            Some(b"bbb".to_vec()),
            Some(b"".to_vec()),
        ])),
        "list_decimal_9" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(9, 0)))
        }
        "list_decimal_18" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(18, 0)))
        }
        "list_decimal_26" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(26, 0)))
        }
        "list_decimal256_9" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            let array = PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(9, 0));
            Box::new(array)
        }
        "list_decimal256_18" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            let array = PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(18, 0));
            Box::new(array)
        }
        "list_decimal256_26" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            let array = PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(26, 0));
            Box::new(array)
        }
        "list_decimal256_39" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            let array = PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(39, 0));
            Box::new(array)
        }
        "list_decimal256_76" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            let array = PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(76, 0));
            Box::new(array)
        }
        "list_nested_i64"
        | "list_nested_decimal"
        | "list_nested_inner_required_i64"
        | "list_nested_inner_required_required_i64" => Box::new(NullArray::new(DataType::Null, 1)),
        "struct_list_nullable" => pyarrow_nested_nullable("list_utf8"),
        "list_struct_nullable" => {
            let array = Utf8Array::<i32>::from([
                Some("a"),
                Some("b"),
                //
                Some("b"),
                None,
                Some("b"),
                //
                None,
                None,
                None,
                //
                Some("d"),
                Some("d"),
                Some("d"),
                //
                Some("e"),
            ])
            .boxed();
            new_struct(
                vec![array],
                vec!["a".to_string()],
                Some(
                    [
                        true, true, //
                        true, false, true, //
                        true, true, true, //
                        true, true, true, //
                        true,
                    ]
                    .into(),
                ),
            )
            .boxed()
        }
        "list_struct_list_nullable" => {
            // [
            // [{"a": ["a"]}, {"a": ["b"]}],
            // None,
            // [{"a": ["b"]}, None, {"a": ["b"]}],
            // [{"a": None}, {"a": None}, {"a": None}],
            // [],
            // [{"a": ["d"]}, {"a": [None]}, {"a": ["c", "d"]}],
            // None,
            // [{"a": []}],
            // ]
            let array = Utf8Array::<i32>::from([
                Some("a"),
                Some("b"),
                //
                Some("b"),
                Some("b"),
                //
                Some("d"),
                None,
                Some("c"),
                Some("d"),
            ])
            .boxed();

            let array = ListArray::<i32>::new(
                DataType::List(Box::new(Field::new(
                    "item",
                    array.data_type().clone(),
                    true,
                ))),
                vec![0, 1, 2, 3, 3, 4, 4, 4, 4, 5, 6, 8, 8]
                    .try_into()
                    .unwrap(),
                array,
                Some(
                    [
                        true, true, true, false, true, false, false, false, true, true, true, true,
                    ]
                    .into(),
                ),
            )
            .boxed();

            new_struct(
                vec![array],
                vec!["a".to_string()],
                Some(
                    [
                        true, true, //
                        true, false, true, //
                        true, true, true, //
                        true, true, true, //
                        true,
                    ]
                    .into(),
                ),
            )
            .boxed()
        }
        other => unreachable!("{}", other),
    };

    match column {
        "list_int64_required_required" => {
            // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
            let data_type = DataType::List(Box::new(Field::new("item", DataType::Int64, false)));
            ListArray::<i32>::new(data_type, offsets, values, None).boxed()
        }
        "list_int64_optional_required" => {
            // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
            let data_type = DataType::List(Box::new(Field::new("item", DataType::Int64, true)));
            ListArray::<i32>::new(data_type, offsets, values, None).boxed()
        }
        "list_nested_i64" => {
            // [[0, 1]], None, [[2, None], [3]], [[4, 5], [6]], [], [[7], None, [9]], [[], [None], None], [[10]]
            let data = [
                Some(vec![Some(vec![Some(0), Some(1)])]),
                None,
                Some(vec![Some(vec![Some(2), None]), Some(vec![Some(3)])]),
                Some(vec![Some(vec![Some(4), Some(5)]), Some(vec![Some(6)])]),
                Some(vec![]),
                Some(vec![Some(vec![Some(7)]), None, Some(vec![Some(9)])]),
                Some(vec![Some(vec![]), Some(vec![None]), None]),
                Some(vec![Some(vec![Some(10)])]),
            ];
            let mut a =
                MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i64>>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        "list_nested_decimal" => {
            // [
            //     [[Decimal(0), Decimal(1)]],
            //     None,
            //     [[Decimal(2), None], [Decimal(3)]],
            //     [[Decimal(4), Decimal(5)], [Decimal(6)]],
            //     [],
            //     [[Decimal(7)], None, [Decimal(9)]],
            //     [[], [None], None],
            //     [[Decimal(10)]],
            // ]

            let data = [
                Some(vec![Some(vec![Some(0), Some(1)])]),
                None,
                Some(vec![Some(vec![Some(2), None]), Some(vec![Some(3)])]),
                Some(vec![Some(vec![Some(4), Some(5)]), Some(vec![Some(6)])]),
                Some(vec![]),
                Some(vec![Some(vec![Some(7)]), None, Some(vec![Some(9)])]),
                Some(vec![Some(vec![]), Some(vec![None]), None]),
                Some(vec![Some(vec![Some(10)])]),
            ];

            let inner_array = MutablePrimitiveArray::<i128>::from(DataType::Decimal(9, 0));
            let middle_array = MutableListArray::<i32, MutablePrimitiveArray<i128>>::new_from(
                inner_array.clone(),
                ListArray::<i32>::default_datatype(inner_array.data_type().clone()),
                0,
            );
            let mut outer_array = MutableListArray::<
                i32,
                MutableListArray<i32, MutablePrimitiveArray<i128>>,
            >::new_from(
                middle_array.clone(),
                ListArray::<i32>::default_datatype(middle_array.data_type().clone()),
                0,
            );

            outer_array.try_extend(data).unwrap();
            let array: ListArray<i32> = outer_array.into();
            Box::new(array)
        }
        "list_nested_inner_required_i64" => {
            let data = [
                Some(vec![Some(vec![Some(0), Some(1)])]),
                None,
                Some(vec![Some(vec![Some(2), Some(3)]), Some(vec![Some(3)])]),
                Some(vec![Some(vec![Some(4), Some(5)]), Some(vec![Some(6)])]),
                Some(vec![]),
                Some(vec![Some(vec![Some(7)]), None, Some(vec![Some(9)])]),
                None,
                Some(vec![Some(vec![Some(10)])]),
            ];
            let mut a =
                MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i64>>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        "list_nested_inner_required_required_i64" => {
            let data = [
                Some(vec![Some(vec![Some(0), Some(1)])]),
                None,
                Some(vec![Some(vec![Some(2), Some(3)]), Some(vec![Some(3)])]),
                Some(vec![Some(vec![Some(4), Some(5)]), Some(vec![Some(6)])]),
                Some(vec![]),
                Some(vec![
                    Some(vec![Some(7)]),
                    Some(vec![Some(8)]),
                    Some(vec![Some(9)]),
                ]),
                None,
                Some(vec![Some(vec![Some(10)])]),
            ];
            let mut a =
                MutableListArray::<i32, MutableListArray<i32, MutablePrimitiveArray<i64>>>::new();
            a.try_extend(data).unwrap();
            let array: ListArray<i32> = a.into();
            Box::new(array)
        }
        "struct_list_nullable" => new_struct(vec![values], vec!["a".to_string()], None).boxed(),
        _ => {
            let field = match column {
                "list_int64" => Field::new("item", DataType::Int64, true),
                "list_int64_required" => Field::new("item", DataType::Int64, false),
                "list_int16" => Field::new("item", DataType::Int16, true),
                "list_bool" => Field::new("item", DataType::Boolean, true),
                "list_utf8" => Field::new("item", DataType::Utf8, true),
                "list_large_binary" => Field::new("item", DataType::LargeBinary, true),
                "list_decimal_9" => Field::new("item", DataType::Decimal(9, 0), true),
                "list_decimal_18" => Field::new("item", DataType::Decimal(18, 0), true),
                "list_decimal_26" => Field::new("item", DataType::Decimal(26, 0), true),
                "list_decimal256_9" => Field::new("item", DataType::Decimal256(9, 0), true),
                "list_decimal256_18" => Field::new("item", DataType::Decimal256(18, 0), true),
                "list_decimal256_26" => Field::new("item", DataType::Decimal256(26, 0), true),
                "list_decimal256_39" => Field::new("item", DataType::Decimal256(39, 0), true),
                "list_decimal256_76" => Field::new("item", DataType::Decimal256(76, 0), true),
                "list_struct_nullable" => Field::new("item", values.data_type().clone(), true),
                "list_struct_list_nullable" => Field::new("item", values.data_type().clone(), true),
                other => unreachable!("{}", other),
            };

            let validity = Some(Bitmap::from([
                true, false, true, true, true, true, false, true,
            ]));
            // [0, 2, 2, 5, 8, 8, 11, 11, 12]
            // [[a1, a2], None, [a3, a4, a5], [a6, a7, a8], [], [a9, a10, a11], None, [a12]]
            let data_type = DataType::List(Box::new(field));
            ListArray::<i32>::new(data_type, offsets, values, validity).boxed()
        }
    }
}

pub fn pyarrow_nullable(column: &str) -> Box<dyn Array> {
    let i64_values = &[
        Some(-256),
        Some(-1),
        None,
        Some(3),
        None,
        Some(5),
        Some(6),
        Some(7),
        None,
        Some(9),
    ];
    let u32_values = &[
        Some(0),
        Some(1),
        None,
        Some(3),
        None,
        Some(5),
        Some(6),
        Some(7),
        None,
        Some(9),
    ];

    match column {
        "int64" => Box::new(PrimitiveArray::<i64>::from(i64_values)),
        "float64" => Box::new(PrimitiveArray::<f64>::from(&[
            Some(0.0),
            Some(1.0),
            None,
            Some(3.0),
            None,
            Some(5.0),
            Some(6.0),
            Some(7.0),
            None,
            Some(9.0),
        ])),
        "string" => Box::new(Utf8Array::<i32>::from([
            Some("Hello".to_string()),
            None,
            Some("aa".to_string()),
            Some("".to_string()),
            None,
            Some("abc".to_string()),
            None,
            None,
            Some("def".to_string()),
            Some("aaa".to_string()),
        ])),
        "bool" => Box::new(BooleanArray::from([
            Some(true),
            None,
            Some(false),
            Some(false),
            None,
            Some(true),
            None,
            None,
            Some(true),
            Some(true),
        ])),
        "timestamp_ms" => Box::new(
            PrimitiveArray::<i64>::from_iter(u32_values.iter().map(|x| x.map(|x| x as i64)))
                .to(DataType::Timestamp(TimeUnit::Millisecond, None)),
        ),
        "uint32" => Box::new(PrimitiveArray::<u32>::from(u32_values)),
        "int32_dict" => {
            let keys = PrimitiveArray::<i32>::from([Some(0), Some(1), None, Some(1)]);
            let values = Box::new(PrimitiveArray::<i32>::from_slice([10, 200]));
            Box::new(DictionaryArray::try_from_keys(keys, values).unwrap())
        }
        "decimal_9" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(9, 0)))
        }
        "decimal_18" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(18, 0)))
        }
        "decimal_26" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(26, 0)))
        }
        "decimal256_9" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(9, 0)))
        }
        "decimal256_18" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(18, 0)))
        }
        "decimal256_26" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(26, 0)))
        }
        "decimal256_39" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(39, 0)))
        }
        "decimal256_76" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(76, 0)))
        }
        "timestamp_us" => Box::new(
            PrimitiveArray::<i64>::from(i64_values)
                .to(DataType::Timestamp(TimeUnit::Microsecond, None)),
        ),
        "timestamp_s" => Box::new(
            PrimitiveArray::<i64>::from(i64_values).to(DataType::Timestamp(TimeUnit::Second, None)),
        ),
        "timestamp_s_utc" => Box::new(PrimitiveArray::<i64>::from(i64_values).to(
            DataType::Timestamp(TimeUnit::Second, Some("UTC".to_string())),
        )),
        _ => unreachable!(),
    }
}

pub fn pyarrow_nullable_statistics(column: &str) -> Statistics {
    match column {
        "int64" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(Int64Array::from_slice([-256])),
            max_value: Box::new(Int64Array::from_slice([9])),
        },
        "float64" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(Float64Array::from_slice([0.0])),
            max_value: Box::new(Float64Array::from_slice([9.0])),
        },
        "string" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(4)]).boxed(),
            min_value: Box::new(Utf8Array::<i32>::from_slice([""])),
            max_value: Box::new(Utf8Array::<i32>::from_slice(["def"])),
        },
        "bool" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(4)]).boxed(),
            min_value: Box::new(BooleanArray::from_slice([false])),
            max_value: Box::new(BooleanArray::from_slice([true])),
        },
        "timestamp_ms" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(
                Int64Array::from_slice([0]).to(DataType::Timestamp(TimeUnit::Millisecond, None)),
            ),
            max_value: Box::new(
                Int64Array::from_slice([9]).to(DataType::Timestamp(TimeUnit::Millisecond, None)),
            ),
        },
        "uint32" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(UInt32Array::from_slice([0])),
            max_value: Box::new(UInt32Array::from_slice([9])),
        },
        "int32_dict" => {
            let new_dict = |array: Box<dyn Array>| -> Box<dyn Array> {
                Box::new(DictionaryArray::try_from_keys(vec![Some(0)].into(), array).unwrap())
            };

            Statistics {
                distinct_count: UInt64Array::from([None]).boxed(),
                null_count: UInt64Array::from([Some(1)]).boxed(),
                min_value: new_dict(Box::new(Int32Array::from_slice([10]))),
                max_value: new_dict(Box::new(Int32Array::from_slice([200]))),
            }
        }
        "decimal_9" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(Int128Array::from_slice([-256]).to(DataType::Decimal(9, 0))),
            max_value: Box::new(Int128Array::from_slice([9]).to(DataType::Decimal(9, 0))),
        },
        "decimal_18" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(Int128Array::from_slice([-256]).to(DataType::Decimal(18, 0))),
            max_value: Box::new(Int128Array::from_slice([9]).to(DataType::Decimal(18, 0))),
        },
        "decimal_26" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(Int128Array::from_slice([-256]).to(DataType::Decimal(26, 0))),
            max_value: Box::new(Int128Array::from_slice([9]).to(DataType::Decimal(26, 0))),
        },
        "decimal256_9" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(
                Int256Array::from_slice([i256(-(256.as_i256()))]).to(DataType::Decimal256(9, 0)),
            ),
            max_value: Box::new(
                Int256Array::from_slice([i256(9.as_i256())]).to(DataType::Decimal256(9, 0)),
            ),
        },
        "decimal256_18" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(
                Int256Array::from_slice([i256(-(256.as_i256()))]).to(DataType::Decimal256(18, 0)),
            ),
            max_value: Box::new(
                Int256Array::from_slice([i256(9.as_i256())]).to(DataType::Decimal256(18, 0)),
            ),
        },
        "decimal256_26" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(
                Int256Array::from_slice([i256(-(256.as_i256()))]).to(DataType::Decimal256(26, 0)),
            ),
            max_value: Box::new(
                Int256Array::from_slice([i256(9.as_i256())]).to(DataType::Decimal256(26, 0)),
            ),
        },
        "decimal256_39" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(
                Int256Array::from_slice([i256(-(256.as_i256()))]).to(DataType::Decimal256(39, 0)),
            ),
            max_value: Box::new(
                Int256Array::from_slice([i256(9.as_i256())]).to(DataType::Decimal256(39, 0)),
            ),
        },
        "decimal256_76" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(
                Int256Array::from_slice([i256(-(256.as_i256()))]).to(DataType::Decimal256(76, 0)),
            ),
            max_value: Box::new(
                Int256Array::from_slice([i256(9.as_i256())]).to(DataType::Decimal256(76, 0)),
            ),
        },
        "timestamp_us" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(
                Int64Array::from_slice([-256]).to(DataType::Timestamp(TimeUnit::Microsecond, None)),
            ),
            max_value: Box::new(
                Int64Array::from_slice([9]).to(DataType::Timestamp(TimeUnit::Microsecond, None)),
            ),
        },
        "timestamp_s" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(
                Int64Array::from_slice([-256]).to(DataType::Timestamp(TimeUnit::Second, None)),
            ),
            max_value: Box::new(
                Int64Array::from_slice([9]).to(DataType::Timestamp(TimeUnit::Second, None)),
            ),
        },
        "timestamp_s_utc" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(3)]).boxed(),
            min_value: Box::new(Int64Array::from_slice([-256]).to(DataType::Timestamp(
                TimeUnit::Second,
                Some("UTC".to_string()),
            ))),
            max_value: Box::new(Int64Array::from_slice([9]).to(DataType::Timestamp(
                TimeUnit::Second,
                Some("UTC".to_string()),
            ))),
        },
        _ => unreachable!(),
    }
}

// these values match the values in `integration`
pub fn pyarrow_required(column: &str) -> Box<dyn Array> {
    let i64_values = &[
        Some(-256),
        Some(-1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
        Some(7),
        Some(8),
        Some(9),
    ];

    match column {
        "int64" => Box::new(PrimitiveArray::<i64>::from(i64_values)),
        "bool" => Box::new(BooleanArray::from_slice([
            true, true, false, false, false, true, true, true, true, true,
        ])),
        "string" => Box::new(Utf8Array::<i32>::from_slice([
            "Hello", "bbb", "aa", "", "bbb", "abc", "bbb", "bbb", "def", "aaa",
        ])),
        "decimal_9" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(9, 0)))
        }
        "decimal_18" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(18, 0)))
        }
        "decimal_26" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| x as i128))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i128>::from(values).to(DataType::Decimal(26, 0)))
        }
        "decimal256_9" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(9, 0)))
        }
        "decimal256_18" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(18, 0)))
        }
        "decimal256_26" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(26, 0)))
        }
        "decimal256_39" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(39, 0)))
        }
        "decimal256_76" => {
            let values = i64_values
                .iter()
                .map(|x| x.map(|x| i256(x.as_i256())))
                .collect::<Vec<_>>();
            Box::new(PrimitiveArray::<i256>::from(values).to(DataType::Decimal256(76, 0)))
        }
        _ => unreachable!(),
    }
}

pub fn pyarrow_required_statistics(column: &str) -> Statistics {
    let mut s = pyarrow_nullable_statistics(column);
    s.null_count = UInt64Array::from([Some(0)]).boxed();
    s
}

pub fn pyarrow_nested_nullable_statistics(column: &str) -> Statistics {
    let new_list = |array: Box<dyn Array>, nullable: bool| {
        ListArray::<i32>::new(
            DataType::List(Box::new(Field::new(
                "item",
                array.data_type().clone(),
                nullable,
            ))),
            vec![0, array.len() as i32].try_into().unwrap(),
            array,
            None,
        )
    };

    match column {
        "list_int16" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(Box::new(Int16Array::from_slice([0])), true).boxed(),
            max_value: new_list(Box::new(Int16Array::from_slice([10])), true).boxed(),
        },
        "list_bool" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(Box::new(BooleanArray::from_slice([false])), true).boxed(),
            max_value: new_list(Box::new(BooleanArray::from_slice([true])), true).boxed(),
        },
        "list_utf8" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(Box::new(Utf8Array::<i32>::from_slice([""])), true).boxed(),
            max_value: new_list(Box::new(Utf8Array::<i32>::from_slice(["ccc"])), true).boxed(),
        },
        "list_large_binary" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(Box::new(BinaryArray::<i64>::from_slice([b""])), true).boxed(),
            max_value: new_list(Box::new(BinaryArray::<i64>::from_slice([b"ccc"])), true).boxed(),
        },
        "list_decimal_9" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(
                Box::new(Int128Array::from_slice([0]).to(DataType::Decimal(9, 0))),
                true,
            )
            .boxed(),
            max_value: new_list(
                Box::new(Int128Array::from_slice([10]).to(DataType::Decimal(9, 0))),
                true,
            )
            .boxed(),
        },
        "list_decimal_18" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(
                Box::new(Int128Array::from_slice([0]).to(DataType::Decimal(18, 0))),
                true,
            )
            .boxed(),
            max_value: new_list(
                Box::new(Int128Array::from_slice([10]).to(DataType::Decimal(18, 0))),
                true,
            )
            .boxed(),
        },
        "list_decimal_26" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(
                Box::new(Int128Array::from_slice([0]).to(DataType::Decimal(26, 0))),
                true,
            )
            .boxed(),
            max_value: new_list(
                Box::new(Int128Array::from_slice([10]).to(DataType::Decimal(26, 0))),
                true,
            )
            .boxed(),
        },
        "list_decimal256_9" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(0.as_i256())]).to(DataType::Decimal256(9, 0)),
                ),
                true,
            )
            .boxed(),
            max_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(10.as_i256())]).to(DataType::Decimal256(9, 0)),
                ),
                true,
            )
            .boxed(),
        },
        "list_decimal256_18" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(0.as_i256())]).to(DataType::Decimal256(18, 0)),
                ),
                true,
            )
            .boxed(),
            max_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(10.as_i256())]).to(DataType::Decimal256(18, 0)),
                ),
                true,
            )
            .boxed(),
        },
        "list_decimal256_26" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(0.as_i256())]).to(DataType::Decimal256(26, 0)),
                ),
                true,
            )
            .boxed(),
            max_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(10.as_i256())]).to(DataType::Decimal256(26, 0)),
                ),
                true,
            )
            .boxed(),
        },
        "list_decimal256_39" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(0.as_i256())]).to(DataType::Decimal256(39, 0)),
                ),
                true,
            )
            .boxed(),
            max_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(10.as_i256())]).to(DataType::Decimal256(39, 0)),
                ),
                true,
            )
            .boxed(),
        },
        "list_decimal256_76" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(0.as_i256())]).to(DataType::Decimal256(76, 0)),
                ),
                true,
            )
            .boxed(),
            max_value: new_list(
                Box::new(
                    Int256Array::from_slice([i256(10.as_i256())]).to(DataType::Decimal256(76, 0)),
                ),
                true,
            )
            .boxed(),
        },
        "list_int64" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(Box::new(Int64Array::from_slice([0])), true).boxed(),
            max_value: new_list(Box::new(Int64Array::from_slice([10])), true).boxed(),
        },
        "list_int64_required" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed(),
            min_value: new_list(Box::new(Int64Array::from_slice([0])), false).boxed(),
            max_value: new_list(Box::new(Int64Array::from_slice([10])), false).boxed(),
        },
        "list_int64_required_required" | "list_int64_optional_required" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), false).boxed(),
            null_count: new_list(UInt64Array::from([Some(0)]).boxed(), false).boxed(),
            min_value: new_list(Box::new(Int64Array::from_slice([0])), false).boxed(),
            max_value: new_list(Box::new(Int64Array::from_slice([10])), false).boxed(),
        },
        "list_nested_i64" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed(), true).boxed(),
            null_count: new_list(UInt64Array::from([Some(2)]).boxed(), true).boxed(),
            min_value: new_list(
                new_list(Box::new(Int64Array::from_slice([0])), true).boxed(),
                true,
            )
            .boxed(),
            max_value: new_list(
                new_list(Box::new(Int64Array::from_slice([10])), true).boxed(),
                true,
            )
            .boxed(),
        },
        "list_nested_decimal" => Statistics {
            distinct_count: new_list(
                new_list(UInt64Array::from([None]).boxed(), true).boxed(),
                true,
            )
            .boxed(),
            null_count: new_list(
                new_list(Box::new(UInt64Array::from_slice([7])), true).boxed(),
                true,
            )
            .boxed(),
            min_value: new_list(
                new_list(
                    Box::new(Int128Array::from_slice([0]).to(DataType::Decimal(9, 0))),
                    true,
                )
                .boxed(),
                true,
            )
            .boxed(),
            max_value: new_list(
                new_list(
                    Box::new(Int128Array::from_slice([10]).to(DataType::Decimal(9, 0))),
                    true,
                )
                .boxed(),
                true,
            )
            .boxed(),
        },
        "list_nested_inner_required_required_i64" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(0)]).boxed(),
            min_value: new_list(
                new_list(Box::new(Int64Array::from_slice([0])), true).boxed(),
                true,
            )
            .boxed(),
            max_value: new_list(
                new_list(Box::new(Int64Array::from_slice([10])), true).boxed(),
                true,
            )
            .boxed(),
        },
        "list_nested_inner_required_i64" => Statistics {
            distinct_count: UInt64Array::from([None]).boxed(),
            null_count: UInt64Array::from([Some(0)]).boxed(),
            min_value: new_list(
                new_list(Box::new(Int64Array::from_slice([0])), true).boxed(),
                true,
            )
            .boxed(),
            max_value: new_list(
                new_list(Box::new(Int64Array::from_slice([10])), true).boxed(),
                true,
            )
            .boxed(),
        },
        "list_struct_nullable" => Statistics {
            distinct_count: new_list(
                new_struct(
                    vec![UInt64Array::from([None]).boxed()],
                    vec!["a".to_string()],
                    None,
                )
                .boxed(),
                true,
            )
            .boxed(),
            null_count: new_list(
                new_struct(
                    vec![UInt64Array::from([Some(4)]).boxed()],
                    vec!["a".to_string()],
                    None,
                )
                .boxed(),
                true,
            )
            .boxed(),
            min_value: new_list(
                new_struct(
                    vec![Utf8Array::<i32>::from_slice(["a"]).boxed()],
                    vec!["a".to_string()],
                    None,
                )
                .boxed(),
                true,
            )
            .boxed(),
            max_value: new_list(
                new_struct(
                    vec![Utf8Array::<i32>::from_slice(["e"]).boxed()],
                    vec!["a".to_string()],
                    None,
                )
                .boxed(),
                true,
            )
            .boxed(),
        },
        "list_struct_list_nullable" => Statistics {
            distinct_count: new_list(
                new_struct(
                    vec![new_list(UInt64Array::from([None]).boxed(), true).boxed()],
                    vec!["a".to_string()],
                    None,
                )
                .boxed(),
                true,
            )
            .boxed(),
            null_count: new_list(
                new_struct(
                    vec![new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed()],
                    vec!["a".to_string()],
                    None,
                )
                .boxed(),
                true,
            )
            .boxed(),
            min_value: new_list(
                new_struct(
                    vec![new_list(Utf8Array::<i32>::from_slice(["a"]).boxed(), true).boxed()],
                    vec!["a".to_string()],
                    None,
                )
                .boxed(),
                true,
            )
            .boxed(),
            max_value: new_list(
                new_struct(
                    vec![new_list(Utf8Array::<i32>::from_slice(["d"]).boxed(), true).boxed()],
                    vec!["a".to_string()],
                    None,
                )
                .boxed(),
                true,
            )
            .boxed(),
        },
        "struct_list_nullable" => Statistics {
            distinct_count: new_struct(
                vec![new_list(UInt64Array::from([None]).boxed(), true).boxed()],
                vec!["a".to_string()],
                None,
            )
            .boxed(),
            null_count: new_struct(
                vec![new_list(UInt64Array::from([Some(1)]).boxed(), true).boxed()],
                vec!["a".to_string()],
                None,
            )
            .boxed(),
            min_value: new_struct(
                vec![new_list(Utf8Array::<i32>::from_slice([""]).boxed(), true).boxed()],
                vec!["a".to_string()],
                None,
            )
            .boxed(),
            max_value: new_struct(
                vec![new_list(Utf8Array::<i32>::from_slice(["ccc"]).boxed(), true).boxed()],
                vec!["a".to_string()],
                None,
            )
            .boxed(),
        },
        other => todo!("{}", other),
    }
}

pub fn pyarrow_nested_edge_statistics(column: &str) -> Statistics {
    let new_list = |array: Box<dyn Array>| {
        ListArray::<i32>::new(
            DataType::List(Box::new(Field::new(
                "item",
                array.data_type().clone(),
                true,
            ))),
            vec![0, array.len() as i32].try_into().unwrap(),
            array,
            None,
        )
    };

    let new_struct = |arrays: Vec<Box<dyn Array>>, names: Vec<String>| {
        let fields = names
            .into_iter()
            .zip(arrays.iter())
            .map(|(n, a)| Field::new(n, a.data_type().clone(), true))
            .collect();
        StructArray::new(DataType::Struct(fields), arrays, None)
    };

    let names = vec!["f1".to_string()];

    match column {
        "simple" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed()).boxed(),
            null_count: new_list(UInt64Array::from([Some(0)]).boxed()).boxed(),
            min_value: new_list(Box::new(Int64Array::from([Some(0)]))).boxed(),
            max_value: new_list(Box::new(Int64Array::from([Some(1)]))).boxed(),
        },
        "null" | "empty" => Statistics {
            distinct_count: new_list(UInt64Array::from([None]).boxed()).boxed(),
            null_count: new_list(UInt64Array::from([Some(0)]).boxed()).boxed(),
            min_value: new_list(Box::new(Int64Array::from([None]))).boxed(),
            max_value: new_list(Box::new(Int64Array::from([None]))).boxed(),
        },
        "struct_list_nullable" => Statistics {
            distinct_count: new_struct(
                vec![new_list(Box::new(UInt64Array::from([None]))).boxed()],
                names.clone(),
            )
            .boxed(),
            null_count: new_struct(
                vec![new_list(Box::new(UInt64Array::from([Some(1)]))).boxed()],
                names.clone(),
            )
            .boxed(),
            min_value: Box::new(new_struct(
                vec![new_list(Box::new(Utf8Array::<i32>::from_slice(["a"]))).boxed()],
                names.clone(),
            )),
            max_value: Box::new(new_struct(
                vec![new_list(Box::new(Utf8Array::<i32>::from_slice(["c"]))).boxed()],
                names,
            )),
        },
        "list_struct_list_nullable" => Statistics {
            distinct_count: new_list(
                new_struct(
                    vec![new_list(Box::new(UInt64Array::from([None]))).boxed()],
                    names.clone(),
                )
                .boxed(),
            )
            .boxed(),
            null_count: new_list(
                new_struct(
                    vec![new_list(Box::new(UInt64Array::from([Some(1)]))).boxed()],
                    names.clone(),
                )
                .boxed(),
            )
            .boxed(),
            min_value: new_list(Box::new(new_struct(
                vec![new_list(Box::new(Utf8Array::<i32>::from_slice(["a"]))).boxed()],
                names.clone(),
            )))
            .boxed(),
            max_value: new_list(Box::new(new_struct(
                vec![new_list(Box::new(Utf8Array::<i32>::from_slice(["c"]))).boxed()],
                names,
            )))
            .boxed(),
        },
        _ => unreachable!(),
    }
}

pub fn pyarrow_struct(column: &str) -> Box<dyn Array> {
    let boolean = [
        Some(true),
        None,
        Some(false),
        Some(false),
        None,
        Some(true),
        None,
        None,
        Some(true),
        Some(true),
    ];
    let boolean = BooleanArray::from(boolean).boxed();

    let string = [
        Some("Hello"),
        None,
        Some("aa"),
        Some(""),
        None,
        Some("abc"),
        None,
        None,
        Some("def"),
        Some("aaa"),
    ];
    let string = Utf8Array::<i32>::from(string).boxed();

    let mask = [true, true, false, true, true, true, true, true, true, true];

    let fields = vec![
        Field::new("f1", DataType::Utf8, true),
        Field::new("f2", DataType::Boolean, true),
    ];
    match column {
        "struct" => StructArray::new(DataType::Struct(fields), vec![string, boolean], None).boxed(),
        "struct_nullable" => {
            let values = vec![string, boolean];
            StructArray::new(DataType::Struct(fields), values, Some(mask.into())).boxed()
        }
        "struct_struct" => {
            let struct_ = pyarrow_struct("struct");
            Box::new(StructArray::new(
                DataType::Struct(vec![
                    Field::new("f1", DataType::Struct(fields), true),
                    Field::new("f2", DataType::Boolean, true),
                ]),
                vec![struct_, boolean],
                None,
            ))
        }
        "struct_struct_nullable" => {
            let struct_ = pyarrow_struct("struct");
            Box::new(StructArray::new(
                DataType::Struct(vec![
                    Field::new("f1", DataType::Struct(fields), true),
                    Field::new("f2", DataType::Boolean, true),
                ]),
                vec![struct_, boolean],
                Some(mask.into()),
            ))
        }
        _ => todo!(),
    }
}

pub fn pyarrow_struct_statistics(column: &str) -> Statistics {
    let new_struct =
        |arrays: Vec<Box<dyn Array>>, names: Vec<String>| new_struct(arrays, names, None);

    let names = vec!["f1".to_string(), "f2".to_string()];

    match column {
        "struct" | "struct_nullable" => Statistics {
            distinct_count: new_struct(
                vec![
                    Box::new(UInt64Array::from([None])),
                    Box::new(UInt64Array::from([None])),
                ],
                names.clone(),
            )
            .boxed(),
            null_count: new_struct(
                vec![
                    Box::new(UInt64Array::from([Some(4)])),
                    Box::new(UInt64Array::from([Some(4)])),
                ],
                names.clone(),
            )
            .boxed(),
            min_value: Box::new(new_struct(
                vec![
                    Box::new(Utf8Array::<i32>::from_slice([""])),
                    Box::new(BooleanArray::from_slice([false])),
                ],
                names.clone(),
            )),
            max_value: Box::new(new_struct(
                vec![
                    Box::new(Utf8Array::<i32>::from_slice(["def"])),
                    Box::new(BooleanArray::from_slice([true])),
                ],
                names,
            )),
        },
        "struct_struct" => Statistics {
            distinct_count: new_struct(
                vec![
                    new_struct(
                        vec![
                            Box::new(UInt64Array::from([None])),
                            Box::new(UInt64Array::from([None])),
                        ],
                        names.clone(),
                    )
                    .boxed(),
                    UInt64Array::from([None]).boxed(),
                ],
                names.clone(),
            )
            .boxed(),
            null_count: new_struct(
                vec![
                    new_struct(
                        vec![
                            Box::new(UInt64Array::from([Some(4)])),
                            Box::new(UInt64Array::from([Some(4)])),
                        ],
                        names.clone(),
                    )
                    .boxed(),
                    UInt64Array::from([Some(4)]).boxed(),
                ],
                names.clone(),
            )
            .boxed(),
            min_value: new_struct(
                vec![
                    new_struct(
                        vec![
                            Utf8Array::<i32>::from_slice([""]).boxed(),
                            BooleanArray::from_slice([false]).boxed(),
                        ],
                        names.clone(),
                    )
                    .boxed(),
                    BooleanArray::from_slice([false]).boxed(),
                ],
                names.clone(),
            )
            .boxed(),
            max_value: new_struct(
                vec![
                    new_struct(
                        vec![
                            Utf8Array::<i32>::from_slice(["def"]).boxed(),
                            BooleanArray::from_slice([true]).boxed(),
                        ],
                        names.clone(),
                    )
                    .boxed(),
                    BooleanArray::from_slice([true]).boxed(),
                ],
                names,
            )
            .boxed(),
        },
        "struct_struct_nullable" => Statistics {
            distinct_count: new_struct(
                vec![
                    new_struct(
                        vec![
                            Box::new(UInt64Array::from([None])),
                            Box::new(UInt64Array::from([None])),
                        ],
                        names.clone(),
                    )
                    .boxed(),
                    UInt64Array::from([None]).boxed(),
                ],
                names.clone(),
            )
            .boxed(),
            null_count: new_struct(
                vec![
                    new_struct(
                        vec![
                            Box::new(UInt64Array::from([Some(5)])),
                            Box::new(UInt64Array::from([Some(5)])),
                        ],
                        names.clone(),
                    )
                    .boxed(),
                    UInt64Array::from([Some(5)]).boxed(),
                ],
                names.clone(),
            )
            .boxed(),
            min_value: new_struct(
                vec![
                    new_struct(
                        vec![
                            Utf8Array::<i32>::from_slice([""]).boxed(),
                            BooleanArray::from_slice([false]).boxed(),
                        ],
                        names.clone(),
                    )
                    .boxed(),
                    BooleanArray::from_slice([false]).boxed(),
                ],
                names.clone(),
            )
            .boxed(),
            max_value: new_struct(
                vec![
                    new_struct(
                        vec![
                            Utf8Array::<i32>::from_slice(["def"]).boxed(),
                            BooleanArray::from_slice([true]).boxed(),
                        ],
                        names.clone(),
                    )
                    .boxed(),
                    BooleanArray::from_slice([true]).boxed(),
                ],
                names,
            )
            .boxed(),
        },
        _ => todo!(),
    }
}

pub fn pyarrow_map(column: &str) -> Box<dyn Array> {
    match column {
        "map" => {
            let s1 = [Some("a1"), Some("a2")];
            let s2 = [Some("b1"), Some("b2")];
            let dt = DataType::Struct(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ]);
            MapArray::try_new(
                DataType::Map(Box::new(Field::new("entries", dt.clone(), false)), false),
                vec![0, 2].try_into().unwrap(),
                StructArray::try_new(
                    dt,
                    vec![
                        Utf8Array::<i32>::from(s1).boxed(),
                        Utf8Array::<i32>::from(s2).boxed(),
                    ],
                    None,
                )
                .unwrap()
                .boxed(),
                None,
            )
            .unwrap()
            .boxed()
        }
        "map_nullable" => {
            let s1 = [Some("a1"), Some("a2")];
            let s2 = [Some("b1"), None];
            let dt = DataType::Struct(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ]);
            MapArray::try_new(
                DataType::Map(Box::new(Field::new("entries", dt.clone(), false)), false),
                vec![0, 2].try_into().unwrap(),
                StructArray::try_new(
                    dt,
                    vec![
                        Utf8Array::<i32>::from(s1).boxed(),
                        Utf8Array::<i32>::from(s2).boxed(),
                    ],
                    None,
                )
                .unwrap()
                .boxed(),
                None,
            )
            .unwrap()
            .boxed()
        }
        _ => unreachable!(),
    }
}

pub fn pyarrow_map_statistics(column: &str) -> Statistics {
    let new_map = |arrays: Vec<Box<dyn Array>>, fields: Vec<Field>| {
        let fields = fields
            .into_iter()
            .zip(arrays.iter())
            .map(|(f, a)| Field::new(f.name, a.data_type().clone(), f.is_nullable))
            .collect::<Vec<_>>();
        MapArray::new(
            DataType::Map(
                Box::new(Field::new(
                    "entries",
                    DataType::Struct(fields.clone()),
                    false,
                )),
                false,
            ),
            vec![0, arrays[0].len() as i32].try_into().unwrap(),
            StructArray::new(DataType::Struct(fields), arrays, None).boxed(),
            None,
        )
    };

    let fields = vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
    ];

    match column {
        "map" => Statistics {
            distinct_count: new_map(
                vec![
                    UInt64Array::from([None]).boxed(),
                    UInt64Array::from([None]).boxed(),
                ],
                fields.clone(),
            )
            .boxed(),
            null_count: new_map(
                vec![
                    UInt64Array::from([Some(0)]).boxed(),
                    UInt64Array::from([Some(0)]).boxed(),
                ],
                fields.clone(),
            )
            .boxed(),
            min_value: Box::new(new_map(
                vec![
                    Utf8Array::<i32>::from_slice(["a1"]).boxed(),
                    Utf8Array::<i32>::from_slice(["b1"]).boxed(),
                ],
                fields.clone(),
            )),
            max_value: Box::new(new_map(
                vec![
                    Utf8Array::<i32>::from_slice(["a2"]).boxed(),
                    Utf8Array::<i32>::from_slice(["b2"]).boxed(),
                ],
                fields,
            )),
        },
        "map_nullable" => Statistics {
            distinct_count: new_map(
                vec![
                    UInt64Array::from([None]).boxed(),
                    UInt64Array::from([None]).boxed(),
                ],
                fields.clone(),
            )
            .boxed(),
            null_count: new_map(
                vec![
                    UInt64Array::from([Some(0)]).boxed(),
                    UInt64Array::from([Some(1)]).boxed(),
                ],
                fields.clone(),
            )
            .boxed(),
            min_value: Box::new(new_map(
                vec![
                    Utf8Array::<i32>::from_slice(["a1"]).boxed(),
                    Utf8Array::<i32>::from_slice(["b1"]).boxed(),
                ],
                fields.clone(),
            )),
            max_value: Box::new(new_map(
                vec![
                    Utf8Array::<i32>::from_slice(["a2"]).boxed(),
                    Utf8Array::<i32>::from_slice(["b1"]).boxed(),
                ],
                fields,
            )),
        },
        _ => unreachable!(),
    }
}

fn integration_write(schema: &Schema, chunks: &[Chunk<Box<dyn Array>>]) -> Result<Vec<u8>> {
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V1,
        data_pagesize_limit: None,
    };

    let encodings = schema
        .fields
        .iter()
        .map(|f| {
            transverse(&f.data_type, |x| {
                if let DataType::Dictionary(..) = x {
                    Encoding::RleDictionary
                } else {
                    Encoding::Plain
                }
            })
        })
        .collect();

    let row_groups =
        RowGroupIterator::try_new(chunks.iter().cloned().map(Ok), schema, options, encodings)?;

    let writer = Cursor::new(vec![]);

    let mut writer = FileWriter::try_new(writer, schema.clone(), options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    writer.end(None)?;

    Ok(writer.into_inner().into_inner())
}

type IntegrationRead = (Schema, Vec<Chunk<Box<dyn Array>>>);

fn integration_read(data: &[u8], limit: Option<usize>) -> Result<IntegrationRead> {
    let mut reader = Cursor::new(data);
    let metadata = p_read::read_metadata(&mut reader)?;
    let schema = p_read::infer_schema(&metadata)?;

    for field in &schema.fields {
        let mut _statistics = deserialize(field, &metadata.row_groups)?;
    }

    let reader = p_read::FileReader::new(
        Cursor::new(data),
        metadata.row_groups,
        schema.clone(),
        None,
        limit,
        None,
    );

    let batches = reader.collect::<Result<Vec<_>>>()?;

    Ok((schema, batches))
}

fn generic_data() -> Result<(Schema, Chunk<Box<dyn Array>>)> {
    let array1 = PrimitiveArray::<i64>::from([Some(1), None, Some(2)])
        .to(DataType::Duration(TimeUnit::Second));
    let array2 = Utf8Array::<i64>::from([Some("a"), None, Some("bb")]);

    let indices = PrimitiveArray::from_values((0..3u64).map(|x| x % 2));
    let values = PrimitiveArray::from_slice([1.0f32, 3.0]).boxed();
    let array3 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = BinaryArray::<i32>::from_slice([b"ab", b"ac"]).boxed();
    let array4 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = FixedSizeBinaryArray::new(
        DataType::FixedSizeBinary(2),
        vec![b'a', b'b', b'a', b'c'].into(),
        None,
    )
    .boxed();
    let array5 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1i16, 3]).boxed();
    let array6 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1i64, 3])
        .to(DataType::Timestamp(
            TimeUnit::Millisecond,
            Some("UTC".to_string()),
        ))
        .boxed();
    let array7 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1.0f64, 3.0]).boxed();
    let array8 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1u8, 3]).boxed();
    let array9 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1u16, 3]).boxed();
    let array10 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1u32, 3]).boxed();
    let array11 = DictionaryArray::try_from_keys(indices.clone(), values).unwrap();

    let values = PrimitiveArray::from_slice([1u64, 3]).boxed();
    let array12 = DictionaryArray::try_from_keys(indices, values).unwrap();

    let array13 = PrimitiveArray::<i32>::from_slice([1, 2, 3])
        .to(DataType::Interval(IntervalUnit::YearMonth));

    let array14 =
        PrimitiveArray::<days_ms>::from_slice([days_ms(1, 1), days_ms(2, 2), days_ms(3, 3)])
            .to(DataType::Interval(IntervalUnit::DayTime));

    let schema = Schema::from(vec![
        Field::new("a1", array1.data_type().clone(), true),
        Field::new("a2", array2.data_type().clone(), true),
        Field::new("a3", array3.data_type().clone(), true),
        Field::new("a4", array4.data_type().clone(), true),
        Field::new("a5", array5.data_type().clone(), true),
        Field::new("a5a", array5.data_type().clone(), false),
        Field::new("a6", array6.data_type().clone(), true),
        Field::new("a7", array7.data_type().clone(), true),
        Field::new("a8", array8.data_type().clone(), true),
        Field::new("a9", array9.data_type().clone(), true),
        Field::new("a10", array10.data_type().clone(), true),
        Field::new("a11", array11.data_type().clone(), true),
        Field::new("a12", array12.data_type().clone(), true),
        Field::new("a13", array13.data_type().clone(), true),
        Field::new("a14", array14.data_type().clone(), true),
    ]);
    let chunk = Chunk::try_new(vec![
        array1.boxed(),
        array2.boxed(),
        array3.boxed(),
        array4.boxed(),
        array5.clone().boxed(),
        array5.boxed(),
        array6.boxed(),
        array7.boxed(),
        array8.boxed(),
        array9.boxed(),
        array10.boxed(),
        array11.boxed(),
        array12.boxed(),
        array13.boxed(),
        array14.boxed(),
    ])?;

    Ok((schema, chunk))
}

fn assert_roundtrip(
    schema: Schema,
    chunk: Chunk<Box<dyn Array>>,
    limit: Option<usize>,
) -> Result<()> {
    let r = integration_write(&schema, &[chunk.clone()])?;

    let (new_schema, new_chunks) = integration_read(&r, limit)?;

    let expected = if let Some(limit) = limit {
        let expected = chunk
            .into_arrays()
            .into_iter()
            .map(|x| x.sliced(0, limit))
            .collect::<Vec<_>>();
        Chunk::new(expected)
    } else {
        chunk
    };

    assert_eq!(new_schema, schema);
    assert_eq!(new_chunks, vec![expected]);
    Ok(())
}

/// Tests that when arrow-specific types (Duration and LargeUtf8) are written to parquet, we can rountrip its
/// logical types.
#[test]
fn arrow_type() -> Result<()> {
    let (schema, chunk) = generic_data()?;
    assert_roundtrip(schema, chunk, None)
}

fn data<T: NativeType, I: Iterator<Item = T>>(
    mut iter: I,
    inner_is_nullable: bool,
) -> Box<dyn Array> {
    // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
    let data = vec![
        Some(vec![Some(iter.next().unwrap()), Some(iter.next().unwrap())]),
        Some(vec![]),
        Some(vec![
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
        ]),
        Some(vec![
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
        ]),
        Some(vec![]),
        Some(vec![
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
            Some(iter.next().unwrap()),
        ]),
        Some(vec![]),
        Some(vec![Some(iter.next().unwrap())]),
    ];
    let mut array = MutableListArray::<i32, _>::new_with_field(
        MutablePrimitiveArray::<T>::new(),
        "item",
        inner_is_nullable,
    );
    array.try_extend(data).unwrap();
    array.into_box()
}

fn assert_array_roundtrip(
    is_nullable: bool,
    array: Box<dyn Array>,
    limit: Option<usize>,
) -> Result<()> {
    let schema = Schema::from(vec![Field::new(
        "a1",
        array.data_type().clone(),
        is_nullable,
    )]);
    let chunk = Chunk::try_new(vec![array])?;

    assert_roundtrip(schema, chunk, limit)
}

fn test_list_array_required_required(limit: Option<usize>) -> Result<()> {
    assert_array_roundtrip(false, data(0..12i8, false), limit)?;
    assert_array_roundtrip(false, data(0..12i16, false), limit)?;
    assert_array_roundtrip(false, data(0..12i32, false), limit)?;
    assert_array_roundtrip(false, data(0..12i64, false), limit)?;
    assert_array_roundtrip(false, data(0..12u8, false), limit)?;
    assert_array_roundtrip(false, data(0..12u16, false), limit)?;
    assert_array_roundtrip(false, data(0..12u32, false), limit)?;
    assert_array_roundtrip(false, data(0..12u64, false), limit)?;
    assert_array_roundtrip(false, data((0..12).map(|x| (x as f32) * 1.0), false), limit)?;
    assert_array_roundtrip(
        false,
        data((0..12).map(|x| (x as f64) * 1.0f64), false),
        limit,
    )
}

#[test]
fn list_array_required_required() -> Result<()> {
    test_list_array_required_required(None)
}

#[test]
fn list_array_optional_optional() -> Result<()> {
    assert_array_roundtrip(true, data(0..12, true), None)
}

#[test]
fn list_array_required_optional() -> Result<()> {
    assert_array_roundtrip(true, data(0..12, false), None)
}

#[test]
fn list_array_optional_required() -> Result<()> {
    assert_array_roundtrip(false, data(0..12, true), None)
}

#[test]
fn list_utf8() -> Result<()> {
    let data = vec![
        Some(vec![Some("a".to_string())]),
        Some(vec![]),
        Some(vec![Some("b".to_string())]),
    ];
    let mut array =
        MutableListArray::<i32, _>::new_with_field(MutableUtf8Array::<i32>::new(), "item", true);
    array.try_extend(data).unwrap();
    assert_array_roundtrip(false, array.into_box(), None)
}

#[test]
fn list_large_utf8() -> Result<()> {
    let data = vec![
        Some(vec![Some("a".to_string())]),
        Some(vec![]),
        Some(vec![Some("b".to_string())]),
    ];
    let mut array =
        MutableListArray::<i32, _>::new_with_field(MutableUtf8Array::<i64>::new(), "item", true);
    array.try_extend(data).unwrap();
    assert_array_roundtrip(false, array.into_box(), None)
}

#[test]
fn list_binary() -> Result<()> {
    let data = vec![
        Some(vec![Some(b"a".to_vec())]),
        Some(vec![]),
        Some(vec![Some(b"b".to_vec())]),
    ];
    let mut array =
        MutableListArray::<i32, _>::new_with_field(MutableBinaryArray::<i32>::new(), "item", true);
    array.try_extend(data).unwrap();
    assert_array_roundtrip(false, array.into_box(), None)
}

#[test]
fn list_slice() -> Result<()> {
    let data = vec![
        Some(vec![None, Some(2)]),
        Some(vec![Some(3), Some(4)]),
        Some(vec![Some(5), Some(6)]),
    ];
    let mut array = MutableListArray::<i32, _>::new_with_field(
        MutablePrimitiveArray::<i32>::new(),
        "item",
        true,
    );
    array.try_extend(data).unwrap();
    let a: ListArray<i32> = array.into();
    let a = a.sliced(2, 1);
    assert_array_roundtrip(false, a.boxed(), None)
}

#[test]
fn struct_slice() -> Result<()> {
    let a = pyarrow_nested_nullable("struct_list_nullable");

    let a = a.sliced(2, 1);
    assert_array_roundtrip(true, a, None)
}

#[test]
fn list_struct_slice() -> Result<()> {
    let a = pyarrow_nested_nullable("list_struct_nullable");

    let a = a.sliced(2, 1);
    assert_array_roundtrip(true, a, None)
}

#[test]
fn large_list_large_binary() -> Result<()> {
    let data = vec![
        Some(vec![Some(b"a".to_vec())]),
        Some(vec![]),
        Some(vec![Some(b"b".to_vec())]),
    ];
    let mut array =
        MutableListArray::<i64, _>::new_with_field(MutableBinaryArray::<i64>::new(), "item", true);
    array.try_extend(data).unwrap();
    assert_array_roundtrip(false, array.into_box(), None)
}

#[test]
fn list_utf8_nullable() -> Result<()> {
    let data = vec![
        Some(vec![Some("a".to_string())]),
        None,
        Some(vec![None, Some("b".to_string())]),
        Some(vec![]),
        Some(vec![Some("c".to_string())]),
        None,
    ];
    let mut array =
        MutableListArray::<i32, _>::new_with_field(MutableUtf8Array::<i32>::new(), "item", true);
    array.try_extend(data).unwrap();
    assert_array_roundtrip(true, array.into_box(), None)
}

#[test]
fn list_int_nullable() -> Result<()> {
    let data = vec![
        Some(vec![Some(1)]),
        None,
        Some(vec![None, Some(2)]),
        Some(vec![]),
        Some(vec![Some(3)]),
        None,
    ];
    let mut array = MutableListArray::<i32, _>::new_with_field(
        MutablePrimitiveArray::<i32>::new(),
        "item",
        true,
    );
    array.try_extend(data).unwrap();
    assert_array_roundtrip(true, array.into_box(), None)
}

#[test]
fn limit() -> Result<()> {
    let (schema, chunk) = generic_data()?;
    assert_roundtrip(schema, chunk, Some(2))
}

#[test]
fn limit_list() -> Result<()> {
    test_list_array_required_required(Some(2))
}

fn nested_dict_data(data_type: DataType) -> Result<(Schema, Chunk<Box<dyn Array>>)> {
    let values = match data_type {
        DataType::Float32 => PrimitiveArray::from_slice([1.0f32, 3.0]).boxed(),
        DataType::Utf8 => Utf8Array::<i32>::from_slice(["a", "b"]).boxed(),
        _ => unreachable!(),
    };

    let indices = PrimitiveArray::from_values((0..3u64).map(|x| x % 2));
    let values = DictionaryArray::try_from_keys(indices, values).unwrap();
    let values = ListArray::try_new(
        DataType::List(Box::new(Field::new(
            "item",
            values.data_type().clone(),
            false,
        ))),
        vec![0i32, 0, 0, 2, 3].try_into().unwrap(),
        values.boxed(),
        Some([true, false, true, true].into()),
    )?;

    let schema = Schema::from(vec![Field::new("c1", values.data_type().clone(), true)]);
    let chunk = Chunk::try_new(vec![values.boxed()])?;

    Ok((schema, chunk))
}

#[test]
fn nested_dict() -> Result<()> {
    let (schema, chunk) = nested_dict_data(DataType::Float32)?;

    assert_roundtrip(schema, chunk, None)
}

#[test]
fn nested_dict_utf8() -> Result<()> {
    let (schema, chunk) = nested_dict_data(DataType::Utf8)?;

    assert_roundtrip(schema, chunk, None)
}

#[test]
fn nested_dict_limit() -> Result<()> {
    let (schema, chunk) = nested_dict_data(DataType::Float32)?;

    assert_roundtrip(schema, chunk, Some(2))
}

#[test]
fn filter_chunk() -> Result<()> {
    let chunk1 = Chunk::new(vec![PrimitiveArray::from_slice([1i16, 3]).boxed()]);
    let chunk2 = Chunk::new(vec![PrimitiveArray::from_slice([2i16, 4]).boxed()]);
    let schema = Schema::from(vec![Field::new("c1", DataType::Int16, true)]);

    let r = integration_write(&schema, &[chunk1.clone(), chunk2.clone()])?;

    let mut reader = Cursor::new(r);

    let metadata = p_read::read_metadata(&mut reader)?;

    let new_schema = p_read::infer_schema(&metadata)?;
    assert_eq!(new_schema, schema);

    // select chunk 1
    let row_groups = metadata
        .row_groups
        .into_iter()
        .enumerate()
        .filter(|(index, _)| *index == 0)
        .map(|(_, row_group)| row_group)
        .collect();

    let reader = p_read::FileReader::new(reader, row_groups, schema, None, None, None);

    let new_chunks = reader.collect::<Result<Vec<_>>>()?;

    assert_eq!(new_chunks, vec![chunk1]);
    Ok(())
}
