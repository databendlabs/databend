// Copyright 2021 Datafuse Labs.
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

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::Blake3HashFunction;
use common_functions::scalars::Md5HashFunction;
use common_functions::scalars::Sha1HashFunction;
use common_functions::scalars::Sha2HashFunction;
use common_functions::scalars::SipHashFunction;
use common_functions::scalars::XxHash32Function;
use common_functions::scalars::XxHash64Function;

#[test]
fn test_siphash_function() -> Result<()> {
    struct Test {
        name: &'static str,
        input_column: DataColumn,
        expect_output_column: DataColumn,
        error: &'static str,
    }

    let tests = vec![
        Test {
            name: "Int8Array siphash",
            input_column: Series::new(vec![1i8, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Int16Array siphash",
            input_column: Series::new(vec![1i16, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Int32Array siphash",
            input_column: Series::new(vec![1i32, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Int64Array siphash",
            input_column: Series::new(vec![1i64, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt8Array siphash",
            input_column: Series::new(vec![1u8, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt16Array siphash",
            input_column: Series::new(vec![1u16, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt32Array siphash",
            input_column: Series::new(vec![1u32, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt64Array siphash",
            input_column: Series::new(vec![1u64, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Float32Array siphash",
            input_column: Series::new(vec![1.0f32, 2., 1.]).into(),
            expect_output_column: Series::new(vec![
                729488449357906283u64,
                9872512741335963328,
                729488449357906283,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Float64Array siphash",
            input_column: Series::new(vec![1.0f64, 2., 1.]).into(),
            expect_output_column: Series::new(vec![
                13833534234735907638u64,
                12773237290464453619,
                13833534234735907638,
            ])
            .into(),
            error: "",
        },
    ];

    for test in tests {
        let function = SipHashFunction::try_create("siphash")?;

        let rows = test.input_column.len();

        let columns = vec![DataColumnWithField::new(
            test.input_column.clone(),
            DataField::new("dummpy", test.input_column.data_type(), false),
        )];
        match function.eval(&columns, rows) {
            Ok(result_column) => assert_eq!(
                &result_column.get_array_ref()?,
                &test.expect_output_column.get_array_ref()?,
                "failed in the test: {}",
                test.name
            ),
            Err(error) => assert_eq!(
                test.error,
                error.to_string(),
                "failed in the test: {}",
                test.name
            ),
        };
    }

    Ok(())
}

#[test]
fn test_md5hash_function() -> Result<()> {
    struct Test {
        name: &'static str,
        arg: DataColumnWithField,
        expect: Result<DataColumn>,
    }
    let tests = vec![
        Test {
            name: "valid input",
            arg: DataColumnWithField::new(
                Series::new([Some("testing")]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Constant(
                DataValue::String(Some("ae2b1fca515949e5d54fb22b8ed95575".as_bytes().to_vec())),
                1,
            )),
        },
        Test {
            name: "valid input with null",
            arg: DataColumnWithField::new(
                Series::new([Some("testing"), None]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Array(Series::new(vec![
                Some("ae2b1fca515949e5d54fb22b8ed95575".as_bytes().to_vec()),
                None,
            ]))),
        },
    ];

    let func = Md5HashFunction::try_create("md5")?;
    for t in tests {
        let got = func.return_type(&[t.arg.data_type().clone()]);
        let got = got.and_then(|_| func.eval(&[t.arg], 1));
        match t.expect {
            Ok(expected) => {
                assert_eq!(&got.unwrap(), &expected, "case: {}", t.name);
            }
            Err(expected_err) => {
                assert_eq!(got.unwrap_err().to_string(), expected_err.to_string());
            }
        }
    }
    Ok(())
}

#[test]
fn test_sha1hash_function() -> Result<()> {
    struct Test {
        name: &'static str,
        arg: DataColumnWithField,
        expect: Result<DataColumn>,
    }
    let tests = vec![
        Test {
            name: "valid input",
            arg: DataColumnWithField::new(
                Series::new(["abc"]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Constant(
                DataValue::String(Some(
                    "a9993e364706816aba3e25717850c26c9cd0d89d"
                        .as_bytes()
                        .to_vec(),
                )),
                1,
            )),
        },
        Test {
            name: "valid input with null",
            arg: DataColumnWithField::new(
                Series::new([Some("abc"), None]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Array(Series::new(vec![
                Some(
                    "a9993e364706816aba3e25717850c26c9cd0d89d"
                        .as_bytes()
                        .to_vec(),
                ),
                None,
            ]))),
        },
    ];

    let func = Sha1HashFunction::try_create("sha1")?;
    for t in tests {
        let got = func.return_type(&[t.arg.data_type().clone()]);
        let got = got.and_then(|_| func.eval(&[t.arg], 1));
        match t.expect {
            Ok(expected) => {
                assert_eq!(&got.unwrap(), &expected, "case: {}", t.name);
            }
            Err(expected_err) => {
                assert_eq!(got.unwrap_err().to_string(), expected_err.to_string());
            }
        }
    }
    Ok(())
}

#[test]
fn test_sha2hash_function() -> Result<()> {
    struct Test {
        name: &'static str,
        arg: Vec<DataColumnWithField>,
        expect: Result<DataColumn>,
    }
    let tests = vec![
        Test {
            name: "Sha0 (256)",
            arg: vec![
                DataColumnWithField::new(
                    Series::new(["abc"]).into(),
                    DataField::new("i", DataType::String, true),
                ),
                DataColumnWithField::new(
                    Series::new([0]).into(),
                    DataField::new("l", DataType::UInt16, true),
                ),
            ],
            expect: Ok(DataColumn::Constant(
                DataValue::String(Some(
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                        .as_bytes()
                        .to_vec(),
                )),
                1,
            )),
        },
        Test {
            name: "Sha224",
            arg: vec![
                DataColumnWithField::new(
                    Series::new(["abc"]).into(),
                    DataField::new("i", DataType::String, true),
                ),
                DataColumnWithField::new(
                    Series::new([224]).into(),
                    DataField::new("l", DataType::UInt16, true),
                ),
            ],
            expect: Ok(DataColumn::Constant(
                DataValue::String(Some(
                    "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
                        .as_bytes()
                        .to_vec(),
                )),
                1,
            )),
        },
        Test {
            name: "Sha256",
            arg: vec![
                DataColumnWithField::new(
                    Series::new(["abc"]).into(),
                    DataField::new("i", DataType::String, true),
                ),
                DataColumnWithField::new(
                    Series::new([256]).into(),
                    DataField::new("l", DataType::UInt16, true),
                ),
            ],
            expect: Ok(DataColumn::Constant(
                DataValue::String(Some(
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
                        .as_bytes()
                        .to_vec(),
                )),
                1,
            )),
        },
        Test {
            name: "Sha384",
            arg: vec![
                DataColumnWithField::new(
                    Series::new(["abc"]).into(),
                    DataField::new("i", DataType::String, true),
                ),
                DataColumnWithField::new(
                    Series::new([384]).into(),
                    DataField::new("l", DataType::UInt16, true),
                ),
            ],
            expect: Ok(DataColumn::Constant(
                DataValue::String(Some(
                    "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"
                        .as_bytes()
                        .to_vec(),
                )),
                1,
            )),
        },
        Test {
            name: "Sha512",
            arg: vec![
                DataColumnWithField::new(
                    Series::new(["abc"]).into(),
                    DataField::new("i", DataType::String, true),
                ),
                DataColumnWithField::new(
                    Series::new([512]).into(),
                    DataField::new("l", DataType::UInt16, true),
                ),
            ],
            expect: Ok(DataColumn::Constant(
                DataValue::String(Some(
                    "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
                        .as_bytes()
                        .to_vec(),
                )),
                1,
            )),
        },
        Test {
            name: "InvalidSha",
            arg: vec![
                DataColumnWithField::new(
                    Series::new(["abc"]).into(),
                    DataField::new("i", DataType::String, true),
                ),
                DataColumnWithField::new(
                    Series::new([1]).into(),
                    DataField::new("l", DataType::UInt16, true),
                ),
            ],
            expect: Ok(DataColumn::Constant(
                DataValue::String(None),
                1,
            )),
        },
        Test {
            name: "Sha Length as Const Field",
            arg: vec![
                DataColumnWithField::new(
                    Series::new(["abc"]).into(),
                    DataField::new("i", DataType::String, true),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::UInt16(Some(224_u16)), 1),
                    DataField::new("l", DataType::UInt16, true),
                ),
            ],
            expect: Ok(DataColumn::Constant(
                DataValue::String(Some(
                    "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
                        .as_bytes()
                        .to_vec(),
                )),
                1,
            )),
        },

        Test {
            name: "Sha Length with null value",
            arg: vec![
                DataColumnWithField::new(
                    Series::new([Some("abc"), None]).into(),
                    DataField::new("i", DataType::String, true),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::UInt16(Some(224_u16)), 1),
                    DataField::new("l", DataType::UInt16, true),
                ),
            ],
            expect: Ok(DataColumn::Array(
                Series::new(vec![  Some(
                    "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"
                        .as_bytes()
                        .to_vec(),
                ), None])
            )),
        },
    ];

    let func = Sha2HashFunction::try_create("sha2")?;
    for t in tests {
        let got = func.eval(&t.arg, 1);
        match t.expect {
            Ok(expected) => {
                assert_eq!(&got.unwrap(), &expected, "case: {}", t.name);
            }
            Err(expected_err) => {
                assert_eq!(got.unwrap_err().to_string(), expected_err.to_string());
            }
        }
    }
    Ok(())
}

#[test]
fn test_blake3hash_function() -> Result<()> {
    struct Test {
        name: &'static str,
        arg: DataColumnWithField,
        expect: Result<DataColumn>,
    }
    let tests = vec![
        Test {
            name: "valid input",
            arg: DataColumnWithField::new(
                Series::new([Some("testing")]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Constant(
                DataValue::String(Some(
                    "61cc98e42ded96807806bf1620e13c4e6a1b85068cad93382a2e3107c269aefe"
                        .as_bytes()
                        .to_vec(),
                )),
                1,
            )),
        },
        Test {
            name: "valid input with null",
            arg: DataColumnWithField::new(
                Series::new([Some("testing"), None]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Array(Series::new(vec![
                Some(
                    "61cc98e42ded96807806bf1620e13c4e6a1b85068cad93382a2e3107c269aefe"
                        .as_bytes()
                        .to_vec(),
                ),
                None,
            ]))),
        },
    ];

    let func = Blake3HashFunction::try_create("blake3")?;
    for t in tests {
        let got = func.return_type(&[t.arg.data_type().clone()]);
        let got = got.and_then(|_| func.eval(&[t.arg], 1));
        match t.expect {
            Ok(expected) => {
                assert_eq!(&got.unwrap(), &expected, "case: {}", t.name);
            }
            Err(expected_err) => {
                assert_eq!(got.unwrap_err().to_string(), expected_err.to_string());
            }
        }
    }
    Ok(())
}

#[test]
fn test_xxhash32_function() -> Result<()> {
    struct Test {
        name: &'static str,
        arg: DataColumnWithField,
        expect: Result<DataColumn>,
    }
    let tests = vec![
        Test {
            name: "valid input",
            arg: DataColumnWithField::new(
                Series::new([Some("testing")]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Constant(
                DataValue::UInt32(Some(210358520u32)),
                1,
            )),
        },
        Test {
            name: "valid input with null",
            arg: DataColumnWithField::new(
                Series::new([Some("testing"), None]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Array(Series::new(vec![
                Some(210358520u32),
                None,
            ]))),
        },
    ];

    let func = XxHash32Function::try_create("xxhash32")?;
    for t in tests {
        let got = func.return_type(&[t.arg.data_type().clone()]);
        let got = got.and_then(|_| func.eval(&[t.arg], 1));
        match t.expect {
            Ok(expected) => {
                assert_eq!(&got.unwrap(), &expected, "case: {}", t.name);
            }
            Err(expected_err) => {
                assert_eq!(got.unwrap_err().to_string(), expected_err.to_string());
            }
        }
    }
    Ok(())
}

#[test]
fn test_xxhash64_function() -> Result<()> {
    struct Test {
        name: &'static str,
        arg: DataColumnWithField,
        expect: Result<DataColumn>,
    }
    let tests = vec![
        Test {
            name: "valid input",
            arg: DataColumnWithField::new(
                Series::new([Some("testing")]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Constant(
                DataValue::UInt64(Some(5654940910216186247u64)),
                1,
            )),
        },
        Test {
            name: "valid input with null",
            arg: DataColumnWithField::new(
                Series::new([Some("testing"), None]).into(),
                DataField::new("arg1", DataType::String, true),
            ),
            expect: Ok(DataColumn::Array(Series::new(vec![
                Some(5654940910216186247u64),
                None,
            ]))),
        },
    ];

    let func = XxHash64Function::try_create("xxhash64")?;
    for t in tests {
        let got = func.return_type(&[t.arg.data_type().clone()]);
        let got = got.and_then(|_| func.eval(&[t.arg], 1));
        match t.expect {
            Ok(expected) => {
                assert_eq!(&got.unwrap(), &expected, "case: {}", t.name);
            }
            Err(expected_err) => {
                assert_eq!(got.unwrap_err().to_string(), expected_err.to_string());
            }
        }
    }
    Ok(())
}
