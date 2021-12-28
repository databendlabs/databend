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
use crate::scalars::scalar_function_test::{ScalarFunctionTest, test_scalar_functions};

#[test]
fn test_siphash_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "Int8Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1i8, 2, 1]).into()],
            expect: Series::new(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ])
                .into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Int16Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1i16, 2, 1]).into()],
            expect: Series::new(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ])
                .into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Int32Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1i32, 2, 1]).into()],
            expect: Series::new(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ])
                .into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Int64Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1i64, 2, 1]).into()],
            expect: Series::new(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "UInt8Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1u8, 2, 1]).into()],
            expect: Series::new(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ])
                .into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "UInt16Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1u16, 2, 1]).into()],
            expect: Series::new(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ])
                .into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "UInt32Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1u32, 2, 1]).into()],
            expect: Series::new(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ])
                .into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "UInt64Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1u64, 2, 1]).into()],
            expect: Series::new(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ])
                .into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Float32Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1.0f32, 2., 1.]).into()],
            expect: Series::new(vec![
                729488449357906283u64,
                9872512741335963328,
                729488449357906283,
            ])
                .into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Float64Array siphash",
            nullable: false,
            columns: vec![Series::new(vec![1.0f64, 2., 1.]).into()],
            expect: Series::new(vec![
                13833534234735907638u64,
                12773237290464453619,
                13833534234735907638,
            ])
                .into(),
            error: "",
        },
    ];

    test_scalar_functions(SipHashFunction::try_create("siphash")?, &tests)
}

#[test]
fn test_md5hash_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "valid input",
            nullable: false,
            columns: vec![Series::new([Some("testing")]).into()],
            expect: Series::new(["ae2b1fca515949e5d54fb22b8ed95575"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "valid input with null",
            nullable: true,
            columns: vec![Series::new([Some("testing"), None]).into()],
            expect: Series::new([Some("ae2b1fca515949e5d54fb22b8ed95575"), None]).into(),
            error: "",
        },
    ];

    test_scalar_functions(Md5HashFunction::try_create("md5")?, &tests)
}

#[test]
fn test_sha1hash_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "valid input",
            nullable: false,
            columns: vec![Series::new(["abc"]).into()],
            expect: Series::new(["a9993e364706816aba3e25717850c26c9cd0d89d"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "valid input with null",
            nullable: true,
            columns: vec![Series::new([Some("abc"), None]).into()],
            expect: Series::new([Some("a9993e364706816aba3e25717850c26c9cd0d89d"), None]).into(),
            error: "",
        },
    ];

    test_scalar_functions(Sha1HashFunction::try_create("sha1")?, &tests)
}

#[test]
fn test_sha2hash_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "Sha0 (256)",
            nullable: true,
            columns: vec![Series::new(["abc"]).into(), Series::new([0_u32]).into()],
            expect: Series::new(["ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Sha224",
            nullable: true,
            columns: vec![Series::new(["abc"]).into(), Series::new([224_u32]).into()],
            expect: Series::new(["23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Sha256",
            nullable: true,
            columns: vec![Series::new(["abc"]).into(), Series::new([256_u32]).into()],
            expect: Series::new(["ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Sha384",
            nullable: true,
            columns: vec![Series::new(["abc"]).into(), Series::new([384_u32]).into()],
            expect: Series::new(["cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Sha512",
            nullable: true,
            columns: vec![Series::new(["abc"]).into(), Series::new([512_u32]).into()],
            expect: Series::new(["ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "InvalidSha",
            nullable: true,
            columns: vec![Series::new(["abc"]).into(), Series::new([1_u32]).into()],
            expect: Series::new([Option::<&str>::None]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Sha Length as Const Field",
            nullable: true,
            columns: vec![
                Series::new(["abc"]).into(),
                Series::new([224_u16]).into(),
            ],
            expect: Series::new(["23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "Sha Length with null value",
            nullable: true,
            columns: vec![
                Series::new([Some("abc"), None]).into(),
                Series::new([Some(224_u16), None]).into(),
            ],
            expect: Series::new(["23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"]).into(),
            error: "",
        },
    ];

    test_scalar_functions(Sha2HashFunction::try_create("sha2")?, &tests)
}

#[test]
fn test_blake3hash_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "valid input",
            nullable: false,
            columns: vec![Series::new([Some("testing")]).into()],
            expect: Series::new(["61cc98e42ded96807806bf1620e13c4e6a1b85068cad93382a2e3107c269aefe"]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "valid input with null",
            nullable: true,
            columns: vec![Series::new([Some("testing"), None]).into()],
            expect: Series::new([Some("61cc98e42ded96807806bf1620e13c4e6a1b85068cad93382a2e3107c269aefe"), None]).into(),
            error: "",
        },
    ];

    test_scalar_functions(Blake3HashFunction::try_create("blake3")?, &tests)
}

#[test]
fn test_xxhash32_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "valid input",
            nullable: false,
            columns: vec![Series::new([Some("testing")]).into()],
            expect: Series::new([210358520u32]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "valid input with null",
            nullable: true,
            columns: vec![Series::new([Some("testing"), None]).into()],
            expect: Series::new([Some(210358520u32), None]).into(),
            error: "",
        },
    ];

    test_scalar_functions(XxHash32Function::try_create("xxhash32")?, &tests)
}

#[test]
fn test_xxhash64_function() -> Result<()> {
    struct Test {
        name: &'static str,
        arg: DataColumnWithField,
        expect: Result<DataColumn>,
    }
    let tests = vec![
        ScalarFunctionTest {
            name: "valid input",
            nullable: false,
            columns: vec![Series::new([Some("testing")]).into()],
            expect: Series::new([5654940910216186247u64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "valid input with null",
            nullable: true,
            columns: vec![Series::new([Some("testing"), None]).into()],
            expect: Series::new(vec![Some(5654940910216186247u64), None]).into(),
            error: "",
        },
    ];

    test_scalar_functions(XxHash64Function::try_create("xxhash64")?, &tests)
}
