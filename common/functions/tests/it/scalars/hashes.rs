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

use common_datavalues2::prelude::*;
use common_exception::Result;
use common_functions::scalars::Blake3HashFunction;
use common_functions::scalars::Md5HashFunction;
use common_functions::scalars::Sha1HashFunction;
use common_functions::scalars::Sha2HashFunction;
use common_functions::scalars::SipHash64Function;
use common_functions::scalars::XxHash32Function;
use common_functions::scalars::XxHash64Function;

use super::scalar_function2_test::test_scalar_functions2;
use super::scalar_function2_test::ScalarFunction2Test;

#[test]
fn test_siphash_function() -> Result<()> {
    let tests = vec![
        ScalarFunction2Test {
            name: "Int8Array siphash",
            columns: vec![Series::from_data(vec![1i8, 2, 1])],
            expect: Series::from_data(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Int16Array siphash",
            columns: vec![Series::from_data(vec![1i16, 2, 1])],
            expect: Series::from_data(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Int32Array siphash",
            columns: vec![Series::from_data(vec![1i32, 2, 1])],
            expect: Series::from_data(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Int64Array siphash",
            columns: vec![Series::from_data(vec![1i64, 2, 1])],
            expect: Series::from_data(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ]),
            error: "",
        },
        ScalarFunction2Test {
            name: "UInt8Array siphash",
            columns: vec![Series::from_data(vec![1u8, 2, 1])],
            expect: Series::from_data(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ]),
            error: "",
        },
        ScalarFunction2Test {
            name: "UInt16Array siphash",
            columns: vec![Series::from_data(vec![1u16, 2, 1])],
            expect: Series::from_data(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ]),
            error: "",
        },
        ScalarFunction2Test {
            name: "UInt32Array siphash",
            columns: vec![Series::from_data(vec![1u32, 2, 1])],
            expect: Series::from_data(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ]),
            error: "",
        },
        ScalarFunction2Test {
            name: "UInt64Array siphash",
            columns: vec![Series::from_data(vec![1u64, 2, 1])],
            expect: Series::from_data(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Float32Array siphash",
            columns: vec![Series::from_data(vec![1.0f32, 2., 1.])],
            expect: Series::from_data(vec![
                729488449357906283u64,
                9872512741335963328,
                729488449357906283,
            ]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Float64Array siphash",
            columns: vec![Series::from_data(vec![1.0f64, 2., 1.])],
            expect: Series::from_data(vec![
                13833534234735907638u64,
                12773237290464453619,
                13833534234735907638,
            ]),
            error: "",
        },
    ];

    test_scalar_functions2(SipHash64Function::try_create("siphash")?, &tests)
}

#[test]
fn test_md5hash_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "valid input",
        columns: vec![Series::from_data(["testing"])],
        expect: Series::from_data(["ae2b1fca515949e5d54fb22b8ed95575"]),
        error: "",
    }];

    test_scalar_functions2(Md5HashFunction::try_create("md5")?, &tests)
}

#[test]
fn test_sha1hash_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "valid input",
        columns: vec![Series::from_data(["abc"])],
        expect: Series::from_data(["a9993e364706816aba3e25717850c26c9cd0d89d"]),
        error: "",
    }];

    test_scalar_functions2(Sha1HashFunction::try_create("sha1")?, &tests)
}

#[test]
fn test_sha2hash_function() -> Result<()> {
    let tests = vec![
        ScalarFunction2Test {
            name: "Sha0 (256)",
            columns: vec![Series::from_data(["abc"]), Series::from_data([0_u32])],
            expect: Series::from_data(["ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Sha224",
            columns: vec![Series::from_data(["abc"]), Series::from_data([224_u32])],
            expect: Series::from_data(["23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Sha256",
            columns: vec![Series::from_data(["abc"]), Series::from_data([256_u32])],
            expect: Series::from_data(["ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Sha384",
            columns: vec![Series::from_data(["abc"]), Series::from_data([384_u32])],
            expect: Series::from_data(["cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7"]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Sha512",
            columns: vec![Series::from_data(["abc"]), Series::from_data([512_u32])],
            expect: Series::from_data(["ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"]),
            error: "",
        },
        ScalarFunction2Test {
            name: "InvalidSha",
            columns: vec![Series::from_data(["abc"]), Series::from_data([1_u32])],
            expect: Series::from_data([Option::<&str>::None]),
            error: "Expected [0, 224, 256, 384, 512] as sha2 encode options, but got 1",
        },
        ScalarFunction2Test {
            name: "Sha Length as Const Field",
            columns: vec![
                Series::from_data(["abc"]),
                Series::from_data([224_u16]),
            ],
            expect: Series::from_data(["23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7"]),
            error: "",
        },
        ScalarFunction2Test {
            name: "Sha Length with null value",
            columns: vec![
                Series::from_data([Option::<&str>::None]),
                Series::from_data([Option::<u16>::None]),
            ],
            expect: Series::from_data([Option::<&str>::None]),
            error: "",
        },
    ];

    test_scalar_functions2(Sha2HashFunction::try_create("sha2")?, &tests)
}

#[test]
fn test_blake3hash_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "valid input",
        columns: vec![Series::from_data(["testing"])],
        expect: Series::from_data([
            "61cc98e42ded96807806bf1620e13c4e6a1b85068cad93382a2e3107c269aefe",
        ]),
        error: "",
    }];

    test_scalar_functions2(Blake3HashFunction::try_create("blake3")?, &tests)
}

#[test]
fn test_xxhash32_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "valid input",
        columns: vec![Series::from_data(["testing"])],
        expect: Series::from_data([210358520u32]),
        error: "",
    }];

    test_scalar_functions2(XxHash32Function::try_create("xxhash32")?, &tests)
}

#[test]
fn test_xxhash64_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "valid input",
        columns: vec![Series::from_data(["testing"])],
        expect: Series::from_data([5654940910216186247u64]),
        error: "",
    }];

    test_scalar_functions2(XxHash64Function::try_create("xxhash64")?, &tests)
}
