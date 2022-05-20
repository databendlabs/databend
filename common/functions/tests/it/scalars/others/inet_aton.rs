// Copyright 2022 Datafuse Labs
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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_try_inet_aton_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "valid input",
            columns: vec![Series::from_data(vec!["127.0.0.1"])],
            expect: Series::from_data(vec![Option::<u32>::Some(2130706433_u32)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "invalid input",
            columns: vec![Series::from_data(vec![Some("invalid")])],
            expect: Series::from_data(vec![Option::<u32>::None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "null input",
            columns: vec![Series::from_data(vec![Option::<Vec<u8>>::None])],
            expect: Series::from_data(vec![Option::<u32>::None]),
            error: "",
        },
    ];

    test_scalar_functions("try_inet_aton", &tests)
}

#[test]
fn test_inet_aton_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "valid input",
            columns: vec![Series::from_data([Some("127.0.0.1")])],
            expect: Series::from_data(vec![Option::<u32>::Some(2130706433_u32)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "null input",
            columns: vec![Series::from_data([Option::<Vec<u8>>::None])],
            expect: Series::from_data([Option::<u32>::None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "invalid input",
            columns: vec![Series::from_data([Some("1.1.1.1"), Some("batman")])],
            expect: Series::from_data(vec![Option::<u32>::None]),
            error: "Failed to parse 'batman' into a IPV4 address, invalid IP address syntax",
        },
        ScalarFunctionTest {
            name: "empty string",
            columns: vec![Series::from_data([Some("1.1.1.1"), Some("")])],
            expect: Series::from_data(vec![Option::<u32>::None]),
            error: "Failed to parse '' into a IPV4 address, invalid IPv4 address syntax",
        },
    ];

    test_scalar_functions("inet_aton", &tests)
}
