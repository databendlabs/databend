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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

use super::scalar_function2_test::test_eval;
use super::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_tuple_function() -> Result<()> {
    let types = vec![vec![UInt8Type::arc()], vec![
        UInt8Type::arc(),
        UInt8Type::arc(),
    ]];
    let tests = vec![
        (
            vec![DataValue::Struct(vec![DataValue::UInt64(0)])],
            ScalarFunctionTest {
                name: "one element to tuple",
                columns: vec![Series::from_data([0_u8])],
                expect: Series::from_data([0_u8]),
                error: "",
            },
        ),
        (
            vec![DataValue::Struct(vec![
                DataValue::UInt64(0),
                DataValue::UInt64(0),
            ])],
            ScalarFunctionTest {
                name: "more element to tuple",
                columns: vec![Series::from_data([0_u8]), Series::from_data([0_u8])],
                expect: Series::from_data([0_u8]),
                error: "",
            },
        ),
    ];

    for ((val, test), typ) in tests.iter().zip(types.iter()) {
        let func = TupleFunction::try_create_func("", &typ.iter().collect::<Vec<_>>())?;
        let result = test_eval(&func, &test.columns, false)?;
        let result = result.convert_full_column();

        let result = (0..result.len()).map(|i| result.get(i)).collect::<Vec<_>>();
        assert!(&result == val)
    }

    Ok(())
}
