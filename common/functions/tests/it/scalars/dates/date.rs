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
use common_datavalues::ColumnWithField;
use common_exception::Result;

use crate::scalars::scalar_function_test::test_scalar_functions_with_type;
use crate::scalars::scalar_function_test::ScalarFunctionWithFieldTest;

#[test]
fn test_round_function() -> Result<()> {
    let ops = vec![
        "toStartOfSecond",
        "toStartOfMinute",
        "toStartOfTenMinutes",
        "toStartOfFifteenMinutes",
        "timeSlot",
        "toStartOfHour",
        "toStartOfDay",
    ];
    let rounds = vec![1, 60, 60 * 10, 60 * 15, 60 * 30, 60 * 60, 60 * 60 * 24];

    for (op, r) in ops.iter().cloned().zip(rounds.iter()) {
        test_scalar_functions_with_type(op, &[ScalarFunctionWithFieldTest {
            name: "test-timeSlot-now",
            columns: vec![ColumnWithField::new(
                Series::from_data(vec![1630812366000000i64, 1630839682000000i64]),
                DataField::new("dummy_1", TimestampType::new_impl(0)),
            )],
            expect: Series::from_data(vec![
                1630812366i64 / r * r * MICROSECONDS,
                1630839682i64 / r * r * MICROSECONDS,
            ]),
            error: "",
        }])?;
    }

    Ok(())
}

#[test]
fn test_to_start_of_function() -> Result<()> {
    let test = vec![ScalarFunctionWithFieldTest {
        name: "test-timeSlot-now",
        columns: vec![ColumnWithField::new(
            Series::from_data(vec![1631705259000000i64]),
            DataField::new("dummy_1", TimestampType::new_impl(0)),
        )],
        expect: Series::from_data(vec![18809i32]),
        error: "",
    }];

    test_scalar_functions_with_type("toStartOfQuarter", &test)
}
