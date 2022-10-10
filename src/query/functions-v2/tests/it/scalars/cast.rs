// Copyright 2022 Datafuse Labs.
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

use std::io::Write;

use common_expression::from_date_data;
use common_expression::types::timestamp::Timestamp;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::Column;
use common_expression::ColumnFrom;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_cast() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast.txt").unwrap();

    test_cast_to_variant(file);
    test_cast_to_timestamp(file);
    test_cast_to_date(file);
    test_cast_between_date_and_timestamp(file);
}

fn test_cast_to_variant(file: &mut impl Write) {
    run_ast(file, "CAST(NULL AS VARIANT)", &[]);
    run_ast(file, "CAST(0 AS VARIANT)", &[]);
    run_ast(file, "CAST(-1 AS VARIANT)", &[]);
    run_ast(file, "CAST(1.1 AS VARIANT)", &[]);
    run_ast(file, "CAST(0/0 AS VARIANT)", &[]);
    run_ast(file, "CAST(1/0 AS VARIANT)", &[]);
    run_ast(file, "CAST(-1/0 AS VARIANT)", &[]);
    run_ast(file, "CAST('🍦 が美味しい' AS VARIANT)", &[]);
    run_ast(file, "CAST([0, 1, 2] AS VARIANT)", &[]);
    run_ast(file, "CAST([0, 'a'] AS VARIANT)", &[]);
    run_ast(file, "CAST(to_timestamp(1000000) AS VARIANT)", &[]);
    run_ast(file, "CAST(false AS VARIANT)", &[]);
    run_ast(file, "CAST(true AS VARIANT)", &[]);
    run_ast(
        file,
        "CAST(CAST('🍦 が美味しい' AS VARIANT) AS VARIANT)",
        &[],
    );
    // TODO(andylokandy): test CAST(<tuple> as variant)

    run_ast(file, "CAST(a AS VARIANT)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::String)),
        Column::from_data_with_validity(vec!["a", "bc", "def"], vec![true, false, true]),
    )]);

    run_ast(file, "TRY_CAST(NULL AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(0 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(-1 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(1.1 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(0/0 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(1/0 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(-1/0 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST('🍦 が美味しい' AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST([0, 1, 2] AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST([0, 'a'] AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(to_timestamp(1000000) AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(false AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(true AS VARIANT)", &[]);
    run_ast(
        file,
        "TRY_CAST(TRY_CAST('🍦 が美味しい' AS VARIANT) AS VARIANT)",
        &[],
    );

    run_ast(file, "TRY_CAST(a AS VARIANT)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::String)),
        Column::from_data_with_validity(vec!["a", "bc", "def"], vec![true, false, true]),
    )]);
}

fn test_cast_to_timestamp(file: &mut impl Write) {
    run_ast(file, "CAST(-30610224000000001 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(-315360000000000 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(-315360000000 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(-100 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(-0 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(0 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(100 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(315360000000 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(315360000000000 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(253402300800000000 AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(a AS TIMESTAMP)", &[(
        "a",
        DataType::Number(NumberDataType::Int64),
        Column::from_data(vec![
            -315360000000000i64,
            -315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);

    run_ast(file, "TRY_CAST(-30610224000000001 AS TIMESTAMP)", &[]);
    run_ast(file, "TRY_CAST(253402300800000000 AS TIMESTAMP)", &[]);
    // run_ast(file, "TRY_CAST(a AS TIMESTAMP)", &[(
    //     "a",
    //     DataType::Number(NumberDataType::Int64),
    //     Column::from_data(vec![
    //         -30610224000000001_i64,
    //         -100,
    //         0,
    //         100,
    //         253402300800000000,
    //     ]),
    // )]);

    run_ast(file, "CAST(TO_TIMESTAMP(-315360000000000) AS INT64)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(-315360000000) AS INT64)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(-100) AS INT64)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(-0) AS INT64)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(0) AS INT64)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(100) AS INT64)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(315360000000) AS INT64)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(315360000000000) AS INT64)", &[]);
    run_ast(file, "CAST(a AS INT64)", &[(
        "a",
        DataType::Timestamp,
        Column::from_data(vec![
            Timestamp {
                ts: -315360000000000,
                precision: 6,
            },
            Timestamp {
                ts: -315360000000,
                precision: 6,
            },
            Timestamp {
                ts: -100,
                precision: 6,
            },
            Timestamp {
                ts: 0,
                precision: 6,
            },
            Timestamp {
                ts: 100,
                precision: 6,
            },
            Timestamp {
                ts: 315360000000,
                precision: 6,
            },
            Timestamp {
                ts: 315360000000000,
                precision: 6,
            },
        ]),
    )]);
}

fn test_cast_to_date(file: &mut impl Write) {
    run_ast(file, "CAST(-354286 AS DATE)", &[]);
    run_ast(file, "CAST(-354285 AS DATE)", &[]);
    run_ast(file, "CAST(-100 AS DATE)", &[]);
    run_ast(file, "CAST(-0 AS DATE)", &[]);
    run_ast(file, "CAST(0 AS DATE)", &[]);
    run_ast(file, "CAST(100 AS DATE)", &[]);
    run_ast(file, "CAST(2932896 AS DATE)", &[]);
    run_ast(file, "CAST(2932897 AS DATE)", &[]);
    run_ast(file, "CAST(a AS DATE)", &[(
        "a",
        DataType::Number(NumberDataType::Int32),
        Column::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);

    run_ast(file, "TRY_CAST(-354286 AS DATE)", &[]);
    run_ast(file, "TRY_CAST(2932897 AS DATE)", &[]);
    run_ast(file, "TRY_CAST(a AS DATE)", &[(
        "a",
        DataType::Number(NumberDataType::Int32),
        Column::from_data(vec![-354286, -100, 0, 100, 2932897]),
    )]);

    run_ast(file, "CAST(TO_DATE(-354285) AS INT32)", &[]);
    run_ast(file, "CAST(TO_DATE(-100) AS INT32)", &[]);
    run_ast(file, "CAST(TO_DATE(-0) AS INT32)", &[]);
    run_ast(file, "CAST(TO_DATE(0) AS INT32)", &[]);
    run_ast(file, "CAST(TO_DATE(100) AS INT32)", &[]);
    run_ast(file, "CAST(TO_DATE(2932896) AS INT32)", &[]);
    run_ast(file, "CAST(a AS INT32)", &[(
        "a",
        DataType::Date,
        from_date_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
}

fn test_cast_between_date_and_timestamp(file: &mut impl Write) {
    run_ast(file, "CAST(TO_DATE(1) AS TIMESTAMP)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(1) AS DATE)", &[]);
    run_ast(file, "CAST(a AS DATE)", &[(
        "a",
        DataType::Timestamp,
        Column::from_data(vec![
            Timestamp {
                ts: -315360000000000,
                precision: 6,
            },
            Timestamp {
                ts: 315360000000,
                precision: 6,
            },
            Timestamp {
                ts: -100,
                precision: 6,
            },
            Timestamp {
                ts: 0,
                precision: 6,
            },
            Timestamp {
                ts: 100,
                precision: 6,
            },
            Timestamp {
                ts: 315360000000,
                precision: 6,
            },
            Timestamp {
                ts: 315360000000000,
                precision: 6,
            },
        ]),
    )]);
    run_ast(file, "CAST(a AS TIMESTAMP)", &[(
        "a",
        DataType::Date,
        from_date_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
}
