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

use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::utils::from_date_data;
use common_expression::utils::from_timestamp_data;
use common_expression::utils::ColumnFrom;
use common_expression::Column;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_cast() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("cast.txt").unwrap();

    test_cast_primitive(file);
    test_cast_to_variant(file);
    test_cast_number_to_timestamp(file);
    test_cast_number_to_date(file);
    test_cast_between_date_and_timestamp(file);
    test_cast_between_string_and_timestamp(file);
    test_between_string_and_date(file);
}

fn test_cast_primitive(file: &mut impl Write) {
    run_ast(file, "TRY_CAST(a AS UINT8)", &[(
        "a",
        DataType::Number(NumberDataType::UInt16),
        Column::from_data(vec![0u16, 64, 255, 512, 1024]),
    )]);
    run_ast(file, "TRY_CAST(a AS UINT16)", &[(
        "a",
        DataType::Number(NumberDataType::Int16),
        Column::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);
    run_ast(file, "TRY_CAST(a AS INT64)", &[(
        "a",
        DataType::Number(NumberDataType::Int16),
        Column::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);
    run_ast(
        file,
        "(TRY_CAST(a AS FLOAT32), TRY_CAST(a AS INT32), TRY_CAST(b AS FLOAT32), TRY_CAST(b AS INT32))",
        &[
            (
                "a",
                DataType::Number(NumberDataType::UInt64),
                Column::from_data(vec![
                    0,
                    1,
                    u8::MAX as u64,
                    u16::MAX as u64,
                    u32::MAX as u64,
                    u64::MAX,
                ]),
            ),
            (
                "b",
                DataType::Number(NumberDataType::Float64),
                Column::from_data(vec![
                    0.0,
                    u32::MAX as f64,
                    u64::MAX as f64,
                    f64::MIN,
                    f64::MAX,
                    f64::INFINITY,
                ]),
            ),
        ],
    );
    run_ast(
        file,
        "TRY_CAST([[a, b], NULL, NULL] AS Array(Array(Int8)))",
        &[
            (
                "a",
                DataType::Number(NumberDataType::Int16),
                Column::from_data(vec![0i16, 1, 2, 127, 255]),
            ),
            (
                "b",
                DataType::Number(NumberDataType::Int16),
                Column::from_data(vec![0i16, -1, -127, -128, -129]),
            ),
        ],
    );
    run_ast(
        file,
        "TRY_CAST((a, b, NULL) AS TUPLE(Int8, UInt8, Boolean NULL))",
        &[
            (
                "a",
                DataType::Number(NumberDataType::Int16),
                Column::from_data(vec![0i16, 1, 2, 127, 256]),
            ),
            (
                "b",
                DataType::Number(NumberDataType::Int16),
                Column::from_data(vec![0i16, 1, -127, -128, -129]),
            ),
        ],
    );
    run_ast(file, "CAST(a AS INT16)", &[(
        "a",
        DataType::Number(NumberDataType::Float64),
        Column::from_data(vec![0.0f64, 1.1, 2.2, 3.3, -4.4]),
    )]);
    run_ast(file, "CAST(b AS INT16)", &[(
        "b",
        DataType::Number(NumberDataType::Int8),
        Column::from_data(vec![0i8, 1, 2, 3, -4]),
    )]);

    run_ast(file, "CAST(a AS UINT16)", &[(
        "a",
        DataType::Number(NumberDataType::Int16),
        Column::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);

    run_ast(file, "CAST(c AS INT16)", &[(
        "c",
        DataType::Number(NumberDataType::Int64),
        Column::from_data(vec![0i64, 11111111111, 2, 3, -4]),
    )]);
}

fn test_cast_to_variant(file: &mut impl Write) {
    run_ast(file, "CAST(NULL AS VARIANT)", &[]);
    run_ast(file, "CAST(0 AS VARIANT)", &[]);
    run_ast(file, "CAST(-1 AS VARIANT)", &[]);
    run_ast(file, "CAST(1.1 AS VARIANT)", &[]);
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
    run_ast(file, "CAST((1,) AS VARIANT)", &[]);
    run_ast(file, "CAST((1, 2) AS VARIANT)", &[]);
    run_ast(file, "CAST((false, true) AS VARIANT)", &[]);
    run_ast(file, "CAST(('a',) AS VARIANT)", &[]);
    run_ast(file, "CAST((1, 2, (false, true, ('a',))) AS VARIANT)", &[]);

    run_ast(file, "CAST(a AS VARIANT)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::String)),
        Column::from_data_with_validity(vec!["a", "bc", "def"], vec![true, false, true]),
    )]);

    run_ast(file, "TRY_CAST(NULL AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(0 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(-1 AS VARIANT)", &[]);
    run_ast(file, "TRY_CAST(1.1 AS VARIANT)", &[]);
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

fn test_cast_number_to_timestamp(file: &mut impl Write) {
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
    run_ast(file, "TRY_CAST(a AS TIMESTAMP)", &[(
        "a",
        DataType::Number(NumberDataType::Int64),
        Column::from_data(vec![
            -30610224000000001_i64,
            -100,
            0,
            100,
            253402300800000000,
        ]),
    )]);

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
        from_timestamp_data(vec![
            -315360000000000,
            -315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);
}

fn test_cast_number_to_date(file: &mut impl Write) {
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

    run_ast(file, "CAST(TO_DATE(-354285) AS INT64)", &[]);
    run_ast(file, "CAST(TO_DATE(-100) AS INT64)", &[]);
    run_ast(file, "CAST(TO_DATE(-0) AS INT64)", &[]);
    run_ast(file, "CAST(TO_DATE(0) AS INT64)", &[]);
    run_ast(file, "CAST(TO_DATE(100) AS INT64)", &[]);
    run_ast(file, "CAST(TO_DATE(2932896) AS INT64)", &[]);
    run_ast(file, "CAST(a AS INT64)", &[(
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
        from_timestamp_data(vec![
            -315360000000000,
            -315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);
    run_ast(file, "CAST(a AS TIMESTAMP)", &[(
        "a",
        DataType::Date,
        from_date_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
    run_ast(file, "CAST(TO_DATE(a) AS TIMESTAMP)", &[(
        "a",
        DataType::Number(NumberDataType::Int32),
        Column::from_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
}

fn test_cast_between_string_and_timestamp(file: &mut impl Write) {
    run_ast(file, "TO_TIMESTAMP('2022')", &[]);
    run_ast(file, "TO_TIMESTAMP('2022-01')", &[]);
    run_ast(file, "TO_TIMESTAMP('2022-01-02')", &[]);
    run_ast(file, "TO_TIMESTAMP('A NON-TIMESTMAP STR')", &[]);
    run_ast(file, "TO_TIMESTAMP('2022-01-02T03:25:02.868894-07:00')", &[
    ]);
    run_ast(file, "TO_TIMESTAMP('2022-01-02 02:00:11')", &[]);
    run_ast(file, "TO_TIMESTAMP('2022-01-02T02:00:22')", &[]);
    run_ast(file, "TO_TIMESTAMP('2022-01-02T01:12:00-07:00')", &[]);
    run_ast(file, "TO_TIMESTAMP('2022-01-02T01')", &[]);
    run_ast(file, "TO_TIMESTAMP(a)", &[(
        "a",
        DataType::String,
        Column::from_data(vec![
            "2022-01-02",
            "2022-01-02T03:25:02.868894-07:00",
            "2022-01-02 02:00:11",
            "2022-01-02T01:12:00-07:00",
            "2022-01-02T01",
        ]),
    )]);

    run_ast(file, "TRY_CAST(a as TIMESTAMP)", &[(
        "a",
        DataType::String,
        Column::from_data(vec![
            "A NON-TIMESTAMP STR",
            "2022",
            "2022-01",
            "2022-01-02",
            "2022-01-02T03:25:02.868894-07:00",
            "2022-01-02 02:00:11",
            "2022-01-02T01:12:00-07:00",
            "2022-01-02T01",
        ]),
    )]);

    run_ast(file, "CAST(TO_TIMESTAMP(-315360000000000) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(-315360000000) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(-100) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(-0) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(0) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(100) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(315360000000) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_TIMESTAMP(315360000000000) AS VARCHAR)", &[]);
    run_ast(file, "CAST(a AS VARCHAR)", &[(
        "a",
        DataType::Timestamp,
        from_timestamp_data(vec![
            -315360000000000,
            -315360000000,
            -100,
            0,
            100,
            315360000000,
            315360000000000,
        ]),
    )]);
}

fn test_between_string_and_date(file: &mut impl Write) {
    run_ast(file, "TO_DATE('2022')", &[]);
    run_ast(file, "TO_DATE('2022-01')", &[]);
    run_ast(file, "TO_DATE('2022-01-02')", &[]);
    run_ast(file, "TO_DATE('A NON-DATE STR')", &[]);
    run_ast(file, "TO_DATE('2022-01-02T03:25:02.868894-07:00')", &[]);
    run_ast(file, "TO_DATE('2022-01-02 02:00:11')", &[]);
    run_ast(file, "TO_DATE('2022-01-02T02:00:22')", &[]);
    run_ast(file, "TO_DATE('2022-01-02T01:12:00-07:00')", &[]);
    run_ast(file, "TO_DATE('2022-01-02T01')", &[]);
    run_ast(file, "TO_DATE(a)", &[(
        "a",
        DataType::String,
        Column::from_data(vec![
            "2022-01-02",
            "2022-01-02T03:25:02.868894-07:00",
            "2022-01-02 02:00:11",
            "2022-01-02T01:12:00-07:00",
            "2022-01-02T01",
        ]),
    )]);

    run_ast(file, "TRY_CAST(a as DATE)", &[(
        "a",
        DataType::String,
        Column::from_data(vec![
            "A NON-DATE STR",
            "2022",
            "2022-01",
            "2022-01-02",
            "2022-01-02T03:25:02.868894-07:00",
            "2022-01-02 02:00:11",
            "2022-01-02T01:12:00-07:00",
            "2022-01-02T01",
        ]),
    )]);

    run_ast(file, "CAST(TO_DATE(-354285) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_DATE(-100) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_DATE(-0) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_DATE(0) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_DATE(100) AS VARCHAR)", &[]);
    run_ast(file, "CAST(TO_DATE(2932896) AS VARCHAR)", &[]);
    run_ast(file, "CAST(a AS VARCHAR)", &[(
        "a",
        DataType::Date,
        from_date_data(vec![-354285, -100, 0, 100, 2932896]),
    )]);
}
