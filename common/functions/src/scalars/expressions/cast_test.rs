// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::scalars::*;

#[test]
fn test_cast_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        columns: Vec<DataColumn>,
        cast_type: DataType,
        expect: Series,
        error: &'static str,
        func: Result<Box<dyn Function>>,
    }

    let tests = vec![
        Test {
            name: "cast-int64-to-int8-passed",
            display: "CAST",
            nullable: false,
            columns: vec![Series::new(vec![4i64, 3, 2, 4]).into()],
            func: CastFunction::create(DataType::Int8),
            cast_type: DataType::Int8,
            expect: Series::new(vec![4i32, 3, 2, 4]),
            error: "",
        },
        Test {
            name: "cast-string-to-date32-passed",
            display: "CAST",
            nullable: false,
            columns: vec![Series::new(vec!["20210305", "20211024"]).into()],
            func: CastFunction::create(DataType::Int32),
            cast_type: DataType::Date32,
            expect: Series::new(vec![20210305i32, 20211024]),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();
        let func = t.func.unwrap();
        if let Err(e) = func.eval(&t.columns, rows) {
            assert_eq!(t.error, e.to_string());
        }
        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&DataSchema::empty())?;
        assert_eq!(expect_null, actual_null);

        let ref v = func.eval(&t.columns, rows)?;
        // Type check.
        let args = vec![t.cast_type];
        let expect_type = func.return_type(&args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);

        let c: DataColumn = t.expect.into();
        assert_eq!(v, &c);
    }
    Ok(())
}
