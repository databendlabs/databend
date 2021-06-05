// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_database_function() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    use crate::udfs::*;
    use crate::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        columns: Vec<DataColumnarValue>,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let tests = vec![Test {
        name: "database-function-passed",
        display: "database",
        nullable: false,
        func: DatabaseFunction::try_create("database")?,
        columns: vec![
            Arc::new(StringArray::from(vec!["default"])).into(),
            Arc::new(Int64Array::from(vec![4])).into(),
        ],
        expect: Arc::new(StringArray::from(vec!["default"])),
        error: "",
    }];

    for t in tests {
        let rows = t.columns[0].len();
        let func = t.func;
        match func.eval(&t.columns, rows) {
            Ok(v) => {
                // Display check.
                let expect_display = t.display.to_string();
                let actual_display = format!("{}", func);
                assert_eq!(expect_display, actual_display);

                assert_eq!(v.to_array()?.as_ref(), t.expect.as_ref());
            }
            Err(e) => {
                assert_eq!(t.error, e.to_string());
            }
        }
    }

    Ok(())
}
