// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::strings::SubstringFunction;
use crate::ColumnFunction;
use crate::IFunction;
use crate::LiteralFunction;

#[test]
fn test_substring_function() -> Result<()> {
    use std::sync::Arc;

    use common_datablocks::*;
    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumnarValue>,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn IFunction>
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Utf8, false),
        DataField::new("b", DataType::UInt64, false),
        DataField::new("c", DataType::UInt64, false),
    ]);

    let tests = vec![
        Test {
            name: "substring-abcde-passed",
            display: "SUBSTRING(a,2,3)",
            nullable: false,
            arg_names: vec!["a", "b", "c"],
            columns: vec![
                Arc::new(StringArray::from(vec!["abcde"])).into(),
                Arc::new(UInt64Array::from(vec![2])).into(),
                Arc::new(UInt64Array::from(vec![3])).into(),
            ],
            func: SubstringFunction::try_create("substring")?,
            expect: Arc::new(StringArray::from(vec!["bcd"])),
            error: ""
        },
        Test {
            name: "substring-abcde-passed",
            display: "SUBSTRING(a,1,3)",
            nullable: false,
            arg_names: vec!["a", "b", "c"],
            columns: vec![
                Arc::new(StringArray::from(vec!["abcde"])).into(),
                Arc::new(UInt64Array::from(vec![1])).into(),
                Arc::new(UInt64Array::from(vec![3])).into(),
            ],
            func: SubstringFunction::try_create("substring")?,
            expect: Arc::new(StringArray::from(vec!["abc"])),
            error: ""
        },
        Test {
            name: "substring-abcde-passed",
            display: "SUBSTRING(a,2)",
            nullable: false,
            arg_names: vec!["a", "b"],
            columns: vec![
                Arc::new(StringArray::from(vec!["abcde"])).into(),
                Arc::new(UInt64Array::from(vec![2])).into(),
            ],

            func: SubstringFunction::try_create("substring")?,
            expect: Arc::new(StringArray::from(vec!["bcde"])),
            error: ""
        },
    ];

    for t in tests {
        let func = t.func;
        let rows = t.columns[0].len();
        if let Err(e) = func.eval(&t.columns, rows) {
            assert_eq!(t.error, e.to_string());
        }
        func.eval(&t.columns, rows)?;

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let ref v = func.eval(&t.columns, rows)?;

        let args = t
            .arg_names
            .iter()
            .map(|name| schema.field_with_name(name)?.data_type())
            .collect::<Result<Vec<DataType>>>()?;

        // Type check.
        let expect_type = func.return_type(&args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);

        assert_eq!(v.to_array()?.as_ref(), t.expect.as_ref());
    }
    Ok(())
}
