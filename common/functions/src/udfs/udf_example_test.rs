use std::sync::Arc;

use common_datavalues::BooleanArray;
use common_datavalues::DataArrayRef;
use common_datavalues::DataColumnarValue;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;

use crate::udfs::*;
use crate::*;

// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_udf_example_function() -> anyhow::Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        columns: Vec<DataColumnarValue>,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn IFunction>,
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Boolean, false),
        DataField::new("b", DataType::Boolean, false),
    ]);

    let tests = vec![Test {
        name: "udf-example-passed",
        display: "example()",
        nullable: false,
        func: UdfExampleFunction::try_create("example")?,
        columns: vec![
            Arc::new(BooleanArray::from(vec![true, true, true, false])).into(),
            Arc::new(BooleanArray::from(vec![true, false, true, true])).into(),
        ],
        expect: Arc::new(BooleanArray::from(vec![true, true, true, true])),
        error: "",
    }];

    for t in tests {
        let func = t.func;
        if let Err(e) = func.eval(&t.columns, t.columns[0].len()) {
            assert_eq!(t.error, e.to_string());
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let ref v = func.eval(&t.columns, t.columns[0].len())?;
        // Type check.
        let arg_types = vec![];
        let expect_type = func.return_type(&arg_types)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);
        assert_eq!(v.to_array()?.as_ref(), t.expect.as_ref());
    }
    Ok(())
}
