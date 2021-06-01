// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::udfs::*;
use crate::*;

#[test]
fn test_to_type_name_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumnarValue>,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn IFunction>,
    }

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Boolean, false)]);

    let tests = vec![Test {
        name: "to_type_name-example-passed",
        display: "toTypeName",
        nullable: false,
        arg_names: vec!["a"],
        func: ToTypeNameFunction::try_create("toTypeName")?,
        columns: vec![Arc::new(BooleanArray::from(vec![true, true, true, false])).into()],
        expect: Arc::new(StringArray::from(vec![
            "Boolean", "Boolean", "Boolean", "Boolean",
        ])),
        error: "",
    }];

    for t in tests {
        let rows = t.columns[0].len();

        let func = t.func;
        println!("{:?}", t.name);
        if let Err(e) = func.eval(&t.columns, rows) {
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

        let ref v = func.eval(&t.columns, rows)?;
        // Type check.
        let mut args = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
        }
        let expect_type = func.return_type(&args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);
        assert_eq!(v.to_array()?.as_ref(), t.expect.as_ref());
    }
    Ok(())
}
