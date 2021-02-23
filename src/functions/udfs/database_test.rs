// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[test]
fn test_database_function() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datablocks::*;
    use crate::datavalues::*;
    use crate::functions::udfs::DatabaseFunction;
    use crate::functions::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        block: DataBlock,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn IFunction>,
    }

    let ctx = crate::tests::try_create_context()?;

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Boolean, false),
        DataField::new("b", DataType::Boolean, false),
    ]));

    let tests = vec![Test {
        name: "database-example-passed",
        display: "database()",
        nullable: false,
        func: DatabaseFunction::try_create(ctx, &[])?,
        block: DataBlock::create(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![true, true, true, false])),
                Arc::new(BooleanArray::from(vec![true, true, true, false])),
            ],
        ),
        expect: Arc::new(StringArray::from(vec![
            "default", "default", "default", "default",
        ])),
        error: "",
    }];

    for t in tests {
        let func = t.func;
        if let Err(e) = func.eval(&t.block) {
            assert_eq!(t.error, e.to_string());
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(t.block.schema())?;
        assert_eq!(expect_null, actual_null);

        let ref v = func.eval(&t.block)?;
        // Type check.
        let expect_type = func.return_type(t.block.schema())?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);
        assert_eq!(v.to_array(t.block.num_rows())?.as_ref(), t.expect.as_ref());
    }
    Ok(())
}
