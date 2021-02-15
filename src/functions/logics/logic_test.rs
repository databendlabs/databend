// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

#[test]
fn test_logic_function() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datablocks::*;
    use crate::datavalues::*;
    use crate::functions::logics::*;
    use crate::functions::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        func_name: &'static str,
        display: &'static str,
        nullable: bool,
        block: DataBlock,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn IFunction>,
    }

    let ctx = crate::sessions::FuseQueryContext::try_create()?;

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Boolean, false),
        DataField::new("b", DataType::Boolean, false),
    ]));

    let field_a = FieldFunction::try_create("a").unwrap();
    let field_b = FieldFunction::try_create("b").unwrap();

    let tests = vec![
        Test {
            name: "and-passed",
            func_name: "AndFunction",
            display: "a and b",
            nullable: false,
            func: LogicAndFunction::try_create_func(
                ctx.clone(),
                &[field_a.clone(), field_b.clone()],
            )?,
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(BooleanArray::from(vec![true, true, true, false])),
                    Arc::new(BooleanArray::from(vec![true, false, true, true])),
                ],
            ),
            expect: Arc::new(BooleanArray::from(vec![true, false, true, false])),
            error: "",
        },
        Test {
            name: "or-passed",
            func_name: "OrFunction",
            display: "a or b",
            nullable: false,
            func: LogicOrFunction::try_create_func(ctx, &[field_a.clone(), field_b.clone()])?,
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(BooleanArray::from(vec![true, true, true, false])),
                    Arc::new(BooleanArray::from(vec![true, false, true, true])),
                ],
            ),
            expect: Arc::new(BooleanArray::from(vec![true, true, true, true])),
            error: "",
        },
    ];

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
