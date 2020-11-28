// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_logic_function() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datablocks::*;
    use crate::datavalues::*;
    use crate::functions::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        func_name: &'static str,
        args: Vec<Function>,
        display: &'static str,
        nullable: bool,
        block: DataBlock,
        expect: DataArrayRef,
        error: &'static str,
        op: DataValueLogicOperator,
    }

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Boolean, false),
        DataField::new("b", DataType::Boolean, false),
    ]));

    let field_a = VariableFunction::try_create("a").unwrap();
    let field_b = VariableFunction::try_create("b").unwrap();

    let tests = vec![
        Test {
            name: "and-passed",
            func_name: "AndFunction",
            args: vec![field_a.clone(), field_b.clone()],
            display: "a and b",
            nullable: false,
            op: DataValueLogicOperator::And,
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
            args: vec![field_a.clone(), field_b.clone()],
            display: "a or b",
            nullable: false,
            op: DataValueLogicOperator::Or,
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
        let mut func = LogicFunction::try_create(t.op, &[t.args[0].clone(), t.args[1].clone()])?;
        if let Err(e) = func.eval(&t.block) {
            assert_eq!(t.error, e.to_string());
        }
        func.eval(&t.block)?;

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{:?}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(t.block.schema())?;
        assert_eq!(expect_null, actual_null);

        let ref v = func.result()?;
        // Type check.
        let expect_type = func.return_type(t.block.schema())?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);

        // Result check.
        if !v.to_array(t.block.num_rows())?.equals(&*t.expect) {
            println!("{}, expect:\n{:?} \nactual:\n{:?}", t.name, t.expect, v);
            assert!(false);
        }
    }
    Ok(())
}
