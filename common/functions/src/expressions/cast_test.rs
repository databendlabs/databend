// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

#[test]
fn test_cast_function() -> Result<()> {
    use std::sync::Arc;

    use common_datablocks::*;
    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    use crate::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        block: DataBlock,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn IFunction>
    }

    let field_a = ColumnFunction::try_create("a").unwrap();

    let tests = vec![
        Test {
            name: "cast-int64-to-int8-passed",
            display: "CAST(a)",
            nullable: false,
            block: DataBlock::create(
                DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int64, false)]),
                vec![Arc::new(Int64Array::from(vec![4, 3, 2, 4]))]
            ),
            func: CastFunction::create(field_a.clone(), DataType::Int8),
            expect: Arc::new(Int8Array::from(vec![4, 3, 2, 4])),
            error: ""
        },
        Test {
            name: "cast-string-to-date32-passed",
            display: "CAST(a)",
            nullable: false,
            block: DataBlock::create(
                DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]),
                vec![Arc::new(StringArray::from(vec!["20210305", "20211024"]))]
            ),
            func: CastFunction::create(field_a.clone(), DataType::Int32),
            expect: Arc::new(Int32Array::from(vec![20210305, 20211024])),
            error: ""
        },
    ];

    for t in tests {
        let func = t.func;
        if let Err(e) = func.eval(&t.block) {
            assert_eq!(t.error, e.to_string());
        }
        func.eval(&t.block)?;

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
