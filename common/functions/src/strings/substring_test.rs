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
        block: DataBlock,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn IFunction>
    }

    let field_a = ColumnFunction::try_create("a").unwrap();

    let tests = vec![
        Test {
            name: "substring-abcde-passed",
            display: "SUBSTRING(a,2,3)",
            nullable: false,
            block: DataBlock::create(
                DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]),
                vec![Arc::new(StringArray::from(vec!["abcde"]))]
            ),
            func: SubstringFunction::try_create("substring", &[
                field_a.clone(),
                LiteralFunction::try_create(DataValue::Int64(Some(2)))?,
                LiteralFunction::try_create(DataValue::UInt64(Some(3)))?
            ])?,
            expect: Arc::new(StringArray::from(vec!["bcd"])),
            error: ""
        },
        Test {
            name: "substring-abcde-passed",
            display: "SUBSTRING(a,1,3)",
            nullable: false,
            block: DataBlock::create(
                DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]),
                vec![Arc::new(StringArray::from(vec!["abcde"]))]
            ),
            func: SubstringFunction::try_create("substring", &[
                field_a.clone(),
                LiteralFunction::try_create(DataValue::Int64(Some(1)))?,
                LiteralFunction::try_create(DataValue::UInt64(Some(3)))?
            ])?,
            expect: Arc::new(StringArray::from(vec!["abc"])),
            error: ""
        },
        Test {
            name: "substring-abcde-passed",
            display: "SUBSTRING(a,2,NULL)",
            nullable: false,
            block: DataBlock::create(
                DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]),
                vec![Arc::new(StringArray::from(vec!["abcde"]))]
            ),
            func: SubstringFunction::try_create("substring", &[
                field_a.clone(),
                LiteralFunction::try_create(DataValue::Int64(Some(2)))?,
                LiteralFunction::try_create(DataValue::UInt64(None))?
            ])?,
            expect: Arc::new(StringArray::from(vec!["bcde"])),
            error: ""
        },
        Test {
            name: "substring-1234567890-passed",
            display: "SUBSTRING(a,-3,3)",
            nullable: false,
            block: DataBlock::create(
                DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]),
                vec![Arc::new(StringArray::from(vec!["1234567890"]))]
            ),
            func: SubstringFunction::try_create("substring", &[
                field_a.clone(),
                LiteralFunction::try_create(DataValue::Int64(Some(-3)))?,
                LiteralFunction::try_create(DataValue::UInt64(Some(3)))?
            ])?,
            expect: Arc::new(StringArray::from(vec!["890"])),
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
