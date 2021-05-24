// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_column_function() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datablocks::*;
    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    use crate::*;

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Boolean, false)]);
    let block = DataBlock::create(
        schema.clone(),
        vec![Arc::new(BooleanArray::from(vec![true, true, true, false]))],
    );

    // Ok.
    {
        let col = ColumnFunction::try_create("a")?;
        let _ = col.eval(&block)?;
    }

    // Field not found error.
    {
        let col = ColumnFunction::try_create("xx")?;
        let actual = col.eval(&block);
        let expect = "Code: 1002, displayText = Invalid argument error: Unable to get field named \"xx\". Valid fields: [\"a\"].";
        assert_eq!(expect, format!("{}", actual.err().unwrap()));
    }

    Ok(())
}
