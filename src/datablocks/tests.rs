// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_datablock() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datablocks::*;
    use crate::datavalues::*;

    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "a",
        DataType::Int64,
        false,
    )]));

    let block = DataBlock::create(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    );
    assert_eq!(&schema, block.schema());

    assert_eq!(3, block.num_rows());
    assert_eq!(1, block.num_columns());
    assert_eq!(3, block.column_by_name("a")?.len());
    assert_eq!(3, block.column(0).len());

    Ok(())
}
