// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test]
async fn test_csv_table() -> anyhow::Result<()> {
    use std::env;

    use common_datavalues::*;
    use common_planners::*;
    use futures::TryStreamExt;

    use crate::datasources::local::*;

    let options: TableOptions = [(
        "location".to_string(),
        env::current_dir()?
            .join("../../tests/data/sample.csv")
            .display()
            .to_string(),
    )]
    .iter()
    .cloned()
    .collect();

    let ctx = crate::tests::try_create_context()?;
    let table = CsvTable::try_create(
        "default".into(),
        "test_csv".into(),
        DataSchema::new(vec![DataField::new("a", DataType::UInt64, false)]).into(),
        options,
    )?;
    table.read_plan(ctx.clone(), PlanBuilder::empty().build()?)?;

    let stream = table.read(ctx).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();

    assert_eq!(rows, 4);
    Ok(())
}
