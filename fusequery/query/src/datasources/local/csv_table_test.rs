// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

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
            .to_string()
    )]
    .iter()
    .cloned()
    .collect();

    let ctx = crate::tests::try_create_context()?;
    let table = CsvTable::try_create(
        "default".into(),
        "test_csv".into(),
        DataSchema::new(vec![DataField::new("column1", DataType::UInt64, false)]).into(),
        options
    )?;

    let scan_plan = &ScanPlan {
        schema_name: "".to_string(),
        table_schema: Arc::new(DataSchema::new(vec![])),
        table_args: None,
        projection: None,
        projected_schema: Arc::new(DataSchema::new(vec![DataField::new(
            "column1",
            DataType::UInt64,
            false
        )])),
        filters: vec![],
        limit: None
    };
    let source_plan = table.read_plan(ctx.clone(), &scan_plan)?;
    ctx.try_set_partitions(source_plan.partitions)?;

    let stream = table.read(ctx).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+---------+",
        "| column1 |",
        "+---------+",
        "| 1       |",
        "| 2       |",
        "| 3       |",
        "| 4       |",
        "| 5       |",
        "| 6       |",
        "+---------+",
    ];
    crate::assert_blocks_sorted_eq!(expected, result.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_csv_table_parse_error() -> anyhow::Result<()> {
    use std::env;

    use common_datavalues::*;
    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::datasources::local::*;

    let options: TableOptions = [(
        "location".to_string(),
        env::current_dir()?
            .join("../../tests/data/sample.csv")
            .display()
            .to_string()
    )]
    .iter()
    .cloned()
    .collect();

    let ctx = crate::tests::try_create_context()?;
    let table = CsvTable::try_create(
        "default".into(),
        "test_csv".into(),
        DataSchema::new(vec![
            DataField::new("column1", DataType::UInt64, false),
            DataField::new("column2", DataType::UInt64, false),
            DataField::new("column3", DataType::UInt64, false),
            DataField::new("column4", DataType::UInt64, false),
        ])
        .into(),
        options
    )?;
    let scan_plan = &ScanPlan {
        schema_name: "".to_string(),
        table_schema: Arc::new(DataSchema::new(vec![])),
        table_args: None,
        projection: None,
        projected_schema: Arc::new(DataSchema::new(vec![DataField::new(
            "column2",
            DataType::UInt64,
            false
        )])),
        filters: vec![],
        limit: None
    };
    let source_plan = table.read_plan(ctx.clone(), &scan_plan)?;
    ctx.try_set_partitions(source_plan.partitions)?;

    let stream = table.read(ctx).await?;
    let result = stream.try_collect::<Vec<_>>().await;
    assert_eq!(true, result.is_err());
    if let Err(e) = result {
        assert_eq!(
            "Code: 1002, displayText = Parser error: Error while parsing value \'Shanghai\' for column 1 at line 1.",
            e.to_string()
        );
    };

    Ok(())
}
