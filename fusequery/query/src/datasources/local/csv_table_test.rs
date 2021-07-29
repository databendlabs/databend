// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::env;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use futures::TryStreamExt;

use crate::datasources::local::*;

#[tokio::test]
async fn test_csv_table() -> Result<()> {
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
        DataSchemaRefExt::create(vec![DataField::new("column1", DataType::UInt64, false)]).into(),
        options,
    )?;

    let scan_plan = &ScanPlan {
        schema_name: "".to_string(),
        table_schema: DataSchemaRefExt::create(vec![]),
        table_id: 0,
        table_version: None,
        table_args: None,
        projected_schema: DataSchemaRefExt::create(vec![DataField::new(
            "column1",
            DataType::UInt64,
            false,
        )]),
        push_downs: Extras::default(),
    };
    let partitions = ctx.get_settings().get_max_threads()? as usize;
    let source_plan = table.read_plan(ctx.clone(), &scan_plan, partitions)?;
    ctx.try_set_partitions(source_plan.parts.clone())?;

    let stream = table.read(ctx, &source_plan).await?;
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
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_csv_table_parse_error() -> Result<()> {
    use std::env;

    use common_datavalues::prelude::*;
    use common_planners::*;
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

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
        DataSchemaRefExt::create(vec![
            DataField::new("column1", DataType::UInt64, false),
            DataField::new("column2", DataType::UInt64, false),
            DataField::new("column3", DataType::UInt64, false),
            DataField::new("column4", DataType::UInt64, false),
        ])
        .into(),
        options,
    )?;
    let scan_plan = &ScanPlan {
        schema_name: "".to_string(),
        table_id: 0,
        table_version: None,
        table_schema: DataSchemaRefExt::create(vec![]),
        table_args: None,
        projected_schema: DataSchemaRefExt::create(vec![DataField::new(
            "column2",
            DataType::UInt64,
            false,
        )]),
        push_downs: Extras::default(),
    };
    let partitions = ctx.get_settings().get_max_threads()? as usize;
    let source_plan = table.read_plan(ctx.clone(), &scan_plan, partitions)?;
    ctx.try_set_partitions(source_plan.parts.clone())?;

    let stream = table.read(ctx, &source_plan).await?;
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
