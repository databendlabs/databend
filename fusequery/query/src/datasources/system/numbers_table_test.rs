// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test]
async fn test_number_table() -> anyhow::Result<()> {
    use std::sync::Arc;

    use common_datavalues::*;
    use common_planners::*;
    use futures::TryStreamExt;

    use crate::datasources::system::*;
    use crate::datasources::*;

    let ctx = crate::tests::try_create_context()?;
    let table = NumbersTable::create("numbers_mt");

    let scan = &ScanPlan {
        schema_name: "scan_test".to_string(),
        table_schema: Arc::new(DataSchema::new(vec![])),
        table_args: Some(ExpressionPlan::Literal(DataValue::UInt64(Some(8)))),
        projection: None,
        projected_schema: Arc::new(DataSchema::new(vec![DataField::new(
            "number",
            DataType::UInt64,
            false
        )])),
        filters: vec![],
        limit: None
    };
    let source_plan = table.read_plan(ctx.clone(), scan)?;
    ctx.try_set_partitions(source_plan.partitions)?;

    let stream = table.read(ctx).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 0      |",
        "| 1      |",
        "| 2      |",
        "| 3      |",
        "| 4      |",
        "| 5      |",
        "| 6      |",
        "| 7      |",
        "+--------+",
    ];
    crate::assert_blocks_sorted_eq!(expected, result.as_slice());

    Ok(())
}
