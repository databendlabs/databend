// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_numbers_table_generate_parts() -> anyhow::Result<()> {
    use common_planners::Partition;
    use pretty_assertions::assert_eq;

    use crate::datasources::system::NumbersTable;

    let t = NumbersTable::create("foo");

    {
        // deal with remainder
        let ps = t.generate_parts(3, 11);

        assert_eq!(3, ps.len());
        assert_eq!(
            Partition {
                name: "11-0-3".into(),
                version: 0,
            },
            ps[0]
        );
        assert_eq!(
            Partition {
                name: "11-3-6".into(),
                version: 0,
            },
            ps[1]
        );
        assert_eq!(
            Partition {
                name: "11-6-11".into(),
                version: 0,
            },
            ps[2]
        );
    }

    {
        // total is zero
        let ps = t.generate_parts(3, 0);

        assert_eq!(1, ps.len());
        assert_eq!(
            Partition {
                name: "0-0-0".into(),
                version: 0,
            },
            ps[0]
        );
    }
    {
        // only one part, total < workers
        let ps = t.generate_parts(3, 2);

        assert_eq!(1, ps.len());
        assert_eq!(
            Partition {
                name: "2-0-2".into(),
                version: 0,
            },
            ps[0]
        );
    }

    Ok(())
}

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
            false,
        )])),
        filters: vec![],
        limit: None,
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
