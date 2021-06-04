// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod tests {
    use std::mem::size_of;
    use std::sync::Arc;

    use common_datavalues::*;
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::optimizers::optimizer_test::*;
    use crate::optimizers::*;

    #[test]
    fn test_constant_folding_optimizer() -> anyhow::Result<()> {
        let ctx = crate::tests::try_create_context()?;

        let total = ctx.get_max_block_size()? as u64;
        let statistics = Statistics {
            read_rows: total as usize,
            read_bytes: ((total) * size_of::<u64>() as u64) as usize,
        };
        ctx.try_set_statistics(&statistics)?;
        let source_plan = PlanNode::ReadSource(ReadDataSourcePlan {
            db: "system".to_string(),
            table: "test".to_string(),
            schema: DataSchemaRefExt::create(vec![
                DataField::new("a", DataType::Utf8, false),
                DataField::new("b", DataType::Utf8, false),
                DataField::new("c", DataType::Utf8, false),
            ]),
            partitions: generate_partitions(8, total as u64),
            statistics: statistics.clone(),
            description: format!(
                "(Read from system.{} table, Read Rows:{}, Read Bytes:{})",
                "test".to_string(),
                statistics.read_rows,
                statistics.read_bytes
            ),
            scan_plan: Arc::new(ScanPlan::empty()),
            remote: false,
        });

        let filter_plan = PlanBuilder::from(&source_plan)
            .filter(col("a").gt(lit(6)).eq(lit(true)))?
            .build()?;

        let plan = PlanNode::Projection(ProjectionPlan {
            expr: vec![col("a")],
            schema: DataSchemaRefExt::create(vec![DataField::new("a", DataType::Utf8, false)]),
            input: Arc::from(filter_plan),
        });

        let mut constant_folding = ConstantFoldingOptimizer::create(ctx);
        let optimized = constant_folding.optimize(&plan)?;

        let expect = "\
        Projection: a:Utf8\
        \n  Filter: (a > 6)\
        \n    ReadDataSource: scan partitions: [8], scan schema: [a:Utf8, b:Utf8, c:Utf8], statistics: [read_rows: 10000, read_bytes: 80000]";
        let actual = format!("{:?}", optimized);
        assert_eq!(expect, actual);
        Ok(())
    }
}
