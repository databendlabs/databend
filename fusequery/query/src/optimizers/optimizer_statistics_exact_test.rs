// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod tests {
    use std::mem::size_of;
    use std::sync::Arc;

    use common_datavalues::*;
    use common_exception::Result;
    use common_planners::*;
    use common_runtime::tokio;
    use pretty_assertions::assert_eq;

    use crate::optimizers::optimizer_test::*;
    use crate::optimizers::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_statistics_exact_optimizer() -> Result<()> {
        let ctx = crate::tests::try_create_context()?;

        let total = ctx.get_settings().get_max_block_size()? as u64;
        let statistics =
            Statistics::new_exact(total as usize, ((total) * size_of::<u64>() as u64) as usize);
        ctx.try_set_statistics(&statistics)?;
        let source_plan = PlanNode::ReadSource(ReadDataSourcePlan {
            db: "system".to_string(),
            table: "test".to_string(),
            schema: DataSchemaRefExt::create(vec![
                DataField::new("a", DataType::Utf8, false),
                DataField::new("b", DataType::Utf8, false),
                DataField::new("c", DataType::Utf8, false),
            ]),
            parts: generate_partitions(8, total as u64),
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

        let aggr_expr = Expression::AggregateFunction {
            op: "count".to_string(),
            distinct: false,
            args: vec![Expression::Literal(DataValue::UInt64(Some(0)))],
        };

        let plan = PlanBuilder::from(&source_plan)
            .expression(
                &[Expression::Literal(DataValue::UInt64(Some(0)))],
                "Before GroupBy",
            )?
            .aggregate_partial(&[aggr_expr.clone()], &[])?
            .aggregate_final(source_plan.schema(), &[aggr_expr], &[])?
            .project(&[Expression::Column("count(0)".to_string())])?
            .build()?;

        let mut statistics_exact = StatisticsExactOptimizer::create(ctx);
        let optimized = statistics_exact.optimize(&plan).await?;

        let expect = "\
        Projection: count(0):UInt64\
        \n  AggregatorFinal: groupBy=[[]], aggr=[[count(0)]]\
        \n    Projection: {\"Struct\":[{\"UInt64\":10000}]} as count(0):Utf8\
        \n      Expression: {\"Struct\":[{\"UInt64\":10000}]}:Utf8 (Exact Statistics)\
        \n        ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1]";
        let actual = format!("{:?}", optimized);
        assert_eq!(expect, actual);
        Ok(())
    }
}
