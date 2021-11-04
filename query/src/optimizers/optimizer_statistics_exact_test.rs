// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use common_datavalues::*;
    use common_exception::Result;
    use common_meta_types::TableInfo;
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::optimizers::optimizer_test::*;
    use crate::optimizers::*;

    #[test]
    fn test_statistics_exact_optimizer() -> Result<()> {
        let ctx = crate::tests::try_create_context()?;

        let total = ctx.get_settings().get_max_block_size()? as u64;
        let statistics =
            Statistics::new_exact(total as usize, ((total) * size_of::<u64>() as u64) as usize);
        ctx.try_set_statistics(&statistics)?;
        let source_plan = PlanNode::ReadSource(ReadDataSourcePlan {
            table_info: TableInfo::simple(
                "system",
                "test",
                DataSchemaRefExt::create(vec![
                    DataField::new("a", DataType::String, false),
                    DataField::new("b", DataType::String, false),
                    DataField::new("c", DataType::String, false),
                ]),
            ),
            scan_fields: None,
            parts: generate_partitions(8, total as u64),
            statistics: statistics.clone(),
            description: format!(
                "(Read from system.{} table, Exactly Read Rows:{}, Read Bytes:{})",
                "test".to_string(),
                statistics.read_rows,
                statistics.read_bytes
            ),
            tbl_args: None,
            push_downs: None,
        });

        let aggr_expr = Expression::AggregateFunction {
            op: "count".to_string(),
            distinct: false,
            params: vec![],
            args: vec![Expression::create_literal(DataValue::UInt64(Some(0)))],
        };

        let plan = PlanBuilder::from(&source_plan)
            .expression(
                &[Expression::create_literal(DataValue::UInt64(Some(0)))],
                "Before GroupBy",
            )?
            .aggregate_partial(&[aggr_expr.clone()], &[])?
            .aggregate_final(source_plan.schema(), &[aggr_expr], &[])?
            .project(&[Expression::Column("count(0)".to_string())])?
            .build()?;

        let mut statistics_exact = StatisticsExactOptimizer::create(ctx);
        let optimized = statistics_exact.optimize(&plan)?;

        let expect = "\
        Projection: count(0):UInt64\
        \n  AggregatorFinal: groupBy=[[]], aggr=[[count(0)]]\
        \n    Projection: 904e as count(0):String\
        \n      Expression: 904e:String (Exact Statistics)\
        \n        ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1], push_downs: []";
        let actual = format!("{:?}", optimized);
        assert_eq!(expect, actual);
        Ok(())
    }
}
