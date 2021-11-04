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

use std::mem::size_of;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::*;
use pretty_assertions::assert_eq;

use crate::optimizers::optimizer_test::*;
use crate::optimizers::*;
use crate::sql::*;

#[test]
fn test_projection_push_down_optimizer_1() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::String, false),
        DataField::new("b", DataType::String, false),
        DataField::new("c", DataType::String, false),
        DataField::new("d", DataType::String, false),
    ]);

    let output_schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::String, false),
        DataField::new("b", DataType::String, false),
        DataField::new("c", DataType::String, false),
    ]);

    let plan = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a"), col("b"), col("c")],
        schema: output_schema,
        input: Arc::from(
            PlanBuilder::from(&PlanNode::Empty(EmptyPlan::create_with_schema(schema))).build()?,
        ),
    });

    let mut projection_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = projection_push_down.optimize(&plan)?;

    let expect = "\
        Projection: a:String, b:String, c:String";

    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}

#[test]
fn test_projection_push_down_optimizer_group_by() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone())
        .build_from_sql("select max(value) as c1, name as c2 from system.settings group by c2")?;

    let mut project_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = project_push_down.optimize(&plan)?;

    let expect = "\
        Projection: max(value) as c1:String, name as c2:String\
        \n  AggregatorFinal: groupBy=[[name]], aggr=[[max(value)]]\
        \n    AggregatorPartial: groupBy=[[name]], aggr=[[max(value)]]\
        \n      ReadDataSource: scan partitions: [1], scan schema: [name:String, value:String], statistics: [read_rows: 0, read_bytes: 0], push_downs: []";

    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);
    Ok(())
}

#[test]
fn test_projection_push_down_optimizer_2() -> Result<()> {
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

    let filter_plan = PlanBuilder::from(&source_plan)
        .filter(col("a").gt(lit(6)).and(col("b").lt_eq(lit(10))))?
        .build()?;

    let plan = PlanNode::Projection(ProjectionPlan {
        expr: vec![col("a")],
        schema: DataSchemaRefExt::create(vec![DataField::new("a", DataType::String, false)]),
        input: Arc::from(filter_plan),
    });

    let mut projection_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = projection_push_down.optimize(&plan)?;

    let expect = "\
        Projection: a:String\
        \n  Filter: ((a > 6) and (b <= 10))\
        \n    ReadDataSource: scan partitions: [8], scan schema: [a:String, b:String], statistics: [read_rows: 10000, read_bytes: 80000]";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}

#[test]
fn test_projection_push_down_optimizer_3() -> Result<()> {
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
                DataField::new("d", DataType::String, false),
                DataField::new("e", DataType::String, false),
                DataField::new("f", DataType::String, false),
                DataField::new("g", DataType::String, false),
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

    let group_exprs = &[col("a"), col("c")];

    // SELECT a FROM table WHERE b = 10 GROUP BY a, c having a < 10 order by c LIMIT 10;
    let plan = PlanBuilder::from(&source_plan)
        .filter(col("b").eq(lit(10)))?
        .aggregate_partial(&[], group_exprs)?
        .aggregate_final(source_plan.schema(), &[], group_exprs)?
        .having(col("a").lt(lit(10)))?
        .sort(&[col("c")])?
        .limit(10)?
        .project(&[col("a")])?
        .build()?;

    let mut projection_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = projection_push_down.optimize(&plan)?;

    let expect = "\
    Projection: a:String\
    \n  Limit: 10\
    \n    Sort: c:String\
    \n      Having: (a < 10)\
    \n        AggregatorFinal: groupBy=[[a, c]], aggr=[[]]\
    \n          AggregatorPartial: groupBy=[[a, c]], aggr=[[]]\
    \n            Filter: (b = 10)\
    \n              ReadDataSource: scan partitions: [8], scan schema: [a:String, b:String, c:String], statistics: [read_rows: 10000, read_bytes: 80000]";

    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    Ok(())
}

#[test]
fn test_projection_push_down_optimizer_4() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone())
        .build_from_sql("select substring(value from 1 for 3)  as c1 from system.settings")?;

    let mut project_push_down = ProjectionPushDownOptimizer::create(ctx);
    let optimized = project_push_down.optimize(&plan)?;

    let expect = "Projection: substring(value, 1, 3) as c1:String\
                        \n  Expression: substring(value, 1, 3):String (Before Projection)\
                        \n    ReadDataSource: scan partitions: [1], scan schema: [value:String], statistics: [read_rows: 0, read_bytes: 0], push_downs: []";

    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);

    let read_source_node = optimized.input(0).input(0).input(0);
    match read_source_node.as_ref() {
        PlanNode::ReadSource(ref read_source_plan) => {
            let schema = read_source_plan.schema();
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).data_type(), &DataType::String);
        }
        _ => panic!(
            "unexpected plan node, must be ReadDataSource, but got: {:?}",
            read_source_node
        ),
    }
    Ok(())
}
