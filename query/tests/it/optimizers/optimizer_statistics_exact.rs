// Copyright 2021 Datafuse Labs.
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

use common_base::base::tokio;
use common_datavalues::*;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::*;
use databend_query::optimizers::*;
use pretty_assertions::assert_eq;

use crate::optimizers::optimizer::*;

#[tokio::test]
async fn test_statistics_exact_optimizer() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;

    let total = ctx.get_settings().get_max_block_size()? as u64;
    let statistics = Statistics::new_exact(
        total as usize,
        ((total) * size_of::<u64>() as u64) as usize,
        total as usize,
        total as usize,
    );
    ctx.try_set_statistics(&statistics)?;
    let source_plan = PlanNode::ReadSource(ReadDataSourcePlan {
        source_info: SourceInfo::TableSource(TableInfo::simple(
            "system",
            "test",
            DataSchemaRefExt::create(vec![
                DataField::new("a", Vu8::to_data_type()),
                DataField::new("b", Vu8::to_data_type()),
                DataField::new("c", Vu8::to_data_type()),
            ]),
        )),
        scan_fields: None,
        parts: generate_partitions(8, total as u64),
        statistics: statistics.clone(),
        description: format!(
            "(Read from system.{} table, Exactly Read Rows:{}, Read Bytes:{})",
            "test", statistics.read_rows, statistics.read_bytes
        ),
        tbl_args: None,
        push_downs: None,
    });

    let aggr_expr = Expression::AggregateFunction {
        op: "count".to_string(),
        distinct: false,
        params: vec![],
        args: vec![],
    };

    let plan = PlanBuilder::from(&source_plan)
        .aggregate_partial(&[aggr_expr.clone()], &[])?
        .aggregate_final(source_plan.schema(), &[aggr_expr], &[])?
        .project(&[Expression::Column("count()".to_string())])?
        .build()?;

    let mut statistics_exact = StatisticsExactOptimizer::create(ctx);
    let optimized = statistics_exact.optimize(&plan)?;

    let expect = "\
        Projection: count():UInt64\
        \n  Projection: 10000 as count():UInt64\
        \n    Expression: 10000:UInt64 (Exact Statistics)\
        \n      ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1]";
    let actual = format!("{:?}", optimized);
    assert_eq!(expect, actual);
    Ok(())
}
