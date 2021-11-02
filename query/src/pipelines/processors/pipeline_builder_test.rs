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

use common_base::tokio;
use common_exception::Result;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::pipelines::processors::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_local_pipeline_builds() -> Result<()> {
    struct Test {
        name: &'static str,
        query: &'static str,
        plan: &'static str,
        pipeline: &'static str,
        block: Vec<&'static str>,
    }

    let tests = vec![
        Test {
            name: "select-alias-pass",
            query: "select number as c1, number as c2 from numbers_mt(10) order by c1 desc",

            plan: "\
            Projection: number as c1:UInt64, number as c2:UInt64\
            \n  Sort: number:UInt64\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",

            pipeline: "\
            ProjectionTransform × 1 processor\
            \n  SortMergeTransform × 1 processor\
            \n    Merge (SortMergeTransform × 8 processors) to (SortMergeTransform × 1)\
            \n      SortMergeTransform × 8 processors\
            \n        SortPartialTransform × 8 processors\
            \n          SourceTransform × 8 processors",


            block: vec![
                "+----+----+",
                "| c1 | c2 |",
                "+----+----+",
                "| 9  | 9  |",
                "| 8  | 8  |",
                "| 7  | 7  |",
                "| 6  | 6  |",
                "| 5  | 5  |",
                "| 4  | 4  |",
                "| 3  | 3  |",
                "| 2  | 2  |",
                "| 1  | 1  |",
                "| 0  | 0  |",
                "+----+----+",
            ]
        },
        Test {
            name: "select-order-by-alias-pass",
            query: "select number as c1, number as c2 from numbers_mt(10) order by c1 desc, c2 asc",

            plan: "\
            Projection: number as c1:UInt64, number as c2:UInt64\
            \n  Sort: number:UInt64, number:UInt64\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",


            pipeline: "\
            ProjectionTransform × 1 processor\
            \n  SortMergeTransform × 1 processor\
            \n    Merge (SortMergeTransform × 8 processors) to (SortMergeTransform × 1)\
            \n      SortMergeTransform × 8 processors\
            \n        SortPartialTransform × 8 processors\
            \n          SourceTransform × 8 processors",

            block: vec![
                "+----+----+",
                "| c1 | c2 |",
                "+----+----+",
                "| 9  | 9  |",
                "| 8  | 8  |",
                "| 7  | 7  |",
                "| 6  | 6  |",
                "| 5  | 5  |",
                "| 4  | 4  |",
                "| 3  | 3  |",
                "| 2  | 2  |",
                "| 1  | 1  |",
                "| 0  | 0  |",
                "+----+----+",
            ]
        },
        Test {
            name: "select-order-by-alias-expression-pass",
            query:
                "select number as c1, (number + 1) as c2 from numbers_mt(10) order by c1 desc, c2 asc",

            plan: "\
            Projection: number as c1:UInt64, (number + 1) as c2:UInt64\
            \n  Sort: number:UInt64, (number + 1):UInt64\
            \n    Expression: number:UInt64, (number + 1):UInt64 (Before OrderBy)\
            \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80], push_downs: []",

            pipeline: "\
            ProjectionTransform × 1 processor\
            \n  SortMergeTransform × 1 processor\
            \n    Merge (SortMergeTransform × 8 processors) to (SortMergeTransform × 1)\
            \n      SortMergeTransform × 8 processors\
            \n        SortPartialTransform × 8 processors\
            \n          ExpressionTransform × 8 processors\
            \n            SourceTransform × 8 processors",

            block: vec![
                "+----+----+",
                "| c1 | c2 |",
                "+----+----+",
                "| 9  | 10 |",
                "| 8  | 9  |",
                "| 7  | 8  |",
                "| 6  | 7  |",
                "| 5  | 6  |",
                "| 4  | 5  |",
                "| 3  | 4  |",
                "| 2  | 3  |",
                "| 1  | 2  |",
                "| 0  | 1  |",
                "+----+----+",
            ]
        },
        Test {
            name: "select-order-by-limit-with-offset-pass",
            query:
                "select number from numbers_mt(100) order by number asc limit 5 offset 5",

            plan: "\
            Limit: 5, 5\
            \n  Projection: number:UInt64\
            \n    Sort: number:UInt64\
            \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 100, read_bytes: 800], push_downs: []",

            pipeline: "\
            LimitTransform × 1 processor\
            \n  ProjectionTransform × 1 processor\
            \n    SortMergeTransform × 1 processor\
            \n      Merge (SortMergeTransform × 8 processors) to (SortMergeTransform × 1)\
            \n        SortMergeTransform × 8 processors\
            \n          SortPartialTransform × 8 processors\
            \n            SourceTransform × 8 processors",

            block: vec![
                "+--------+",
                "| number |",
                "+--------+",
                "| 5      |",
                "| 6      |",
                "| 7      |",
                "| 8      |",
                "| 9      |",
                "+--------+",
            ]
        },
    ];

    let ctx = crate::tests::try_create_context()?;
    for test in tests {
        // Plan build check.
        let plan = PlanParser::create(ctx.clone()).build_from_sql(test.query)?;
        let actual_plan = format!("{:?}", plan);
        assert_eq!(test.plan, actual_plan, "{:#?}", test.name);

        // Pipeline build check.
        let pipeline_builder = PipelineBuilder::create(ctx.clone());
        let mut pipeline = pipeline_builder.build(&plan)?;
        let actual_pipeline = format!("{:?}", pipeline);
        assert_eq!(test.pipeline, actual_pipeline, "{:#?}", test.name);

        // Result check.
        let stream = pipeline.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        common_datablocks::assert_blocks_eq_with_name(test.name, test.block, result.as_slice());
    }
    Ok(())
}
