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

use common_base::tokio;
use common_exception::Result;
use databend_query::optimizers::Optimizer;
use databend_query::optimizers::ScattersOptimizer;
use databend_query::sql::PlanParser;

use crate::tests::create_query_context_with_cluster;
use crate::tests::ClusterDescriptor;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scatter_optimizer() -> Result<()> {
    struct Test {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests = vec![
        Test {
            name: "Scalar query",
            query: "SELECT 1",
            expect: "\
            Projection: 1:UInt8\
            \n  Expression: 1:UInt8 (Before Projection)\
            \n    ReadDataSource: scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
        },
        Test {
            name: "Small local table query",
            query: "SELECT number FROM numbers_local(100)",
            expect: "\
            Projection: number:UInt64\
            \n  ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100, read_bytes: 800, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
        },
        Test {
            name: "Small local table aggregate query with group by key",
            query: "SELECT SUM(number) FROM numbers_local(100) GROUP BY number % 3",
            expect: "\
            Projection: SUM(number):UInt64\
            \n  AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]\
            \n    AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]\
            \n      Expression: (number % 3):UInt8, number:UInt64 (Before GroupBy)\
            \n        ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100, read_bytes: 800, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
        },
        Test {
            name: "Small local table aggregate query with group by keys",
            query: "SELECT SUM(number) FROM numbers_local(100) GROUP BY number % 3, number % 2",
            expect: "\
            Projection: SUM(number):UInt64\
            \n  AggregatorFinal: groupBy=[[(number % 3), (number % 2)]], aggr=[[SUM(number)]]\
            \n    AggregatorPartial: groupBy=[[(number % 3), (number % 2)]], aggr=[[SUM(number)]]\
            \n      Expression: (number % 3):UInt8, (number % 2):UInt8, number:UInt64 (Before GroupBy)\
            \n        ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100, read_bytes: 800, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
        },
        Test {
            name: "Small local table aggregate query without group by",
            query: "SELECT SUM(number) FROM numbers_local(100)",
            expect: "\
            Projection: SUM(number):UInt64\
            \n  AggregatorFinal: groupBy=[[]], aggr=[[SUM(number)]]\
            \n    AggregatorPartial: groupBy=[[]], aggr=[[SUM(number)]]\
            \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100, read_bytes: 800, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]",
        },
        Test {
            name: "Large local table query",
            query: "SELECT number FROM numbers_local(100000000)",
            expect: "\
            Projection: number:UInt64\
            \n  ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100000000, read_bytes: 800000000, partitions_scanned: 10001, partitions_total: 10001], push_downs: [projections: [0]]",
        },
        Test {
            name: "Large local table aggregate query with group by key",
            query: "SELECT SUM(number) FROM numbers_local(100000000) GROUP BY number % 3",
            expect: "\
            Projection: SUM(number):UInt64\
            \n  AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]\
            \n    AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]\
            \n      Expression: (number % 3):UInt8, number:UInt64 (Before GroupBy)\
            \n        ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100000000, read_bytes: 800000000, partitions_scanned: 10001, partitions_total: 10001], push_downs: [projections: [0]]",
        },
        Test {
            name: "Large local table aggregate query with group by keys",
            query: "SELECT SUM(number) FROM numbers_local(100000000) GROUP BY number % 3, number % 2",
            expect: "\
            Projection: SUM(number):UInt64\
            \n  AggregatorFinal: groupBy=[[(number % 3), (number % 2)]], aggr=[[SUM(number)]]\
            \n    AggregatorPartial: groupBy=[[(number % 3), (number % 2)]], aggr=[[SUM(number)]]\
            \n      Expression: (number % 3):UInt8, (number % 2):UInt8, number:UInt64 (Before GroupBy)\
            \n        ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100000000, read_bytes: 800000000, partitions_scanned: 10001, partitions_total: 10001], push_downs: [projections: [0]]",
        },
        Test {
            name: "Large local table aggregate query without group by",
            query: "SELECT SUM(number) FROM numbers_local(100000000)",
            expect: "\
            Projection: SUM(number):UInt64\
            \n  AggregatorFinal: groupBy=[[]], aggr=[[SUM(number)]]\
            \n    AggregatorPartial: groupBy=[[]], aggr=[[SUM(number)]]\
            \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100000000, read_bytes: 800000000, partitions_scanned: 10001, partitions_total: 10001], push_downs: [projections: [0]]",
        },
        Test {
            name: "Large cluster table query",
            query: "SELECT number FROM numbers(100000000)",
            expect: "\
            RedistributeStage[expr: 0]\
            \n  Projection: number:UInt64\
            \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100000000, read_bytes: 800000000, partitions_scanned: 10001, partitions_total: 10001], push_downs: [projections: [0]]",
        },
        Test {
            name: "Large cluster table aggregate query with group by key",
            query: "SELECT SUM(number) FROM numbers(100000000) GROUP BY number % 3",
            expect: "\
            RedistributeStage[expr: 0]\
            \n  Projection: SUM(number):UInt64\
            \n    AggregatorFinal: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]\
            \n      RedistributeStage[expr: sipHash(_group_by_key)]\
            \n        AggregatorPartial: groupBy=[[(number % 3)]], aggr=[[SUM(number)]]\
            \n          Expression: (number % 3):UInt8, number:UInt64 (Before GroupBy)\
            \n            ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100000000, read_bytes: 800000000, partitions_scanned: 10001, partitions_total: 10001], push_downs: [projections: [0]]",
        },
        Test {
            name: "Large cluster table aggregate query with group by keys",
            query: "SELECT SUM(number) FROM numbers(100000000) GROUP BY number % 3, number % 2",
            expect: "\
            RedistributeStage[expr: 0]\
            \n  Projection: SUM(number):UInt64\
            \n    AggregatorFinal: groupBy=[[(number % 3), (number % 2)]], aggr=[[SUM(number)]]\
            \n      RedistributeStage[expr: sipHash(_group_by_key)]\
            \n        AggregatorPartial: groupBy=[[(number % 3), (number % 2)]], aggr=[[SUM(number)]]\
            \n          Expression: (number % 3):UInt8, (number % 2):UInt8, number:UInt64 (Before GroupBy)\
            \n            ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100000000, read_bytes: 800000000, partitions_scanned: 10001, partitions_total: 10001], push_downs: [projections: [0]]",
        },
        Test {
            name: "Large cluster table aggregate query without group by",
            query: "SELECT SUM(number) FROM numbers(100000000)",
            expect: "\
            Projection: SUM(number):UInt64\
            \n  AggregatorFinal: groupBy=[[]], aggr=[[SUM(number)]]\
            \n    RedistributeStage[expr: 0]\
            \n      AggregatorPartial: groupBy=[[]], aggr=[[SUM(number)]]\
            \n        ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 100000000, read_bytes: 800000000, partitions_scanned: 10001, partitions_total: 10001], push_downs: [projections: [0]]",
        },
        Test {
            name: "Standalone query with standalone subquery",
            query: "SELECT * FROM numbers_local(1) WHERE EXISTS(SELECT * FROM numbers_local(1))",
            expect: "\
            Projection: number:UInt64\
            \n  Filter: exists(subquery(_subquery_1))\
            \n    Create sub queries sets: [_subquery_1]\
            \n      Projection: number:UInt64\
            \n        ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1, read_bytes: 8, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]\
            \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1, read_bytes: 8, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [exists(subquery(_subquery_1))]]",
        },
        Test {
            name: "Standalone query with cluster subquery",
            query: "SELECT * FROM numbers_local(1) WHERE EXISTS(SELECT * FROM numbers(1))",
            expect: "Projection: number:UInt64\
            \n  Filter: exists(subquery(_subquery_1))\
            \n    Create sub queries sets: [_subquery_1]\
            \n      RedistributeStage[expr: 0]\
            \n        Projection: number:UInt64\
            \n          ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1, read_bytes: 8, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]\
            \n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1, read_bytes: 8, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [exists(subquery(_subquery_1))]]",
        },
        Test {
            name: "Cluster query with standalone subquery",
            query: "SELECT * FROM numbers(1) WHERE EXISTS(SELECT * FROM numbers_local(1))",
            expect: "\
            RedistributeStage[expr: 0]\
            \n  Projection: number:UInt64\
            \n    Filter: exists(subquery(_subquery_1))\
            \n      Create sub queries sets: [_subquery_1]\
            \n        Broadcast in cluster\
            \n          Projection: number:UInt64\
            \n            ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1, read_bytes: 8, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]\
            \n        ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1, read_bytes: 8, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [exists(subquery(_subquery_1))]]",
        },
        Test {
            name: "Cluster query with cluster subquery",
            query: "SELECT * FROM numbers(1) WHERE EXISTS(SELECT * FROM numbers(1))",
            expect: "\
            RedistributeStage[expr: 0]\
            \n  Projection: number:UInt64\
            \n    Filter: exists(subquery(_subquery_1))\
            \n      Create sub queries sets: [_subquery_1]\
            \n        Broadcast in cluster\
            \n          Projection: number:UInt64\
            \n            ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1, read_bytes: 8, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0]]\
            \n        ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 1, read_bytes: 8, partitions_scanned: 1, partitions_total: 1], push_downs: [projections: [0], filters: [exists(subquery(_subquery_1))]]",
        },
    ];

    for test in tests {
        let ctx = create_query_context_with_cluster(
            ClusterDescriptor::new()
                .with_node("Github", "www.github.com:9090")
                .with_node("dummy_local", "127.0.0.1:9090")
                .with_local_id("dummy_local"),
        )?;

        let plan = PlanParser::parse(ctx.clone(), test.query).await?;
        let mut optimizer = ScattersOptimizer::create(ctx);
        let optimized = optimizer.optimize(&plan)?;
        let actual = format!("{:?}", optimized);
        assert_eq!(test.expect, actual, "{:#?}", test.name);
    }

    Ok(())
}
