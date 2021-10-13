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
use pretty_assertions::assert_eq;

use crate::pipelines::processors::processor_dag::ProcessorsDAGBuilder;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processors_dag_builder() -> Result<()> {
    struct TestCase {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests = vec![
        TestCase {
            name: "Simple query",
            query: "SELECT * FROM numbers(1000)",
            expect: "digraph {\
            \n    0 [ label = \"SourceTransform\" ]\
            \n    1 [ label = \"SourceTransform\" ]\
            \n    2 [ label = \"SourceTransform\" ]\
            \n    3 [ label = \"SourceTransform\" ]\
            \n    4 [ label = \"SourceTransform\" ]\
            \n    5 [ label = \"SourceTransform\" ]\
            \n    6 [ label = \"SourceTransform\" ]\
            \n    7 [ label = \"SourceTransform\" ]\
            \n    8 [ label = \"ProjectionTransform\" ]\
            \n    9 [ label = \"ProjectionTransform\" ]\
            \n    10 [ label = \"ProjectionTransform\" ]\
            \n    11 [ label = \"ProjectionTransform\" ]\
            \n    12 [ label = \"ProjectionTransform\" ]\
            \n    13 [ label = \"ProjectionTransform\" ]\
            \n    14 [ label = \"ProjectionTransform\" ]\
            \n    15 [ label = \"ProjectionTransform\" ]\
            \n    0 -> 8 [ ]\
            \n    1 -> 9 [ ]\
            \n    2 -> 10 [ ]\
            \n    3 -> 11 [ ]\
            \n    4 -> 12 [ ]\
            \n    5 -> 13 [ ]\
            \n    6 -> 14 [ ]\
            \n    7 -> 15 [ ]\
            \n}\n",
        },
        TestCase {
            name: "Mixed graph query without order by",
            query: "SELECT AVG(number) c FROM numbers(100000) GROUP BY number % 1000 HAVING c > 100 LIMIT 1;",
            expect: "digraph {\
            \n    0 [ label = \"SourceTransform\" ]\
            \n    1 [ label = \"SourceTransform\" ]\
            \n    2 [ label = \"SourceTransform\" ]\
            \n    3 [ label = \"SourceTransform\" ]\
            \n    4 [ label = \"SourceTransform\" ]\
            \n    5 [ label = \"SourceTransform\" ]\
            \n    6 [ label = \"SourceTransform\" ]\
            \n    7 [ label = \"SourceTransform\" ]\
            \n    8 [ label = \"ExpressionTransform\" ]\
            \n    9 [ label = \"ExpressionTransform\" ]\
            \n    10 [ label = \"ExpressionTransform\" ]\
            \n    11 [ label = \"ExpressionTransform\" ]\
            \n    12 [ label = \"ExpressionTransform\" ]\
            \n    13 [ label = \"ExpressionTransform\" ]\
            \n    14 [ label = \"ExpressionTransform\" ]\
            \n    15 [ label = \"ExpressionTransform\" ]\
            \n    16 [ label = \"GroupByPartialTransform\" ]\
            \n    17 [ label = \"GroupByPartialTransform\" ]\
            \n    18 [ label = \"GroupByPartialTransform\" ]\
            \n    19 [ label = \"GroupByPartialTransform\" ]\
            \n    20 [ label = \"GroupByPartialTransform\" ]\
            \n    21 [ label = \"GroupByPartialTransform\" ]\
            \n    22 [ label = \"GroupByPartialTransform\" ]\
            \n    23 [ label = \"GroupByPartialTransform\" ]\
            \n    24 [ label = \"GroupByFinalTransform\" ]\
            \n    25 [ label = \"HavingTransform\" ]\
            \n    26 [ label = \"HavingTransform\" ]\
            \n    27 [ label = \"HavingTransform\" ]\
            \n    28 [ label = \"HavingTransform\" ]\
            \n    29 [ label = \"HavingTransform\" ]\
            \n    30 [ label = \"HavingTransform\" ]\
            \n    31 [ label = \"HavingTransform\" ]\
            \n    32 [ label = \"HavingTransform\" ]\
            \n    33 [ label = \"ProjectionTransform\" ]\
            \n    34 [ label = \"ProjectionTransform\" ]\
            \n    35 [ label = \"ProjectionTransform\" ]\
            \n    36 [ label = \"ProjectionTransform\" ]\
            \n    37 [ label = \"ProjectionTransform\" ]\
            \n    38 [ label = \"ProjectionTransform\" ]\
            \n    39 [ label = \"ProjectionTransform\" ]\
            \n    40 [ label = \"ProjectionTransform\" ]\
            \n    41 [ label = \"LimitTransform\" ]\
            \n    0 -> 8 [ ]\
            \n    1 -> 9 [ ]\
            \n    2 -> 10 [ ]\
            \n    3 -> 11 [ ]\
            \n    4 -> 12 [ ]\
            \n    5 -> 13 [ ]\
            \n    6 -> 14 [ ]\
            \n    7 -> 15 [ ]\
            \n    8 -> 16 [ ]\
            \n    9 -> 17 [ ]\
            \n    10 -> 18 [ ]\
            \n    11 -> 19 [ ]\
            \n    12 -> 20 [ ]\
            \n    13 -> 21 [ ]\
            \n    14 -> 22 [ ]\
            \n    15 -> 23 [ ]\
            \n    16 -> 24 [ ]\
            \n    17 -> 24 [ ]\
            \n    18 -> 24 [ ]\
            \n    19 -> 24 [ ]\
            \n    20 -> 24 [ ]\
            \n    21 -> 24 [ ]\
            \n    22 -> 24 [ ]\
            \n    23 -> 24 [ ]\
            \n    24 -> 25 [ ]\
            \n    24 -> 26 [ ]\
            \n    24 -> 27 [ ]\
            \n    24 -> 28 [ ]\
            \n    24 -> 29 [ ]\
            \n    24 -> 30 [ ]\
            \n    24 -> 31 [ ]\
            \n    24 -> 32 [ ]\
            \n    25 -> 33 [ ]\
            \n    26 -> 34 [ ]\
            \n    27 -> 35 [ ]\
            \n    28 -> 36 [ ]\
            \n    29 -> 37 [ ]\
            \n    30 -> 38 [ ]\
            \n    31 -> 39 [ ]\
            \n    32 -> 40 [ ]\
            \n    33 -> 41 [ ]\
            \n    34 -> 41 [ ]\
            \n    35 -> 41 [ ]\
            \n    36 -> 41 [ ]\
            \n    37 -> 41 [ ]\
            \n    38 -> 41 [ ]\
            \n    39 -> 41 [ ]\
            \n    40 -> 41 [ ]\
            \n}\n",
        }];

    for test in tests {
        let ctx = crate::tests::try_create_context()?;

        let query_plan = crate::tests::parse_query(test.query)?;
        let processors_graph_builder = ProcessorsDAGBuilder::create(ctx);

        let processors_graph = processors_graph_builder.build(&query_plan)?;
        assert_eq!(
            format!("{}", processors_graph.display_graphviz()),
            test.expect,
            "{:#?}",
            test.name
        );
    }

    Ok(())
}
