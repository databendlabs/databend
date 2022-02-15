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
use common_exception::ErrorCode;
use common_exception::Result;
use databend_query::sql::statements::AnalyzableStatement;
use databend_query::sql::statements::AnalyzedResult;
use databend_query::sql::DfParser;
use databend_query::sql::DfStatement;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_statement_select_analyze() -> Result<()> {
    struct TestCase {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests = vec![
        TestCase {
            name: "Simple query",
            query: "SELECT 1",
            expect: "QueryAnalyzeState { before_projection: [1], projection: [1] }",
        },
        TestCase {
            name: "Simple filter query",
            query: "SELECT * FROM system.databases WHERE name = 'xxx'",
            expect: "QueryAnalyzeState { filter: (name = xxx), before_projection: [name], projection: [name] }",
        },
        TestCase {
            name: "Simple filter query between",
            query: "SELECT * FROM system.databases WHERE name = 'xxx' AND (name between 'aaa' and 'bbb')",
            expect: "QueryAnalyzeState { filter: ((name = xxx) AND ((name >= aaa) and (name <= bbb))), before_projection: [name], projection: [name] }",
        },
        TestCase {
            name: "Simple having query",
            query: "SELECT * FROM system.databases HAVING name = 'xxx'",
            expect: "QueryAnalyzeState { before_projection: [name], having: (name = xxx), projection: [name] }",
        },
        TestCase {
            name: "Simple order by query",
            query: "SELECT * FROM system.databases ORDER BY name",
            expect: "QueryAnalyzeState { before_order_by: [name], order_by: [name], projection: [name] }",
        },
        TestCase {
            name: "Simple order by query 2",
            query: "SELECT * FROM system.databases ORDER BY name = 'xxx'",
            expect: "QueryAnalyzeState { before_order_by: [name, (name = xxx)], order_by: [(name = 'xxx')], projection: [name] }",
        },
        TestCase {
            name: "Group by query with filter",
            query: "SELECT number % 2 AS number FROM numbers(10) WHERE number > 2 GROUP BY number",
            expect: "QueryAnalyzeState { filter: (number > 2), before_group_by: [(number % 2)], aggregator: [(number % 2)], before_projection: [(number % 2)], projection: [(number % 2) as number] }",
        },
        TestCase {
            name: "Group by query with aggregate",
            query: "SELECT number % 2 AS number, COUNT() as count FROM numbers(10) GROUP BY number",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], aggregate: [COUNT()], before_projection: [(number % 2), COUNT()], projection: [(number % 2) as number, COUNT() as count] }",
        },
        TestCase {
            name: "Group by query with having",
            query: "SELECT number % 2 AS number FROM numbers(10) GROUP BY number HAVING number > 10",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], before_projection: [(number % 2)], having: ((number % 2) > 10), projection: [(number % 2) as number] }",
        },
        TestCase {
            name: "Group by query with having 2",
            query: "SELECT number % 2 AS number FROM numbers(10) GROUP BY number HAVING number % 2 > 10",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], before_projection: [(number % 2)], having: (((number % 2) % 2) > 10), projection: [(number % 2) as number] }",
        },
        TestCase {
            name: "Group by query with having 3",
            query: "SELECT number % 2 AS number FROM numbers(10) GROUP BY number HAVING COUNT() > 2",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], aggregate: [COUNT()], before_projection: [(number % 2)], having: (COUNT() > 2), projection: [(number % 2) as number] }",
        },
        TestCase {
            name: "Group by query with order",
            query: "SELECT number % 2 AS number FROM numbers(10) GROUP BY number ORDER BY number",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], before_order_by: [(number % 2)], order_by: [(number % 2)], projection: [(number % 2) as number] }",
        },
        TestCase {
            name: "Group by query with having 2",
            query: "SELECT number % 2 AS number FROM numbers(10) GROUP BY number ORDER BY number % 3",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], before_order_by: [(number % 2), ((number % 2) % 3)], order_by: [((number % 2) % 3)], projection: [(number % 2) as number] }",
        },
        TestCase {
            name: "Group by query with having 3",
            query: "SELECT number % 2 AS number FROM numbers(10) GROUP BY number ORDER BY COUNT()",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], aggregate: [COUNT()], before_order_by: [(number % 2), COUNT()], order_by: [COUNT()], projection: [(number % 2) as number] }",
        },
        TestCase {
            name: "Group by query with projection",
            query: "SELECT number % 2 AS number1 FROM numbers(10) GROUP BY number % 2",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], before_projection: [(number % 2)], projection: [(number % 2) as number1] }",
        },
        TestCase {
            name: "Group by query with projection 2",
            query: "SELECT number % 2 % 3 AS number1 FROM numbers(10) GROUP BY number % 2",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], before_projection: [((number % 2) % 3)], projection: [((number % 2) % 3) as number1] }",
        },
        TestCase {
            name: "Group by query with projection 3",
            query: "SELECT COUNT() AS count FROM numbers(10) GROUP BY number % 2",
            expect: "QueryAnalyzeState { before_group_by: [(number % 2)], aggregator: [(number % 2)], aggregate: [COUNT()], before_projection: [COUNT()], projection: [COUNT() as count] }",
        },
        TestCase {
            name: "Group by query with projection 4",
            query: "SELECT avg(number), max(number + 1) + 1 FROM numbers_mt(10000) GROUP BY 1;",
            expect: "QueryAnalyzeState { before_group_by: [1, number, (number + 1)], aggregator: [1], aggregate: [avg(number), max((number + 1))], before_projection: [avg(number), (max((number + 1)) + 1)], projection: [avg(number), (max((number + 1)) + 1)] }",
        },
    ];

    for test_case in &tests {
        let ctx = create_query_context()?;
        let (mut statements, _) = DfParser::parse_sql(test_case.query)?;

        match statements.remove(0) {
            DfStatement::Query(query) => {
                match query.analyze(ctx).await? {
                    AnalyzedResult::SelectQuery(state) => {
                        assert_eq!(
                            test_case.expect,
                            format!("{:?}", state),
                            "{:#?}",
                            test_case.name
                        );
                    }
                    _ => {
                        return Err(ErrorCode::LogicalError(
                            "Query analyzed must be return QueryAnalyzeState",
                        ));
                    }
                };
            }
            _ => {
                return Err(ErrorCode::LogicalError("Cannot get analyze query state."));
            }
        }
    }

    Ok(())
}
