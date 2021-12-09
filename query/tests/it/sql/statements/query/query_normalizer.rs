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
use databend_query::sql::statements::query::QueryNormalizer;
use databend_query::sql::DfParser;
use databend_query::sql::DfStatement;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_query_normalizer() -> Result<()> {
    struct TestCase {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests = vec![
        TestCase {
            name: "Simple query",
            query: "SELECT 1",
            expect: "NormalQuery { projection: [1] }",
        },
        TestCase {
            name: "Simple with aggr query",
            query: "SELECT SUM(number) FROM numbers(100)",
            expect: "NormalQuery { aggregate: [SUM(number)], projection: [SUM(number)] }",
        },
        TestCase {
            name: "Filter query",
            query: "SELECT * FROM numbers(100) WHERE number = 3",
            expect: "NormalQuery { filter: (number = 3), projection: [*] }",
        },
        TestCase {
            name: "Group column query",
            query: "SELECT number FROM numbers(100) GROUP BY number",
            expect: "NormalQuery { group by: [number], projection: [number] }",
        },
        TestCase {
            name: "Group alias query",
            query: "SELECT number + 1 AS number FROM numbers(100) GROUP BY number",
            expect: "NormalQuery { group by: [(number + 1)], projection: [(number + 1) as number] }",
        },
        TestCase {
            name: "Having column without group query",
            query: "SELECT number FROM numbers(100) HAVING number = 3",
            expect: "NormalQuery { having: (number = 3), projection: [number] }",
        },
        TestCase {
            name: "Having alias without group query",
            query: "SELECT number + 1 AS number FROM numbers(100) HAVING number = 3",
            expect: "NormalQuery { having: ((number + 1) = 3), projection: [(number + 1) as number] }",
        },
        TestCase {
            name: "Having column with group query and without aggr",
            query: "SELECT number FROM numbers(100) GROUP BY number HAVING number = 3",
            expect: "NormalQuery { group by: [number], having: (number = 3), projection: [number] }",
        },
        TestCase {
            name: "Having alias with group query and without aggr",
            query: "SELECT number + 1 AS number FROM numbers(100) GROUP BY number HAVING number = 3",
            expect: "NormalQuery { group by: [(number + 1)], having: ((number + 1) = 3), projection: [(number + 1) as number] }",
        },
        TestCase {
            name: "Having aggr column with group query and with aggr",
            query: "SELECT number AS number1 FROM numbers(100) GROUP BY number HAVING SUM(number1) = 3",
            expect: "NormalQuery { group by: [number], having: (SUM(number) = 3), aggregate: [SUM(number)], projection: [number as number1] }",
        },
        TestCase {
            name: "Having aggr alias with group query and with aggr",
            query: "SELECT SUM(number) AS number1 FROM numbers(100) GROUP BY number HAVING number1 = 3",
            expect: "NormalQuery { group by: [number], having: (SUM(number) = 3), aggregate: [SUM(number)], projection: [SUM(number) as number1] }",
        },
        TestCase {
            name: "Order column without group query",
            query: "SELECT number FROM numbers(100) ORDER BY number",
            expect: "NormalQuery { order by: [number], projection: [number] }",
        },
        TestCase {
            name: "Order alias without group query",
            query: "SELECT number + 1 AS number FROM numbers(100) ORDER BY number",
            expect: "NormalQuery { order by: [(number + 1)], projection: [(number + 1) as number] }",
        },
        TestCase {
            name: "Order column with group query and without aggr",
            query: "SELECT number FROM numbers(100) GROUP BY number ORDER BY number",
            expect: "NormalQuery { group by: [number], order by: [number], projection: [number] }",
        },
        TestCase {
            name: "Order alias with group query and without aggr",
            query: "SELECT number + 1 AS number FROM numbers(100) GROUP BY number ORDER BY number",
            expect: "NormalQuery { group by: [(number + 1)], order by: [(number + 1)], projection: [(number + 1) as number] }",
        },
        TestCase {
            name: "Order aggr column with group query and with aggr",
            query: "SELECT number AS number1 FROM numbers(100) GROUP BY number ORDER BY SUM(number1)",
            expect: "NormalQuery { group by: [number], aggregate: [SUM(number)], order by: [SUM(number)], projection: [number as number1] }",
        },
        TestCase {
            name: "Order aggr alias with group query and with aggr",
            query: "SELECT SUM(number) AS number1 FROM numbers(100) GROUP BY number ORDER BY number1",
            expect: "NormalQuery { group by: [number], aggregate: [SUM(number)], order by: [SUM(number)], projection: [SUM(number) as number1] }",
        },
    ];

    for test_case in &tests {
        let ctx = create_query_context()?;
        let (mut statements, _) = DfParser::parse_sql(test_case.query)?;

        match statements.remove(0) {
            DfStatement::Query(query) => {
                let ir = QueryNormalizer::normalize(ctx, &query).await?;
                assert_eq!(
                    test_case.expect,
                    format!("{:?}", ir),
                    "{:#?}",
                    test_case.name
                );
            }
            _ => {
                return Err(ErrorCode::LogicalError("Cannot get analyze query state."));
            }
        }
    }

    Ok(())
}
