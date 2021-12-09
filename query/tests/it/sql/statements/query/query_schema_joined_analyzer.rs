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
use databend_query::sql::statements::query::JoinedSchemaAnalyzer;
use databend_query::sql::DfParser;
use databend_query::sql::DfStatement;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_joined_schema_analyzer() -> Result<()> {
    struct TestCase {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }
    let tests = vec![
        TestCase {
            name: "Simple query",
            query: "SELECT 1",
            expect: "QuerySchema { short_names: [\"dummy\"] }",
        },
        TestCase {
            name: "Table query",
            query: "SELECT * FROM system.databases",
            expect: "QuerySchema { short_names: [\"name\"] }",
        },
        TestCase {
            name: "Subquery query",
            query: "SELECT * FROM (SELECT name FROM system.databases)",
            expect: "QuerySchema { short_names: [\"name\"] }",
        },
        TestCase {
            name: "Subquery query with wildcard",
            query: "SELECT * FROM (SELECT * FROM system.databases)",
            expect: "QuerySchema { short_names: [\"name\"] }",
        },
    ];

    for test_case in &tests {
        let ctx = create_query_context()?;
        let (mut statements, _) = DfParser::parse_sql(test_case.query)?;

        match statements.remove(0) {
            DfStatement::Query(query) => {
                let analyzer = JoinedSchemaAnalyzer::create(ctx);
                assert_eq!(
                    test_case.expect,
                    format!("{:?}", analyzer.analyze(&query).await?),
                    "{:#?}",
                    test_case.name
                )
            }
            _ => {
                return Err(ErrorCode::LogicalError("Cannot get analyze query state."));
            }
        }
    }

    Ok(())
}
