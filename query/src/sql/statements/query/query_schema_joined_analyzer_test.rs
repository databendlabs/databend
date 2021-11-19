use common_base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::statements::query::query_schema_joined_analyzer::JoinedSchemaAnalyzer;
use crate::sql::DfParser;
use crate::sql::DfStatement;
use crate::tests::try_create_context;

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
        let ctx = try_create_context()?;
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
