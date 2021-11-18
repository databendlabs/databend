use common_base::tokio;
use common_exception::{ErrorCode, Result};
use crate::sql::{DfParser, DfStatement};
use crate::tests::try_create_context;
use crate::sql::statements::query::from_analyzer::FromAnalyzer;

#[tokio::test]
async fn test_from_analyzer() -> Result<()> {
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
        // TODO:
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
                let from_analyzer = FromAnalyzer::create(ctx);
                assert_eq!(test_case.expect, format!("{:?}", from_analyzer.analyze(&query).await?), "{:#?}", test_case.name)
            }
            _ => {
                return Err(ErrorCode::LogicalError("Cannot get analyze query state."));
            }
        }
    }

    Ok(())
}
