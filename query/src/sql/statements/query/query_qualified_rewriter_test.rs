use common_base::tokio;
use common_exception::{ErrorCode, Result};
use crate::sql::{DfParser, DfStatement};
use crate::sql::statements::query::{JoinedSchemaAnalyzer, QualifiedRewriter, QueryNormalizer};
use crate::tests::try_create_context;

#[tokio::test]
async fn test_query_qualified_rewriter() -> Result<()> {
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
            name: "Alias query",
            query: "SELECT alias.name FROM system.databases AS alias",
            expect: "NormalQuery { projection: [name] }",
        },
        TestCase {
            name: "Database and table query",
            query: "SELECT system.databases.name FROM system.databases",
            expect: "NormalQuery { projection: [name] }",
        },
        TestCase {
            name: "Alias query with filter",
            query: "SELECT name FROM system.databases AS alias WHERE alias.name = 'XXX'",
            expect: "NormalQuery { filter: =(name, XXX), projection: [name] }",
        },
        TestCase {
            name: "Database and table query with filter",
            query: "SELECT name FROM system.databases WHERE system.databases.name = 'XXX'",
            expect: "NormalQuery { filter: =(name, XXX), projection: [name] }",
        },
        TestCase {
            name: "Alias query with group",
            query: "SELECT name FROM system.databases AS alias GROUP BY alias.name",
            expect: "NormalQuery { group by: [name], projection: [name] }",
        },
        TestCase {
            name: "Database and table query with group",
            query: "SELECT name FROM system.databases GROUP BY system.databases.name",
            expect: "NormalQuery { group by: [name], projection: [name] }",
        },
        TestCase {
            name: "Alias query with having",
            query: "SELECT name FROM system.databases AS alias HAVING alias.name = 'xxx'",
            expect: "NormalQuery { having: =(name, xxx), projection: [name] }",
        },
        TestCase {
            name: "Database and table query with having",
            query: "SELECT name FROM system.databases HAVING system.databases.name = 'xxx'",
            expect: "NormalQuery { having: =(name, xxx), projection: [name] }",
        },
        TestCase {
            name: "Alias query with order",
            query: "SELECT name FROM system.databases AS alias ORDER BY alias.name",
            expect: "NormalQuery { order by: [name], projection: [name] }",
        },
        TestCase {
            name: "Database and table query with order",
            query: "SELECT name FROM system.databases ORDER BY system.databases.name",
            expect: "NormalQuery { order by: [name], projection: [name] }",
        },
        TestCase {
            name: "Alias query with aggregate",
            query: "SELECT COUNT(alias.name) AS name FROM system.databases AS alias WHERE name = 'xxx'",
            expect: "NormalQuery { filter: =(name, xxx), aggregate: [COUNT(name)], projection: [COUNT(name) as name] }",
        },
        TestCase {
            name: "Database and table query with aggregate",
            query: "SELECT COUNT(system.databases.name) AS name FROM system.databases WHERE system.databases.name = 'xxx'",
            expect: "NormalQuery { filter: =(name, xxx), aggregate: [COUNT(name)], projection: [COUNT(name) as name] }",
        },
    ];

    for test_case in &tests {
        let ctx = try_create_context()?;
        let (mut statements, _) = DfParser::parse_sql(test_case.query)?;

        match statements.remove(0) {
            DfStatement::Query(query) => {
                let analyzer = JoinedSchemaAnalyzer::create(ctx.clone());
                let joined_schema = analyzer.analyze(&query).await?;

                let transform = QueryNormalizer::create(ctx.clone());
                let data = transform.transform(&query).await?;

                let rewriter = QualifiedRewriter::create(joined_schema, ctx);
                assert_eq!(test_case.expect, format!("{:?}", rewriter.rewrite(data).await?), "{:#?}", test_case.name)
            }
            _ => {
                return Err(ErrorCode::LogicalError("Cannot get analyze query state."));
            }
        }
    }

    Ok(())
}

