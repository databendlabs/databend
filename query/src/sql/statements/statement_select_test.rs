use common_base::tokio;
use common_exception::{Result, ErrorCode};
use crate::sql::DfParser;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult, QueryNormalizerData};
use crate::tests::try_create_context;
use crate::sql::statements::query::AnalyzeQuerySchema;

// #[tokio::test]
// async fn test_joined_schema_with_dummy() -> Result<()> {
//     let analyze_state = analyze_query("SELECT 1").await?;
//     let joined_schema = analyze_state.joined_schema;
//     assert_columns(&joined_schema, vec!["dummy"]);
//     Ok(())
// }
//
// #[tokio::test]
// async fn test_joined_schema_with_table() -> Result<()> {
//     let analyze_state = analyze_query("SELECT name FROM system.databases").await?;
//
//     let joined_schema = analyze_state.joined_schema;
//     assert_columns(&joined_schema, vec!["name"]);
//     assert_columns_with_fullname(&joined_schema, vec![vec!["system", "databases", "name"]]);
//     Ok(())
// }
//
// #[tokio::test]
// async fn test_joined_schema_with_table_alias() -> Result<()> {
//     let analyze_state = analyze_query("SELECT name FROM system.databases AS alias").await?;
//
//     let joined_schema = analyze_state.joined_schema;
//     assert_columns(&joined_schema, vec!["name"]);
//     assert_columns_with_fullname(&joined_schema, vec![vec!["alias", "name"]]);
//     Ok(())
// }
//
// // #[tokio::test]
// // async fn test_joined_schema_with_subquery() -> Result<()> {
// //     // TODO:
// //     let analyze_state = analyze_query("SELECT name FROM (SELECT name FROM system.databases)").await?;
// //
// //     let joined_schema = analyze_state.joined_schema;
// //     assert_columns(&joined_schema, vec!["name"]);
// //     Ok(())
// // }
//
// #[tokio::test]
// async fn test_select_with_filter() -> Result<()> {
//     match analyze_query("SELECT name FROM system.databases WHERE xx = 'xx'").await {
//         Ok(_) => assert!(false, "Must be return error when filter unknown column."),
//         Err(error) => assert_eq!("Unknown column xx. columns: [\"name\"] (while in analyze select filter).", error.message()),
//     }
//
//     let without_filter = analyze_query("SELECT name FROM system.databases").await?;
//     assert!(without_filter.filter_predicate.is_none());
//
//     let with_filter = analyze_query("SELECT name FROM system.databases WHERE name = 'xx'").await?;
//     assert!(with_filter.filter_predicate.is_some());
//
//     Ok(())
// }
//
// #[tokio::test]
// async fn test_select_with_group() -> Result<()> {
//     match analyze_query("SELECT COUNT() FROM system.databases GROUP BY xx").await {
//         Ok(_) => assert!(false, "Must be return error when group unknown column."),
//         Err(error) => assert_eq!("Unknown column xx. columns: [\"name\"] (while in analyze select group by).", error.message()),
//     }
//
//     let without_group = analyze_query("SELECT name FROM system.databases").await?;
//     assert!(without_group.group_by_expressions.is_empty());
//     assert!(without_group.aggregate_expressions.is_empty());
//
//     let without_group = analyze_query("SELECT COUNT() FROM system.databases").await?;
//     assert!(without_group.group_by_expressions.is_empty());
//     assert!(!without_group.aggregate_expressions.is_empty());
//
//     let with_group = analyze_query("SELECT name FROM system.databases GROUP BY name").await?;
//     assert!(!with_group.group_by_expressions.is_empty());
//     assert!(with_group.aggregate_expressions.is_empty());
//
//     let with_group = analyze_query("SELECT COUNT() FROM system.databases GROUP BY name").await?;
//     assert!(!with_group.group_by_expressions.is_empty());
//     assert!(!with_group.aggregate_expressions.is_empty());
//
//     Ok(())
// }
//
// #[tokio::test]
// async fn test_before_group_by_exprs() -> Result<()> {
//     let analyzed_query = analyze_query("SELECT 1").await?;
//     println!("{:?}", analyzed_query.before_group_by_expressions);
//     // assert!(!analyzed_query.before_group_by_expressions.is_empty());
//
//     let analyzed_query = analyze_query("SELECT COUNT() FROM system.databases").await?;
//     assert!(analyzed_query.before_group_by_expressions.is_empty());
//
//     Ok(())
// }
//
// fn assert_columns(schema: &AnalyzeQuerySchema, columns_name: Vec<&str>) {
//     for column_name in &columns_name {
//         assert!(schema.contains_column(column_name));
//     }
//
//     for field in schema.to_data_schema().fields() {
//         assert!(columns_name.contains(&field.name().as_str()));
//     }
// }
//
// fn assert_columns_with_fullname(schema: &AnalyzeQuerySchema, columns_fullname: Vec<Vec<&str>>) {
//     for column_fullname in &columns_fullname {
//         let fullname = column_fullname.iter().map(|s| s.to_string()).collect::<Vec<_>>();
//         assert!(schema.get_column_by_fullname(&fullname).is_some());
//     }
// }
//
// async fn analyze_query(query: &str) -> Result<AnalyzeQueryState> {
//     let ctx = try_create_context()?;
//     let (mut statements, _) = DfParser::parse_sql(query)?;
//
//     match statements.remove(0).analyze(ctx).await? {
//         AnalyzedResult::SelectQuery(data) => Ok(data),
//         _ => Err(ErrorCode::LogicalError("Cannot get analyze query state."))
//     }
// }
