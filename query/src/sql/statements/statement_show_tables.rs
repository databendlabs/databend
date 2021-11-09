use sqlparser::ast::{Expr, Ident, ObjectName};
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use common_exception::Result;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub enum DfShowTables {
    All,
    Like(Ident),
    Where(Expr),
    FromOrIn(ObjectName),
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowTables {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let rewritten_query = self.rewritten_query(ctx.clone());
        let rewritten_query_plan = PlanParser::parse(rewritten_query.as_str(), ctx);
        Ok(AnalyzedResult::SimpleQuery(rewritten_query_plan.await?))
    }
}

impl DfShowTables {
    fn show_all_tables(&self, ctx: DatabendQueryContextRef) -> String {
        format!(
            "SELECT name FROM system.tables where database = '{}' ORDER BY database, name",
            ctx.get_current_database()
        )
    }

    fn show_tables_with_like(&self, i: &Ident, ctx: DatabendQueryContextRef) -> String {
        format!(
            "SELECT name FROM system.tables where database = '{}' AND name LIKE {} ORDER BY database, name",
            ctx.get_current_database(), i,
        )
    }

    fn show_tables_with_predicate(&self, e: &Expr, ctx: DatabendQueryContextRef) -> String {
        format!(
            "SELECT name FROM system.tables where database = '{}' AND ({}) ORDER BY database, name",
            ctx.get_current_database(), e,
        )
    }

    fn show_tables_from_db(name: &ObjectName) -> String {
        format!(
            "SELECT name FROM system.tables where database = '{}' ORDER BY database, name",
            name.0[0].value.clone()
        )
    }

    fn rewritten_query(&self, ctx: DatabendQueryContextRef) -> String {
        match self {
            DfShowTables::All => self.show_all_tables(ctx),
            DfShowTables::Like(i) => self.show_tables_with_like(i, ctx),
            DfShowTables::Where(e) => self.show_tables_with_predicate(e, ctx),
            DfShowTables::FromOrIn(name) => DfShowTables::show_tables_from_db(name),
        }
    }
}

