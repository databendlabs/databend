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
        let rewritten_query = self.rewritten_query();
        let rewritten_query_plan = PlanParser::parse(rewritten_query.as_str(), ctx);
        Ok(AnalyzedResult::SimpleQuery(rewritten_query_plan.await?))
    }
}

impl DfShowTables {
    fn show_all_tables(self) -> String {
        format!(
            "SELECT name FROM system.tables where database = '{}' ORDER BY database, name",
            self.ctx.get_current_database()
        )
    }

    fn show_tables_with_like(self, i: Ident) -> String {
        format!(
            "SELECT name FROM system.tables where database = '{}' AND name LIKE {} ORDER BY database, name",
            self.ctx.get_current_database(), i,
        )
    }

    fn show_tables_with_predicate(self, e: Expr) -> String {
        format!(
            "SELECT name FROM system.tables where database = '{}' AND ({}) ORDER BY database, name",
            self.ctx.get_current_database(), e,
        )
    }

    fn show_tables_from_db(name: ObjectName) -> String {
        format!(
            "SELECT name FROM system.tables where database = '{}' ORDER BY database, name",
            name.0[0].value.clone()
        )
    }

    fn rewritten_query(self) -> String {
        match self {
            DfShowTables::All => self.show_all_tables(),
            DfShowTables::Like(i) => self.show_tables_with_like(i),
            DfShowTables::Where(e) => self.show_tables_with_predicate(e),
            DfShowTables::FromOrIn(name) => DfShowTables::show_tables_from_db(name),
        }
    }
}

