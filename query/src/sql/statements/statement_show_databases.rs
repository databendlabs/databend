use sqlparser::ast::Expr;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use common_exception::Result;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowDatabases {
    pub where_opt: Option<Expr>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowDatabases {
    // #[tracing::instrument(level = "info", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let rewritten_query = self.rewritten_query();
        let rewritten_query_plan = PlanParser::parse(rewritten_query.as_str(), ctx);
        Ok(AnalyzedResult::SimpleQuery(rewritten_query_plan.await?))
    }
}

impl DfShowDatabases {
    fn show_all_databases() -> String {
        String::from("SELECT name AS Database FROM system.databases ORDER BY name")
    }

    fn show_databases_with_predicate(expr: &Expr) -> String {
        format!("SELECT name AS Database FROM system.databases WHERE {} ORDER BY name", expr)
    }

    fn rewritten_query(&self) -> String {
        match &self.where_opt {
            None => Self::show_all_databases(),
            Some(expr) => Self::show_databases_with_predicate(expr),
        }
    }
}
