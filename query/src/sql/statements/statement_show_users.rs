use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use common_exception::Result;
use crate::sql::PlanParser;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowUsers;

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowUsers {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let rewritten_query = "SELECT * FROM system.users ORDER BY name";
        let rewritten_query_plan = PlanParser::parse(rewritten_query, ctx);
        Ok(AnalyzedResult::SimpleQuery(rewritten_query_plan.await?))
    }
}
