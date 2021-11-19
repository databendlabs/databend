use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::TruncateTablePlan;
use sqlparser::ast::ObjectName;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfTruncateTable {
    pub name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfTruncateTable {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let (db, table) = self.resolve_table(ctx)?;
        Ok(AnalyzedResult::SimpleQuery(PlanNode::TruncateTable(
            TruncateTablePlan { db, table },
        )))
    }
}

impl DfTruncateTable {
    fn resolve_table(&self, ctx: DatabendQueryContextRef) -> Result<(String, String)> {
        let DfTruncateTable {
            name: ObjectName(idents),
            ..
        } = self;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Truncate table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "Truncate table name must be [`db`].`table`",
            )),
        }
    }
}
