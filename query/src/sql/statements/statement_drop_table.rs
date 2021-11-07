use sqlparser::ast::ObjectName;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use common_exception::{Result, ErrorCode};
use common_planners::{PlanNode, DropTablePlan};

#[derive(Debug, Clone, PartialEq)]
pub struct DfDropTable {
    pub if_exists: bool,
    pub name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfDropTable {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let if_exists = self.if_exists;
        let (db, table) = self.resolve_table(ctx)?;

        Ok(AnalyzedResult::SimpleQuery(
            PlanNode::DropTable(
                DropTablePlan { if_exists, db, table }
            )
        ))
    }
}

impl DfDropTable {
    fn resolve_table(&self, ctx: DatabendQueryContextRef) -> Result<(String, String)> {
        let DfDropTable { name: ObjectName(idents), .. } = self;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Drop table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException("Drop table name must be [`db`].`table`"))
        }
    }
}

