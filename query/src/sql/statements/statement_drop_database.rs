use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::DropDatabasePlan;
use common_planners::PlanNode;
use sqlparser::ast::ObjectName;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfDropDatabase {
    pub if_exists: bool,
    pub name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfDropDatabase {
    async fn analyze(&self, _ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let db = self.database_name()?;
        let if_exists = self.if_exists;

        Ok(AnalyzedResult::SimpleQuery(PlanNode::DropDatabase(
            DropDatabasePlan { db, if_exists },
        )))
    }
}

impl DfDropDatabase {
    fn database_name(&self) -> Result<String> {
        if self.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Create database name is empty"));
        }

        Ok(self.name.0[0].value.clone())
    }
}
