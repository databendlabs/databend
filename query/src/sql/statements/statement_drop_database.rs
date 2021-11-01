use sqlparser::ast::ObjectName;
use crate::sql::analyzer::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use common_exception::{Result, ErrorCode};
use common_planners::{PlanNode, DropDatabasePlan};

#[derive(Debug, Clone, PartialEq)]
pub struct DfDropDatabase {
    pub if_exists: bool,
    pub name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfDropDatabase {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let db = self.database_name()?;
        let if_exists = self.if_exists;

        Ok(AnalyzedResult::SimpleQuery(
            PlanNode::DropDatabase(
                DropDatabasePlan { db, if_exists }
            )
        ))
    }
}

impl DfDropDatabase {
    fn database_name(&self) -> Result<String> {
        if self.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Create database name is empty"));
        }

        self.name.0[0].value.clone()
    }
}

