use std::collections::HashMap;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::CreateDatabasePlan;
use common_planners::PlanNode;
use sqlparser::ast::ObjectName;
use sqlparser::ast::SqlOption;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateDatabase {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub options: Vec<SqlOption>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateDatabase {
    async fn analyze(&self, _ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let db = self.database_name()?;
        let options = self.database_options();
        let if_not_exists = self.if_not_exists;

        Ok(AnalyzedResult::SimpleQuery(PlanNode::CreateDatabase(
            CreateDatabasePlan {
                db,
                options,
                if_not_exists,
            },
        )))
    }
}

impl DfCreateDatabase {
    fn database_name(&self) -> Result<String> {
        if self.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Create database name is empty"));
        }

        Ok(self.name.0[0].value.clone())
    }

    fn database_options(&self) -> HashMap<String, String> {
        self.options
            .iter()
            .map(|option| (option.name.value.to_lowercase(), option.value.to_string()))
            .collect()
    }
}
