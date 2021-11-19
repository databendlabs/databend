use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::ShowCreateTablePlan;
use sqlparser::ast::ObjectName;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowCreateTable {
    pub name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowCreateTable {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let schema = Self::schema();
        let (db, table) = self.resolve_table(ctx)?;
        Ok(AnalyzedResult::SimpleQuery(PlanNode::ShowCreateTable(
            ShowCreateTablePlan { db, table, schema },
        )))
    }
}

impl DfShowCreateTable {
    fn schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("Table", DataType::String, false),
            DataField::new("Create Table", DataType::String, false),
        ])
    }

    fn resolve_table(&self, ctx: DatabendQueryContextRef) -> Result<(String, String)> {
        let DfShowCreateTable {
            name: ObjectName(idents),
        } = &self;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException(
                "Show create table name is empty",
            )),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "Show create table name must be [`db`].`table`",
            )),
        }
    }
}
