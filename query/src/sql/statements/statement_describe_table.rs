use sqlparser::ast::ObjectName;
use crate::sql::statements::{AnalyzableStatement, AnalyzedResult};
use crate::sessions::DatabendQueryContextRef;
use common_exception::{Result, ErrorCode};
use common_datavalues::{DataSchemaRefExt, DataField, DataType, DataSchemaRef};
use common_planners::{PlanNode, DescribeTablePlan};

#[derive(Debug, Clone, PartialEq)]
pub struct DfDescribeTable {
    pub name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfDescribeTable {
    async fn analyze(self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let schema = Self::schema();
        let (db, table) = self.resolve_table(ctx)?;

        Ok(AnalyzedResult::SimpleQuery(
            PlanNode::DescribeTable(
                DescribeTablePlan { db, table, schema }
            )
        ))
    }
}

impl DfDescribeTable {
    fn resolve_table(&self, ctx: DatabendQueryContextRef) -> Result<(String, String)> {
        let DfDescribeTable { name: ObjectName(idents), .. } = self;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Desc table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException("Desc table name must be [`db`].`table`"))
        }
    }

    fn schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("Field", DataType::String, false),
            DataField::new("Type", DataType::String, false),
            DataField::new("Null", DataType::String, false),
        ])
    }
}
