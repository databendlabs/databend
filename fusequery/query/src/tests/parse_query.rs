use common_exception::Result;
use common_planners::PlanNode;

use crate::sessions::FuseQueryContextRef;
use crate::sql::PlanParser;
use crate::tests::try_create_context;

pub fn parse_query(query: impl ToString) -> Result<PlanNode> {
    let context = try_create_context()?;
    PlanParser::create(context).build_from_sql(&query.to_string())
}
