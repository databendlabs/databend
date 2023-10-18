use common_ast::ast::ScheduleOptions;
use common_ast::ast::WarehouseOptions;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTaskPlan {
    pub if_not_exists: bool,
    pub tenant: String,
    pub task_name: String,
    pub warehouse_opts: WarehouseOptions,
    pub schedule_opts: ScheduleOptions,
    pub suspend_task_after_num_failures: Option<u64>,
    pub sql: String,
    pub comment: String,
}

impl CreateTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}
