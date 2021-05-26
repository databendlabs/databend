use common_planners::ExpressionAction;
use common_planners::PlanNode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ExecutePlanWithShuffleAction {
    pub query_id: String,
    pub stage_id: String,
    pub plan: PlanNode,
    pub scatters: Vec<String>,
    pub scatters_action: ExpressionAction
}
