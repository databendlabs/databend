use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use warp::http::StatusCode;
use warp::reply::Response;
use warp::Reply;

use crate::api::http::v1::action::Action;
use crate::api::http::v1::responses::ErrorCodeResponseHelper;
use crate::api::http::v1::responses::StatusCodeResponseHelper;
use crate::sessions::SessionManagerRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct NodeInfo {}

pub struct CreateAction {
    info: NodeInfo,
    sessions: SessionManagerRef,
}

impl CreateAction {
    pub fn create(info: NodeInfo, sessions: SessionManagerRef) -> CreateAction {
        CreateAction { info, sessions }
    }

    fn register_cluster_executor(&self) -> Result<String> {
        self.sessions.register_executor()?;
        Ok(String::from(
            "Successfully registered the cluster executor.",
        ))
    }
}

#[async_trait::async_trait]
impl Action for CreateAction {
    async fn do_action_impl(self) -> Response {
        ClusterExecutor::create();
        self.sessions.get_cluster_manager().add_node()
        // match self.register_cluster_executor() {
        //     Err(error) => error.into_response(),
        //     Ok(message) => StatusCode::CREATED.into_with_body_response(message),
        // }
    }
}
