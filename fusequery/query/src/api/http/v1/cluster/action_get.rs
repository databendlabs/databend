use std::sync::Arc;

use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use warp::hyper::Body;
use warp::reply::Response;
use warp::Reply;

use crate::api::http::v1::action::Action;
use crate::api::http::v1::responses::ErrorCodeResponseHelper;
use crate::api::http::v1::responses::JSONResponseHelper;
use crate::sessions::SessionManagerRef;

pub struct GetAction {
    name: String,
    sessions: SessionManagerRef,
}

impl GetAction {
    pub fn create(name: String, sessions: SessionManagerRef) -> GetAction {
        GetAction { name, sessions }
    }
}

#[async_trait::async_trait]
impl Action for GetAction {
    async fn do_action_impl(self) -> Response {
        let sessions = self.sessions;
        let cluster_manager = sessions.get_cluster_manager();
        let get_executor_by_name = cluster_manager.get_executor_by_name(self.name);

        match get_executor_by_name.await {
            Err(error) => error.into_response(),
            Ok(executors) => executors.into_json_response(),
        }
    }
}
