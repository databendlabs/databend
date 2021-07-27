use crate::sessions::SessionManagerRef;
use warp::Reply;
use warp::reply::Response;
use common_exception::Result;
use crate::api::http::v1::responses::{ErrorCodeResponseHelper, StatusCodeResponseHelper};
use warp::http::StatusCode;

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
        Ok(String::from("Successfully registered the cluster executor."))
    }
}

impl Reply for CreateAction {
    fn into_response(self) -> Response {
        match self.register_cluster_executor() {
            Err(error) => error.into_response(),
            Ok(message) => StatusCode::CREATED.into_with_body_response(message),
        }
    }
}

