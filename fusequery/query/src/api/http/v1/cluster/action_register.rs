use crate::sessions::SessionManagerRef;
use warp::Reply;
use warp::reply::Response;
use common_exception::Result;
use crate::api::http::v1::responses::{ErrorCodeResponseHelper, StatusCodeResponseHelper};
use warp::http::StatusCode;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct NodeInfo {}

pub struct RegisterAction {
    info: NodeInfo,
    sessions: SessionManagerRef,
}

impl RegisterAction {
    pub fn create(info: NodeInfo, sessions: SessionManagerRef) -> RegisterAction {
        RegisterAction { info, sessions }
    }

    fn register_cluster_executor(&self) -> Result<String> {
        self.sessions.register_executor()?;
        Ok(String::from("Successfully registered the cluster executor."))
    }
}

impl Reply for RegisterAction {
    fn into_response(self) -> Response {
        // TODO: maybe should change OK to CREATED?
        match self.register_cluster_executor() {
            Err(error) => error.into_response(),
            Ok(message) => StatusCode::OK.into_with_body_response(message),
        }
    }
}

