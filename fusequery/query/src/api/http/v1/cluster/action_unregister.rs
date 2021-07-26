use crate::sessions::SessionManagerRef;
use warp::Reply;
use warp::reply::Response;
use common_exception::Result;
use crate::api::http::v1::responses::{ErrorCodeResponseHelper, StatusCodeResponseHelper};
use warp::http::StatusCode;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct NodeIdentifier {
    name: String,
}

pub struct UnregisterAction {
    name: NodeIdentifier,
    sessions: SessionManagerRef,
}

impl UnregisterAction {
    pub fn create(name: NodeIdentifier, sessions: SessionManagerRef) -> UnregisterAction {
        UnregisterAction { name, sessions }
    }

    fn unregister_cluster_executor(&self) -> Result<String> {
        self.sessions.unregister_executor()?;
        Ok(String::from("Successfully unregistered the cluster executor."))
    }
}

impl Reply for UnregisterAction {
    fn into_response(self) -> Response {
        match self.unregister_cluster_executor() {
            Err(error) => error.into_response(),
            Ok(message) => StatusCode::OK.into_with_body_response(message),
        }
    }
}

