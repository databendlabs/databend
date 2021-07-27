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

pub struct RemoveAction {
    name: String,
    sessions: SessionManagerRef,
}

impl RemoveAction {
    pub fn create(name: String, sessions: SessionManagerRef) -> RemoveAction {
        RemoveAction { name, sessions }
    }

    fn unregister_cluster_executor(&self) -> Result<String> {
        self.sessions.unregister_executor()?;
        Ok(String::from("Successfully unregistered the cluster executor."))
    }
}

impl Reply for RemoveAction {
    fn into_response(self) -> Response {
        match self.unregister_cluster_executor() {
            Err(error) => error.into_response(),
            Ok(message) => StatusCode::ACCEPTED.into_with_body_response(message),
        }
    }
}

