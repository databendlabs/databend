use crate::sessions::SessionManagerRef;
use warp::Reply;
use warp::reply::Response;
use common_exception::Result;
use warp::hyper::Body;
use crate::api::http::v1::responses::{ErrorCodeResponseHelper, JSONResponseHelper};
use std::sync::Arc;
use common_management::cluster::ClusterExecutor;

pub struct GetAction {
    name: String,
    sessions: SessionManagerRef,
}

impl GetAction {
    pub fn create(name: String, sessions: SessionManagerRef) -> GetAction {
        GetAction { name, sessions }
    }
}

impl Reply for GetAction {
    fn into_response(self) -> Response {
        match self.sessions.try_get_executors() {
            Err(error) => error.into_response(),
            Ok(executors) => executors.into_json_response()
        }
    }
}

