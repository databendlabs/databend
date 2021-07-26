use crate::sessions::SessionManagerRef;
use warp::Reply;
use warp::reply::Response;
use common_exception::Result;
use warp::hyper::Body;
use crate::api::http::v1::responses::{ErrorCodeResponseHelper, JSONResponseHelper};
use std::sync::Arc;
use common_management::cluster::ClusterExecutor;

pub struct ListAction {
    sessions: SessionManagerRef,
}

impl ListAction {
    pub fn create(sessions: SessionManagerRef) -> ListAction {
        ListAction { sessions }
    }
}

impl Reply for ListAction {
    fn into_response(self) -> Response {
        match self.sessions.try_get_executors() {
            Err(error) => error.into_response(),
            Ok(executors) => executors.into_json_response()
        }
    }
}

