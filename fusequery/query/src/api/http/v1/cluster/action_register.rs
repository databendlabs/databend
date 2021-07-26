use crate::sessions::SessionManagerRef;
use warp::Reply;
use warp::reply::Response;

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
}

impl Reply for RegisterAction {
    fn into_response(self) -> Response {
        use warp::http::*;
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

