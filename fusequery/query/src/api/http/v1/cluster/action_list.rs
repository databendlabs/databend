use crate::sessions::SessionManagerRef;
use warp::Reply;
use warp::reply::Response;

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
        use warp::http::*;
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

