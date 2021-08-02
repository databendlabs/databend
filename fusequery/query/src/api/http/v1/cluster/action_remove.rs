use common_exception::Result;
use warp::http::StatusCode;
use warp::reply::Response;
use warp::Reply;

use crate::api::http::v1::action::Action;
use crate::api::http::v1::responses::ErrorCodeResponseHelper;
use crate::api::http::v1::responses::StatusCodeResponseHelper;
use crate::sessions::SessionManagerRef;

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
        Ok(String::from(
            "Successfully unregistered the cluster executor.",
        ))
    }
}

#[async_trait::async_trait]
impl Action for RemoveAction {
    async fn do_action_impl(self) -> Response {
        unimplemented!()
        // match self.unregister_cluster_executor() {
        //     Err(error) => error.into_response(),
        //     Ok(message) => StatusCode::ACCEPTED.into_with_body_response(message),
        // }
    }
}
