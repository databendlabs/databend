use std::sync::Arc;
use std::task::Context;

use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use common_runtime::tokio::macros::support::Pin;
use common_runtime::tokio::macros::support::Poll;
use futures::Future;
use futures::TryFuture;
use warp::hyper::Body;
use warp::reply::Response;
use warp::Rejection;
use warp::Reply;

use crate::api::http::v1::action::Action;
use crate::api::http::v1::responses::ErrorCodeResponseHelper;
use crate::api::http::v1::responses::JSONResponseHelper;
use crate::sessions::SessionManagerRef;

pub struct ListAction {
    sessions: SessionManagerRef,
}

impl ListAction {
    pub fn create(sessions: SessionManagerRef) -> ListAction {
        ListAction { sessions }
    }
}

#[async_trait::async_trait]
impl Action for ListAction {
    async fn do_action_impl(self) -> Response {
        match self.sessions.get_cluster_manager().get_executors().await {
            Err(error) => error.into_response(),
            Ok(executors) => executors.into_json_response(),
        }
    }
}
