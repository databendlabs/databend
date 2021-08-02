use std::sync::Arc;
use std::task::Context;

use common_runtime::tokio::macros::support::Pin;
use common_runtime::tokio::macros::support::Poll;
use futures::Future;
use futures::TryFuture;
use nom::AndThen;
use warp::filters::BoxedFilter;
use warp::reply::Response;
use warp::Filter;
use warp::Rejection;
use warp::Reply;

#[async_trait::async_trait]
pub trait Action: Sized {
    async fn do_action_impl(self) -> Response;

    async fn do_action(self) -> Result<ResponseReplyWarp, Rejection> {
        Ok(ResponseReplyWarp(self.do_action_impl().await))
    }
}

pub struct ResponseReplyWarp(Response);

impl Reply for ResponseReplyWarp {
    fn into_response(self) -> Response {
        self.0
    }
}
