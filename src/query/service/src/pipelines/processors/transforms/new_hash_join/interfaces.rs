// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_sql::ColumnSet;
// use futures_util::FutureExt; // Not needed anymore

pub enum ProbeData {
    Next,
    DataBlock(DataBlock),
}

pub struct Progress {
    pub total_rows: usize,
    pub total_bytes: usize,
    pub progressed_rows: usize,
    pub progressed_bytes: usize,
}

/// Core Join trait that abstracts different join implementations.
/// Provides unified interface for both regular hash join and grace hash join.
pub trait Join: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn add_block(&self, block: DataBlock) -> Result<TryCompleteStream<Progress>>;

    fn finish_build(&self) -> Result<TryCompleteStream<Progress>>;

    fn probe(&self, block: DataBlock) -> Result<TryCompleteStream<ProbeData>>;

    fn finish_probe(&self) -> Result<TryCompleteStream<ProbeData>>;
}

// /// Convertible join trait for joins that can be converted from one type to another

//     /// Convert this join implementation to another type
// pub trait ConvertibleJoin<T: Join>: Join {
//     fn convert_to(self) -> Result<T>;
// }

/// Join configuration for setting up different join types
#[derive(Clone)]
pub struct JoinSettings {
    pub max_block_rows: usize,
    pub max_block_bytes: usize,
    // pub join_type: JoinType,
    // pub build_keys: Vec<Expr>,
    // pub probe_keys: Vec<Expr>,
    // pub hash_method: HashMethodKind,
    // pub func_ctx: Arc<FunctionContext>,
    // pub memory_settings: MemorySettings,
    // pub is_null_equal: Vec<bool>,
    // pub max_block_size: usize,
}

impl JoinSettings {
    pub fn check_threshold(&self, block: &DataBlock) -> bool {
        block.num_rows() >= self.max_block_rows || block.memory_size() >= self.max_block_bytes
    }
}

pub struct JoinParams {
    pub build_keys: Vec<Expr>,
    pub probe_keys: Vec<Expr>,
    pub is_null_equals: Vec<bool>,

    pub build_projections: ColumnSet,

    pub func_ctx: Arc<FunctionContext>,
}

pub trait ITryCompleteStream<T>: Send + 'static {
    fn next_try_complete(&mut self) -> Result<Option<TryCompleteFuture<T>>>;
}

#[async_trait::async_trait]
pub trait ITryCompleteFuture<T>: Send + 'static {
    fn try_complete(&mut self) -> Result<Option<T>>;

    async fn async_complete(&mut self) -> Result<T> {
        unreachable!()
    }
}

pub type TryCompleteStream<T> = Box<dyn ITryCompleteStream<T>>;

/// A wrapper struct that implements Future for ITryCompleteFuture
/// This allows ITryCompleteFuture to be polled as a regular Future
pub struct TryCompleteFuture<T> {
    inner: Option<Box<dyn ITryCompleteFuture<T>>>,
    async_future: Option<Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>>,
}

impl<T: Send + 'static> TryCompleteFuture<T> {
    pub fn new<F: ITryCompleteFuture<T>>(inner: F) -> Self {
        Self {
            inner: Some(Box::new(inner)),
            async_future: None,
        }
    }

    pub fn try_complete(&mut self) -> Result<Option<T>> {
        if let Some(inner) = &mut self.inner {
            if let Some(res) = inner.try_complete()? {
                self.inner = None;
                return Ok(Some(res));
            }

            let mut inner = self.inner.take().unwrap();
            let fut = async move { inner.async_complete().await };
            self.async_future = Some(Box::pin(fut));
        }

        Ok(None)
    }
}

impl<T: Send + 'static> Future for TryCompleteFuture<T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();

        // If we have an async future already running, poll it
        if let Some(ref mut fut) = this.async_future {
            match fut.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    this.async_future = None;
                    return Poll::Ready(result);
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Try synchronous completion first
        if let Some(ref mut inner) = this.inner {
            match inner.try_complete() {
                Ok(Some(result)) => {
                    this.inner = None;
                    return Poll::Ready(Ok(result));
                }
                Ok(None) => {
                    // Move to async completion
                    if let Some(mut inner) = this.inner.take() {
                        let fut = async move { inner.async_complete().await };
                        this.async_future = Some(Box::pin(fut));
                        // Immediately poll the new future
                        return self.poll(cx);
                    }
                }
                Err(e) => {
                    this.inner = None;
                    return Poll::Ready(Err(e));
                }
            }
        }

        Poll::Pending
    }
}

pub struct NoneTryCompleteStream<T: Send + 'static>(PhantomData<T>);

impl<T: Send + 'static> NoneTryCompleteStream<T> {
    pub fn create() -> TryCompleteStream<T> {
        Box::new(NoneTryCompleteStream(PhantomData))
    }
}

impl<T: Send + 'static> ITryCompleteStream<T> for NoneTryCompleteStream<T> {
    fn next_try_complete(&mut self) -> Result<Option<TryCompleteFuture<T>>> {
        Ok(None)
    }
}

pub struct FlattenTryCompleteStream<T> {
    stream: VecDeque<TryCompleteStream<T>>,
}

impl<T: Send + 'static> FlattenTryCompleteStream<T> {
    pub fn create(stream: Vec<TryCompleteStream<T>>) -> TryCompleteStream<T> {
        Box::new(FlattenTryCompleteStream {
            stream: VecDeque::from(stream),
        })
    }
}

impl<T: Send + 'static> ITryCompleteStream<T> for FlattenTryCompleteStream<T> {
    fn next_try_complete(&mut self) -> Result<Option<TryCompleteFuture<T>>> {
        while let Some(stream) = self.stream.front_mut() {
            if let Some(v) = stream.next_try_complete()? {
                return Ok(Some(v));
            }

            self.stream.pop_front();
        }

        Ok(None)
    }
}
