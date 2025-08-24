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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;

pub enum ProbeData {
    Next,
    DataBlock(DataBlock),
}

pub struct Progress {
    total_rows: usize,
    total_bytes: usize,
    progressed_rows: usize,
    progressed_bytes: usize,
}

/// Core Join trait that abstracts different join implementations.
/// Provides unified interface for both regular hash join and grace hash join.
pub trait Join: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn add_block(&self, block: DataBlock) -> Result<TryCompleteStream<Progress>>;

    fn finish_build(&self) -> Result<TryCompleteStream<Progress>>;

    fn probe(&self, block: DataBlock) -> Result<TryCompleteStream<ProbeData>>;

    fn finished_probe(&self) -> Result<TryCompleteStream<ProbeData>>;
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

pub struct JoinParams {
    pub build_keys: Vec<Expr>,
    pub probe_keys: Vec<Expr>,
    pub is_null_equals: Vec<bool>,

    pub func_ctx: Arc<FunctionContext>,
}

pub trait ITryCompleteStream<T>: Send + 'static {
    fn next_try_complete(&self) -> Result<Option<TryCompleteFuture<T>>>;
}

pub trait ITryCompleteFuture<T>: Future<Output = Result<T>> + Send + 'static {
    fn try_complete(&self) -> Result<Option<T>>;
}

pub type TryCompleteStream<T> = Box<dyn ITryCompleteStream<T>>;

pub type TryCompleteFuture<T> = Pin<Box<dyn ITryCompleteFuture<T>>>;

pub struct NoneTryCompleteStream<T>;

impl<T> NoneTryCompleteStream<T> {
    pub fn create() -> TryCompleteStream<T> {
        Box::new(NoneTryCompleteStream)
    }
}

impl<T> ITryCompleteStream<T> for NoneTryCompleteStream<T> {
    fn next_try_complete(&self) -> Result<Option<TryCompleteFuture<T>>> {
        Ok(None)
    }
}
