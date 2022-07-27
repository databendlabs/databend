//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_datavalues::DataSchemaRef;
use common_planners::Expression;
use opendal::Operator;

pub trait Limiter {
    fn within_limit(&self, n: usize) -> bool;
}

pub struct Unlimited;

impl Limiter for Unlimited {
    fn within_limit(&self, _: usize) -> bool {
        true
    }
}

impl Limiter for AtomicUsize {
    fn within_limit(&self, n: usize) -> bool {
        let o = self.fetch_sub(n, Ordering::Relaxed);
        o < n
    }
}

#[async_trait::async_trait]
pub trait BloomPruner {
    async fn eval(&self, loc: &str) -> common_exception::Result<bool>;
}

pub(crate) struct NonPruner;

#[async_trait::async_trait]
impl BloomPruner for NonPruner {
    async fn eval(&self, _loc: &str) -> common_exception::Result<bool> {
        Ok(true)
    }
}

pub struct BloomFilterPruner<'a> {
    cols: &'a [String],
    expr: &'a Expression,
    dal: &'a Operator,
    schema: &'a DataSchemaRef,
    ctx: &'a Arc<dyn TableContext>,
}

impl<'a> BloomFilterPruner<'a> {
    pub fn new(
        filter_block_column_names: &'a [String],
        filter_expression: &'a Expression,
        dal: &'a Operator,
        schema: &'a DataSchemaRef,
        ctx: &'a Arc<dyn TableContext>,
    ) -> Self {
        Self {
            cols: filter_block_column_names,
            expr: filter_expression,
            dal,
            schema,
            ctx,
        }
    }
}

#[async_trait::async_trait]
impl BloomPruner for BloomFilterPruner<'_> {
    async fn eval(&self, loc: &str) -> common_exception::Result<bool> {
        super::util::filter_block_by_bloom_index(
            self.ctx,
            self.dal.clone(),
            self.schema,
            self.expr,
            self.cols,
            loc,
        )
        .await
    }
}
