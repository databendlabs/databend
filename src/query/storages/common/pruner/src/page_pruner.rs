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

use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_index::PageIndex;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use log::warn;

pub trait PagePruner {
    // returns true, if target should NOT be pruned (false positive allowed)
    fn should_keep(&self, _stats: &Option<ClusterStatistics>) -> (bool, Option<Range<usize>>);
}

struct KeepTrue;

impl PagePruner for KeepTrue {
    fn should_keep(&self, _stats: &Option<ClusterStatistics>) -> (bool, Option<Range<usize>>) {
        (true, None)
    }
}

struct KeepFalse;

impl PagePruner for KeepFalse {
    fn should_keep(&self, _stats: &Option<ClusterStatistics>) -> (bool, Option<Range<usize>>) {
        (false, None)
    }
}

impl PagePruner for PageIndex {
    fn should_keep(&self, stats: &Option<ClusterStatistics>) -> (bool, Option<Range<usize>>) {
        match self.apply(stats) {
            Ok(r) => r,
            Err(e) => {
                // swallow exceptions intentionally, corrupted index should not prevent execution
                warn!("failed to page filter, returning true. {}", e);
                (true, None)
            }
        }
    }
}

pub struct PagePrunerCreator;

impl PagePrunerCreator {
    /// Create a new [`PagePruner`] from expression and schema.
    ///
    /// Note: the schema should be the schema of the table, not the schema of the input.
    pub fn try_create<'a>(
        func_ctx: FunctionContext,
        schema: &'a TableSchemaRef,
        filter_expr: Option<&'a Expr<String>>,
        cluster_key_meta: Option<ClusterKey>,
        cluster_keys: Vec<RemoteExpr<String>>,
    ) -> Result<Arc<dyn PagePruner + Send + Sync>> {
        if cluster_key_meta.is_none()
            || cluster_keys.is_empty()
            || cluster_keys
                .iter()
                .any(|expr| !matches!(expr, RemoteExpr::ColumnRef { .. }))
        {
            return Ok(Arc::new(KeepTrue));
        }

        let cluster_key_meta = cluster_key_meta.unwrap();

        Ok(match filter_expr {
            Some(expr) => {
                let cluster_keys = cluster_keys
                    .iter()
                    .map(|expr| match expr {
                        RemoteExpr::ColumnRef { id, .. } => id.to_string(),
                        _ => unreachable!(),
                    })
                    .collect::<Vec<_>>();

                let page_filter = PageIndex::try_create(
                    func_ctx,
                    cluster_key_meta.0,
                    cluster_keys,
                    expr,
                    schema.clone(),
                )?;
                match page_filter.try_apply_const() {
                    Ok(v) => {
                        if v {
                            Arc::new(page_filter)
                        } else {
                            Arc::new(KeepFalse)
                        }
                    }
                    Err(_) => Arc::new(page_filter),
                }
            }
            _ => Arc::new(KeepTrue),
        })
    }
}
