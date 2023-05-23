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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::Result;
use common_expression::ColumnId;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::TableSchemaRef;
use storages_common_index::RangeIndex;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::StatisticsOfColumns;

pub trait RangePruner {
    // returns true, if target should NOT be pruned (false positive allowed)
    fn should_keep(
        &self,
        input: &StatisticsOfColumns,
        metas: Option<&HashMap<ColumnId, ColumnMeta>>,
    ) -> bool;
}

struct KeepTrue;

impl RangePruner for KeepTrue {
    fn should_keep(
        &self,
        _input: &StatisticsOfColumns,
        _metas: Option<&HashMap<ColumnId, ColumnMeta>>,
    ) -> bool {
        true
    }
}

struct KeepFalse;

impl RangePruner for KeepFalse {
    fn should_keep(
        &self,
        _input: &StatisticsOfColumns,
        _metas: Option<&HashMap<ColumnId, ColumnMeta>>,
    ) -> bool {
        false
    }
}

impl RangePruner for RangeIndex {
    fn should_keep(
        &self,
        stats: &StatisticsOfColumns,
        metas: Option<&HashMap<ColumnId, ColumnMeta>>,
    ) -> bool {
        match self.apply(stats, |k| {
            if let Some(metas) = metas {
                metas.get(k).is_none()
            } else {
                false
            }
        }) {
            Ok(r) => r,
            Err(e) => {
                // swallow exceptions intentionally, corrupted index should not prevent execution
                tracing::warn!("failed to range filter, returning true. {}", e);
                true
            }
        }
    }
}

pub struct RangePrunerCreator;

impl RangePrunerCreator {
    /// Create a new [`RangePruner`] from expression and schema.
    ///
    /// Note: the schema should be the schema of the table, not the schema of the input.
    pub fn try_create<'a>(
        func_ctx: FunctionContext,
        schema: &'a TableSchemaRef,
        filter_expr: Option<&'a Expr<String>>,
    ) -> Result<Arc<dyn RangePruner + Send + Sync>> {
        Self::try_create_with_default_stats(
            func_ctx,
            schema,
            filter_expr,
            StatisticsOfColumns::default(),
        )
    }

    pub fn try_create_with_default_stats<'a>(
        func_ctx: FunctionContext,
        schema: &'a TableSchemaRef,
        filter_expr: Option<&'a Expr<String>>,
        default_stats: StatisticsOfColumns,
    ) -> Result<Arc<dyn RangePruner + Send + Sync>> {
        Ok(match filter_expr {
            Some(exprs) => {
                let range_filter =
                    RangeIndex::try_create(func_ctx, exprs, schema.clone(), default_stats)?;
                match range_filter.try_apply_const() {
                    Ok(v) => {
                        if v {
                            Arc::new(range_filter)
                        } else {
                            Arc::new(KeepFalse)
                        }
                    }
                    Err(_) => Arc::new(range_filter),
                }
            }
            _ => Arc::new(KeepTrue),
        })
    }
}
