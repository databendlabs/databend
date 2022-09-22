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

use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_fuse_meta::meta::StatisticsOfColumns;
use common_legacy_planners::LegacyExpression;
use common_storages_index::RangeFilter;

pub trait RangeFilterPruner {
    // returns ture, if target should NOT be pruned (false positive allowed)
    fn should_keep(&self, input: &StatisticsOfColumns, row_count: u64) -> bool;
}

struct KeepTrue;

impl RangeFilterPruner for KeepTrue {
    fn should_keep(&self, _input: &StatisticsOfColumns, _row_count: u64) -> bool {
        true
    }
}

struct KeepFalse;

impl RangeFilterPruner for KeepFalse {
    fn should_keep(&self, _input: &StatisticsOfColumns, _row_count: u64) -> bool {
        false
    }
}

impl RangeFilterPruner for RangeFilter {
    fn should_keep(&self, stats: &StatisticsOfColumns, row_count: u64) -> bool {
        match self.eval(stats, row_count) {
            Ok(r) => r,
            Err(e) => {
                // swallow exceptions intentionally, corrupted index should not prevent execution
                tracing::warn!("failed to range filter, returning ture. {}", e);
                true
            }
        }
    }
}

pub fn new_range_filter_pruner<'a>(
    ctx: &Arc<dyn TableContext>,
    filter_expr: Option<&'a [LegacyExpression]>,
    schema: &'a DataSchemaRef,
) -> Result<Arc<dyn RangeFilterPruner + Send + Sync>> {
    Ok(match filter_expr {
        Some(exprs) if !exprs.is_empty() => {
            let range_filter = RangeFilter::try_create(ctx.clone(), exprs, schema.clone())?;
            match range_filter.try_eval_const() {
                Ok(v) => {
                    if v {
                        Arc::new(KeepTrue)
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
