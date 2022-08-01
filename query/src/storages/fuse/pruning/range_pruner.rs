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
use common_planners::Expression;
use common_tracing::tracing;

use crate::storages::index::RangeFilter;

pub trait RangeFilterPruner {
    // returns ture, if target should NOT be pruned (false positive allowed)
    fn should_keep(&self, input: &StatisticsOfColumns) -> bool;
}

struct NoPruner;

impl RangeFilterPruner for NoPruner {
    fn should_keep(&self, _input: &StatisticsOfColumns) -> bool {
        true
    }
}

impl RangeFilterPruner for RangeFilter {
    fn should_keep(&self, stats: &StatisticsOfColumns) -> bool {
        match self.eval(stats) {
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
    filter_expr: Option<&'a Expression>,
    schema: &'a DataSchemaRef,
) -> Result<Arc<dyn RangeFilterPruner + Send + Sync>> {
    Ok(match filter_expr {
        Some(expr) => {
            let range_filter = RangeFilter::try_create(ctx.clone(), expr, schema.clone())?;
            Arc::new(range_filter)
        }
        _ => Arc::new(NoPruner),
    })
}
