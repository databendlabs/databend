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

use crate::storages::index::RangeFilter;

pub type RangeFilterPredicate =
    Box<dyn Fn(&StatisticsOfColumns) -> Result<bool> + Send + Sync + Unpin>;

pub fn new_range_filter_predicate(
    ctx: &Arc<dyn TableContext>,
    filter_expr: Option<&Expression>,
    schema: &DataSchemaRef,
) -> Result<RangeFilterPredicate> {
    Ok(match filter_expr {
        Some(expr) => {
            let range_filter = RangeFilter::try_create(ctx.clone(), expr, schema.clone())?;
            Box::new(move |v: &StatisticsOfColumns| range_filter.eval(v))
        }
        _ => Box::new(|_: &StatisticsOfColumns| Ok(true)),
    })
}
