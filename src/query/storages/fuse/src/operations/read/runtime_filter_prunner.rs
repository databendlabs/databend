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

use common_catalog::plan::PartInfoPtr;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::ConstantFolder;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_functions::BUILTIN_FUNCTIONS;
use log::info;
use storages_common_index::statistics_to_domain;

use crate::FusePartInfo;

pub fn runtime_filter_pruner(
    table_schema: Arc<TableSchema>,
    part: &PartInfoPtr,
    filters: &HashMap<String, Expr<String>>,
    func_ctx: &FunctionContext,
) -> Result<bool> {
    if filters.is_empty() {
        return Ok(false);
    }

    let part = FusePartInfo::from_part(part)?;
    let pruned = filters.iter().any(|(_, filter)| {
        let column_refs = filter.column_refs();
        // Currently only support filter with one column(probe key).
        debug_assert!(column_refs.len() == 1);
        let ty = column_refs.values().last().unwrap();
        let name = column_refs.keys().last().unwrap();
        let filters = filters.clone();
        if let Some(stats) = &part.columns_stat {
            let column_ids = table_schema.leaf_columns_of(name);
            debug_assert!(column_ids.len() == 1);
            if let Some(stat) = stats.get(&column_ids[0]) {
                debug_assert_eq!(&stat.min.as_ref().infer_data_type(), ty);
                let stats = vec![stat];
                let domain = statistics_to_domain(stats, ty);
                let mut input_domains = HashMap::new();
                input_domains.insert(name.to_string(), domain);
                let (new_expr, _) = ConstantFolder::fold_with_domain(
                    filter,
                    &input_domains,
                    func_ctx,
                    &BUILTIN_FUNCTIONS,
                );
                return matches!(new_expr, Expr::Constant {
                    scalar: Scalar::Boolean(false),
                    ..
                });
            }
        }
        false
    });

    if pruned {
        info!(
            "Pruned partition with {:?} rows by runtime filter",
            part.nums_rows
        );
        return Ok(true);
    }

    Ok(false)
}
