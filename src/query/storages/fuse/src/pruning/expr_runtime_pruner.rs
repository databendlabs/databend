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

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::statistics_to_domain;
use log::debug;
use log::info;

use crate::FuseBlockPartInfo;

/// Runtime pruner that uses expressions to prune partitions.
pub struct ExprRuntimePruner {
    exprs: Vec<Expr<String>>,
}

impl ExprRuntimePruner {
    /// Create a new expression runtime pruner.
    pub fn new(exprs: Vec<Expr<String>>) -> Self {
        Self { exprs }
    }

    /// Prune a partition based on expressions.
    /// Returns true if the partition should be pruned.
    pub fn prune(
        &self,
        func_ctx: &FunctionContext,
        table_schema: Arc<TableSchema>,
        part: &PartInfoPtr,
    ) -> Result<bool> {
        if self.exprs.is_empty() {
            return Ok(false);
        }

        let part = FuseBlockPartInfo::from_part(part)?;
        let pruned = self.exprs.iter().any(|filter| {
            let column_refs = filter.column_refs();
            // Currently only support filter with one column (probe key).
            debug_assert!(column_refs.len() == 1);
            let ty = column_refs.values().last().unwrap();
            let name = column_refs.keys().last().unwrap();

            if let Some(stats) = &part.columns_stat {
                let column_ids = table_schema.leaf_columns_of(name);
                if column_ids.len() != 1 {
                    return false;
                }
                debug_assert!(column_ids.len() == 1);
                if let Some(stat) = stats.get(&column_ids[0]) {
                    let stats = vec![stat];
                    let domain = statistics_to_domain(stats, ty);

                    let mut input_domains = HashMap::new();
                    input_domains.insert(name.to_string(), domain.clone());

                    let (new_expr, _) = ConstantFolder::fold_with_domain(
                        filter,
                        &input_domains,
                        func_ctx,
                        &BUILTIN_FUNCTIONS,
                    );
                    debug!("Runtime filter after constant fold is {:?}", new_expr.sql_display());
                    return matches!(new_expr, Expr::Constant(Constant {
                        scalar: Scalar::Boolean(false),
                        ..
                    }));
                }
            }

            info!("Can't prune the partition by runtime filter, because there is no statistics for the partition");
            false
        });

        if pruned {
            info!(
                "Pruned partition with {:?} rows by runtime filter",
                part.nums_rows
            );
            Profile::record_usize_profile(ProfileStatisticsName::RuntimeFilterPruneParts, 1);
        }

        Ok(pruned)
    }
}
