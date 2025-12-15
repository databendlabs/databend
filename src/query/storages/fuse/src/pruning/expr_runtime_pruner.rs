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
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::RuntimeFilterStats;
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
    exprs: Vec<RuntimeFilterExpr>,
}

#[derive(Clone)]
pub struct RuntimeFilterExpr {
    pub filter_id: usize,
    pub expr: Expr<String>,
    pub stats: Arc<RuntimeFilterStats>,
}

impl ExprRuntimePruner {
    /// Create a new expression runtime pruner.
    pub fn new(exprs: Vec<RuntimeFilterExpr>) -> Self {
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
        let mut partition_pruned = false;
        for entry in self.exprs.iter() {
            let start = Instant::now();
            let filter = &entry.expr;
            let mut should_prune = false;
            // If the filter is a constant false, we can prune the partition.
            if matches!(
                filter,
                Expr::Constant(Constant {
                    scalar: Scalar::Boolean(false),
                    ..
                })
            ) {
                should_prune = true;
            } else {
                let column_refs = filter.column_refs();
                // Currently only support filter with one column (probe key).
                debug_assert!(column_refs.len() == 1);
                let ty = column_refs.values().last().unwrap();
                let name = column_refs.keys().last().unwrap();

                if let Some(stats) = &part.columns_stat {
                    let column_ids = table_schema.leaf_columns_of(name);
                    if column_ids.len() == 1 {
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
                            debug!(
                                "Runtime filter after constant fold is {:?}",
                                new_expr.sql_display()
                            );
                            if matches!(
                                new_expr,
                                Expr::Constant(Constant {
                                    scalar: Scalar::Boolean(false),
                                    ..
                                })
                            ) {
                                should_prune = true;
                            }
                        }
                    }
                } else {
                    info!(
                        "Can't prune the partition by runtime filter, because there is no statistics for the partition"
                    );
                }
            }

            let elapsed = start.elapsed();
            entry.stats.record_inlist_min_max(
                elapsed.as_nanos() as u64,
                if should_prune {
                    part.nums_rows as u64
                } else {
                    0
                },
                if should_prune { 1 } else { 0 },
            );

            if should_prune {
                partition_pruned = true;
                break;
            }
        }

        if partition_pruned {
            Profile::record_usize_profile(ProfileStatisticsName::RuntimeFilterPruneParts, 1);
        }

        Ok(partition_pruned)
    }
}
