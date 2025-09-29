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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::arrow::and_validities;
use databend_common_expression::type_check::check_function;
use databend_common_expression::BlockEntry;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use parking_lot::RwLock;

use crate::physical_plans::HashJoin;
use crate::physical_plans::PhysicalRuntimeFilter;
use crate::physical_plans::PhysicalRuntimeFilters;
use crate::sql::plans::JoinType;

pub const MARKER_KIND_TRUE: u8 = 0;
pub const MARKER_KIND_FALSE: u8 = 1;
pub const MARKER_KIND_NULL: u8 = 2;

pub struct MarkJoinDesc {
    // pub(crate) marker_index: Option<IndexType>,
    pub(crate) has_null: RwLock<bool>,
}

pub struct HashJoinDesc {
    pub(crate) build_keys: Vec<Expr>,
    pub(crate) probe_keys: Vec<Expr>,
    pub(crate) is_null_equal: Vec<bool>,
    pub(crate) join_type: JoinType,
    pub(crate) single_to_inner: Option<JoinType>,
    /// when we have non-equal conditions for hash join,
    /// for example `a = b and c = d and e > f`, we will use `and_filters`
    /// to wrap `e > f` as a other_predicate to do next step's check.
    pub(crate) other_predicate: Option<Expr>,
    pub(crate) marker_join_desc: MarkJoinDesc,
    /// Whether the Join are derived from correlated subquery.
    pub(crate) from_correlated_subquery: bool,
    pub(crate) runtime_filter: RuntimeFiltersDesc,
}

#[derive(Debug)]
pub struct RuntimeFilterDesc {
    pub id: usize,
    pub build_key: Expr,
    pub probe_key: Expr<String>,
    pub scan_id: usize,
    pub enable_bloom_runtime_filter: bool,
    pub enable_inlist_runtime_filter: bool,
    pub enable_min_max_runtime_filter: bool,
}

pub struct RuntimeFiltersDesc {
    pub filters: Vec<RuntimeFilterDesc>,
}

impl From<&PhysicalRuntimeFilters> for RuntimeFiltersDesc {
    fn from(runtime_filter: &PhysicalRuntimeFilters) -> Self {
        Self {
            filters: runtime_filter.filters.iter().map(|rf| rf.into()).collect(),
        }
    }
}

impl From<&PhysicalRuntimeFilter> for RuntimeFilterDesc {
    fn from(runtime_filter: &PhysicalRuntimeFilter) -> Self {
        Self {
            id: runtime_filter.id,
            build_key: runtime_filter.build_key.as_expr(&BUILTIN_FUNCTIONS),
            probe_key: runtime_filter.probe_key.as_expr(&BUILTIN_FUNCTIONS),
            scan_id: runtime_filter.scan_id,
            enable_bloom_runtime_filter: runtime_filter.enable_bloom_runtime_filter,
            enable_inlist_runtime_filter: runtime_filter.enable_inlist_runtime_filter,
            enable_min_max_runtime_filter: runtime_filter.enable_min_max_runtime_filter,
        }
    }
}

impl HashJoinDesc {
    pub fn create(join: &HashJoin) -> Result<HashJoinDesc> {
        let other_predicate = Self::join_predicate(&join.join_type, &join.non_equi_conditions)?;

        let build_keys: Vec<Expr> = join
            .build_keys
            .iter()
            .map(|k| k.as_expr(&BUILTIN_FUNCTIONS))
            .collect();
        let probe_keys: Vec<Expr> = join
            .probe_keys
            .iter()
            .map(|k| k.as_expr(&BUILTIN_FUNCTIONS))
            .collect();

        Ok(HashJoinDesc {
            join_type: join.join_type.clone(),
            build_keys,
            probe_keys,
            is_null_equal: join.is_null_equal.clone(),
            other_predicate,
            marker_join_desc: MarkJoinDesc {
                has_null: RwLock::new(false),
                // marker_index: join.marker_index,
            },
            from_correlated_subquery: join.from_correlated_subquery,
            single_to_inner: join.single_to_inner.clone(),
            runtime_filter: (&join.runtime_filter).into(),
        })
    }

    fn join_predicate(
        join_type: &JoinType,
        non_equi_conditions: &[RemoteExpr],
    ) -> Result<Option<Expr>> {
        let expr = non_equi_conditions
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .try_reduce(|lhs, rhs| {
                check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
            });
        // For RIGHT MARK join, we can't use is_true to cast filter into non_null boolean
        match expr {
            Ok(Some(expr)) => match expr {
                Expr::Constant(Constant { ref scalar, .. }) if !scalar.is_null() => {
                    Ok(Some(cast_expr_to_non_null_boolean(expr)?))
                }
                _ => {
                    if matches!(join_type, JoinType::RightMark) {
                        Ok(Some(expr))
                    } else {
                        Ok(Some(check_function(
                            None,
                            "is_true",
                            &[],
                            &[expr],
                            &BUILTIN_FUNCTIONS,
                        )?))
                    }
                }
            },
            other => other,
        }
    }

    pub fn build_key(&self, block: &DataBlock, ctx: &FunctionContext) -> Result<Vec<BlockEntry>> {
        let build_keys = &self.build_keys;
        let evaluator = Evaluator::new(block, ctx, &BUILTIN_FUNCTIONS);
        build_keys
            .iter()
            .map(|expr| {
                Ok(evaluator
                    .run(expr)?
                    .convert_to_full_column(expr.data_type(), block.num_rows())
                    .into())
            })
            .collect::<Result<_>>()
    }

    pub fn probe_key(&self, block: &DataBlock, ctx: &FunctionContext) -> Result<Vec<BlockEntry>> {
        let build_keys = &self.probe_keys;
        let evaluator = Evaluator::new(block, ctx, &BUILTIN_FUNCTIONS);
        build_keys
            .iter()
            .map(|expr| {
                Ok(evaluator
                    .run(expr)?
                    .convert_to_full_column(expr.data_type(), block.num_rows())
                    .into())
            })
            .collect::<Result<_>>()
    }

    pub fn build_valids_by_keys(&self, keys: &mut DataBlock) -> Result<Option<Bitmap>> {
        let is_null_equal = &self.is_null_equal;
        let mut valids = None;

        let num_rows = keys.num_rows();

        for (entry, null_equals) in keys.columns().iter().zip(is_null_equal.iter()) {
            if !null_equals {
                let (is_all_null, column_valids) = entry.as_column().unwrap().validity();

                if is_all_null {
                    valids = Some(Bitmap::new_constant(false, num_rows));
                    break;
                }

                valids = and_validities(valids, column_valids.cloned());

                if let Some(bitmap) = valids.as_ref() {
                    if bitmap.null_count() == bitmap.len() {
                        break;
                    }

                    if bitmap.null_count() == 0 {
                        valids = None;
                    }
                }
            }
        }

        for (entry, is_null) in keys.columns_mut().iter_mut().zip(is_null_equal.iter()) {
            if !is_null && entry.data_type().is_nullable() {
                *entry = entry.clone().remove_nullable();
            }
        }

        Ok(valids)
    }
}
