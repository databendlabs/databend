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

use std::collections::HashSet;

use databend_common_exception::Result;

use super::materialized_view;
use crate::IndexType;
use crate::match_op;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::plans::RelOperator;

pub struct RuleTryApplyMaterializedView {
    id: RuleID,
    ctx: std::sync::Arc<OptimizerContext>,
    metadata: crate::MetadataRef,
    output_columns: Option<HashSet<crate::Symbol>>,

    matchers: Vec<Matcher>,
}

impl RuleTryApplyMaterializedView {
    fn matchers() -> Vec<Matcher> {
        // QueryInfo walks the complete unary query subtree and supports arbitrary combinations of
        // EvalScalar, Filter, Aggregate, Sort and TopN. Expression binding can introduce multiple
        // intermediate EvalScalar nodes, so do not enumerate fixed-depth shapes here. A Limit
        // root is matched only by top-N materialized views.
        vec![match_op!(EvalScalar -> *), match_op!(Limit -> *)]
    }

    fn is_supported_query_subtree(s_expr: &SExpr) -> bool {
        match s_expr.plan() {
            RelOperator::Scan(_) => true,
            RelOperator::EvalScalar(_)
            | RelOperator::Filter(_)
            | RelOperator::Aggregate(_)
            | RelOperator::Sort(_)
            | RelOperator::TopN(_) => {
                s_expr.arity() == 1 && s_expr.child(0).is_ok_and(Self::is_supported_query_subtree)
            }
            _ => false,
        }
    }

    fn contains_ordering(s_expr: &SExpr) -> bool {
        if matches!(s_expr.plan(), RelOperator::Sort(_) | RelOperator::TopN(_)) {
            return true;
        }
        s_expr.arity() == 1 && s_expr.child(0).is_ok_and(Self::contains_ordering)
    }

    pub fn new(ctx: std::sync::Arc<OptimizerContext>) -> Self {
        let metadata = ctx.get_metadata();
        Self {
            id: RuleID::TryApplyMaterializedView,
            ctx,
            metadata,
            output_columns: None,
            matchers: Self::matchers(),
        }
    }

    pub fn new_with_output_columns(
        ctx: std::sync::Arc<OptimizerContext>,
        output_columns: Option<HashSet<crate::Symbol>>,
    ) -> Self {
        let metadata = ctx.get_metadata();
        Self {
            id: RuleID::TryApplyMaterializedView,
            ctx,
            metadata,
            output_columns,
            matchers: Self::matchers(),
        }
    }
}

impl Rule for RuleTryApplyMaterializedView {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut crate::optimizer::optimizers::rule::TransformResult,
    ) -> Result<()> {
        let is_limit = matches!(s_expr.plan(), RelOperator::Limit(_));
        if is_limit {
            if s_expr.arity() != 1 || !s_expr.child(0).is_ok_and(Self::is_supported_query_subtree) {
                return Ok(());
            }
        } else if !Self::is_supported_query_subtree(s_expr) || Self::contains_ordering(s_expr) {
            return Ok(());
        }

        // A source with a row access or masking policy has no candidates: binding skips it
        // before building any, because MV storage does not follow source policies.
        let (table_index, _, has_sample) = self.get_table(s_expr);
        if has_sample {
            // Candidate plans were bound without the query's SAMPLE. Rebinding
            // sampled Fresh/Hybrid branches is required before this is safe.
            return Ok(());
        }
        let metadata = self.metadata.read();
        let source_table_id = metadata.table(table_index).table().get_id();
        let Some(all_candidates) = metadata.get_materialized_view_candidates(source_table_id)
        else {
            return Ok(());
        };
        // A top-N MV holds a subset of its definition's rows, so it can only answer a Limit, and
        // a Limit is answered only from top-N MVs.
        let candidates = all_candidates
            .iter()
            .filter(|candidate| {
                candidate.source_table_index == table_index
                    && materialized_view::is_top_n_candidate(candidate) == is_limit
            })
            .cloned()
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(());
        }
        let table_name = metadata.table(table_index).name();

        let required_output_columns = self.output_columns.as_ref();
        let rewritten = if is_limit {
            let Some(required_output_columns) = required_output_columns else {
                return Ok(());
            };
            materialized_view::try_rewrite_top_n(
                table_index,
                table_name,
                &metadata,
                s_expr,
                &candidates,
                required_output_columns,
                self.ctx.get_stat_context(),
            )?
        } else {
            materialized_view::try_rewrite(
                table_index,
                table_name,
                &metadata,
                s_expr,
                &candidates,
                required_output_columns,
                self.ctx.get_stat_context(),
            )?
        };
        if let Some((mut result, mv_table_id)) = rewritten {
            if let Some(candidate) = candidates
                .iter()
                .find(|candidate| candidate.mv_table_id == mv_table_id)
            {
                self.ctx
                    .get_table_ctx()
                    .result_cache_state()
                    .add_cache_key_extra(format!(
                        "mv:{}:{}:{:?}|source:{}:{}:{:?}",
                        candidate.mv_table_id,
                        candidate.mv_table_seq,
                        candidate.mv_snapshot_location,
                        candidate.source_table_id,
                        candidate.source_table_seq,
                        candidate.source_snapshot_location
                    ));
            }
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl RuleTryApplyMaterializedView {
    fn get_table(&self, s_expr: &SExpr) -> (IndexType, String, bool) {
        match s_expr.plan() {
            RelOperator::Scan(scan) => {
                let metadata = self.metadata.read();
                let table = metadata.table(scan.table_index);
                (
                    scan.table_index,
                    format!("{}.{}.{}", table.catalog(), table.database(), table.name()),
                    scan.sample.is_some(),
                )
            }
            _ => self.get_table(s_expr.child(0).unwrap()),
        }
    }
}
