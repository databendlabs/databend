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

use databend_common_exception::Result;

use super::materialized_view;
use crate::IndexType;
use crate::match_op;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::plans::RelOp;
use crate::plans::RelOperator;

pub struct RuleTryApplyMaterializedView {
    id: RuleID,
    ctx: std::sync::Arc<OptimizerContext>,
    metadata: crate::MetadataRef,

    matchers: Vec<Matcher>,
}

impl RuleTryApplyMaterializedView {
    fn sorted_matchers() -> Vec<Matcher> {
        vec![
            match_op!(EvalScalar -> Sort -> Scan),
            match_op!(EvalScalar -> Sort -> Filter -> Scan),
            match_op!(EvalScalar -> Sort -> Aggregate -> EvalScalar -> Scan),
            match_op!(EvalScalar -> Sort -> Aggregate -> EvalScalar -> Filter -> Scan),
            match_op!(EvalScalar -> TopN -> Scan),
            match_op!(EvalScalar -> TopN -> Filter -> Scan),
            match_op!(EvalScalar -> TopN -> Aggregate -> EvalScalar -> Scan),
            match_op!(EvalScalar -> TopN -> Aggregate -> EvalScalar -> Filter -> Scan),
        ]
    }

    fn normal_matchers() -> Vec<Matcher> {
        vec![
            // Expression
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Scan,
                    children: vec![],
                }],
            },
            // Expression
            //     |
            //   Filter
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Scan,
                        children: vec![],
                    }],
                }],
            },
            // Expression
            //     |
            // Aggregation
            //     |
            // Expression
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::EvalScalar,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Scan,
                            children: vec![],
                        }],
                    }],
                }],
            },
            // Expression
            //     |
            // Aggregation
            //     |
            // Expression
            //     |
            //   Filter
            //     |
            //    Scan
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::EvalScalar,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Filter,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::Scan,
                                children: vec![],
                            }],
                        }],
                    }],
                }],
            },
        ]
    }

    fn matchers() -> Vec<Matcher> {
        let mut patterns = Self::normal_matchers();
        patterns.extend(Self::sorted_matchers());
        patterns
    }

    pub fn new(ctx: std::sync::Arc<OptimizerContext>) -> Self {
        let metadata = ctx.get_metadata();
        Self {
            id: RuleID::TryApplyMaterializedView,
            ctx,
            metadata,
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
        // Row access policy tables must not use materialized-view rewrites, because
        // the pre-aggregated results were computed over the full table without
        // applying the policy's row-level filter.
        if self.scan_has_secure_predicates(s_expr) {
            return Ok(());
        }

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
        let candidates = all_candidates
            .iter()
            .filter(|candidate| candidate.source_table_index == table_index)
            .cloned()
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(());
        }
        let table_name = metadata.table(table_index).name();

        if let Some((mut result, mv_table_id)) =
            materialized_view::try_rewrite(table_index, table_name, &metadata, s_expr, &candidates)?
        {
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

    fn scan_has_secure_predicates(&self, s_expr: &SExpr) -> bool {
        match s_expr.plan() {
            RelOperator::Scan(scan) => scan.secure_predicates.is_some(),
            _ => {
                if let Ok(child) = s_expr.child(0) {
                    self.scan_has_secure_predicates(child)
                } else {
                    false
                }
            }
        }
    }
}
