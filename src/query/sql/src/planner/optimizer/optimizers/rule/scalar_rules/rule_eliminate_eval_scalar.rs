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

use crate::ColumnSet;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::EvalScalar;
use crate::plans::Operator;
use crate::plans::RelOp;

pub struct RuleEliminateEvalScalar {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RuleEliminateEvalScalar {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::EliminateEvalScalar,
            // EvalScalar
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::Leaf],
            }],
            metadata,
        }
    }
}

impl Rule for RuleEliminateEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        // Eliminate empty EvalScalar
        let eval_scalar: EvalScalar = s_expr.plan().clone().try_into()?;
        if eval_scalar.items.is_empty() {
            state.add_result(s_expr.child(0)?.clone());
            return Ok(());
        }

        if self.metadata.read().has_agg_indexes() {
            return Ok(());
        }

        let child = s_expr.child(0)?;
        let child_output_cols = child
            .plan()
            .derive_relational_prop(&RelExpr::with_s_expr(child))?
            .output_columns
            .clone();
        let eval_scalar_output_cols: ColumnSet =
            eval_scalar.items.iter().map(|x| x.index).collect();

        if eval_scalar_output_cols.is_subset(&child_output_cols) {
            // check if there's f(#x) as #x, if so we can't eliminate the eval scalar
            for item in eval_scalar.items {
                match item.scalar {
                    ScalarExpr::FunctionCall(func) => {
                        if func.arguments.len() == 1 {
                            if let ScalarExpr::BoundColumnRef(bound_column_ref) = &func.arguments[0]
                            {
                                if bound_column_ref.column.index == item.index {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    ScalarExpr::CastExpr(cast) => {
                        if let ScalarExpr::BoundColumnRef(bound_column_ref) = cast.argument.as_ref()
                        {
                            if bound_column_ref.column.index == item.index {
                                return Ok(());
                            }
                        }
                    }
                    _ => {}
                }
            }
            state.add_result(s_expr.child(0)?.clone());
            return Ok(());
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
