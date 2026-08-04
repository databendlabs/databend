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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::NumberScalar;

use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::SExprVisitor;
use crate::optimizer::ir::VisitAction;
use crate::plans::ConstantExpr;
use crate::plans::Exchange;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;

pub struct MaterializedCTEDistributionOptimizer {
    ctx: Arc<OptimizerContext>,
}

impl MaterializedCTEDistributionOptimizer {
    pub fn new(ctx: Arc<OptimizerContext>) -> Self {
        Self { ctx }
    }

    pub fn optimize_sync(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut result = if self.ctx.get_enable_distributed_optimization() {
            s_expr
                .accept(&mut SerialProducerRedistributor)?
                .unwrap_or_else(|| s_expr.clone())
        } else {
            s_expr.clone()
        };

        let mut finder = SerialSequenceFinder::default();
        result.accept(&mut finder)?;
        if finder.found {
            result = result.accept(&mut ExchangeRemover)?.unwrap_or(result);
        }

        Ok(result)
    }
}

/// A Sequence producer must participate in the distributed fragment graph. When
/// its CTE definition ends in a scalar operator, redistribute the result after
/// that operator instead of forcing the entire query to run without Exchanges.
/// Hashing by a constant preserves the producer's rows without broadcasting or
/// changing the scalar operator's empty-input behavior.
struct SerialProducerRedistributor;

impl SExprVisitor for SerialProducerRedistributor {
    fn visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
        Ok(VisitAction::Continue)
    }

    fn post_visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
        if !matches!(expr.plan(), RelOperator::Sequence(_)) {
            return Ok(VisitAction::Continue);
        }

        let left = expr.left_child();
        let physical_prop = RelExpr::with_s_expr(left).derive_physical_prop()?;
        if physical_prop.distribution != Distribution::Serial {
            return Ok(VisitAction::Continue);
        }
        if !matches!(left.plan(), RelOperator::MaterializedCTE(_)) {
            return Err(ErrorCode::Internal(
                "Sequence left child is expected to be MaterializedCTE".to_string(),
            ));
        }

        let hash_key = ScalarExpr::ConstantExpr(ConstantExpr {
            value: Scalar::Number(NumberScalar::UInt32(0)),
            span: None,
        });
        let exchange = left
            .unary_child_arc()
            .ref_build_unary(Exchange::GlobalHash(vec![hash_key]));
        let left = left.replace_children([Arc::new(exchange)]);
        Ok(VisitAction::Replace(expr.replace_left_child(left)))
    }
}

#[derive(Default)]
struct SerialSequenceFinder {
    found: bool,
}

impl SExprVisitor for SerialSequenceFinder {
    fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
        if self.found {
            return Ok(VisitAction::SkipChildren);
        }

        if matches!(expr.plan(), RelOperator::Sequence(_)) {
            let left = expr.left_child();
            let physical_prop = RelExpr::with_s_expr(left).derive_physical_prop()?;
            if physical_prop.distribution == Distribution::Serial {
                self.found = true;
                return Ok(VisitAction::SkipChildren);
            }
        }

        Ok(VisitAction::Continue)
    }
}

struct ExchangeRemover;

impl SExprVisitor for ExchangeRemover {
    fn visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
        Ok(VisitAction::Continue)
    }

    fn post_visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
        if matches!(expr.plan(), RelOperator::Exchange(_)) {
            Ok(VisitAction::Replace(expr.unary_child().clone()))
        } else {
            Ok(VisitAction::Continue)
        }
    }
}

#[async_trait::async_trait]
impl Optimizer for MaterializedCTEDistributionOptimizer {
    fn name(&self) -> String {
        "MaterializedCTEDistributionOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(s_expr)
    }
}
