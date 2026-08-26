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

use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::SExprVisitor;
use crate::optimizer::ir::VisitAction;
use crate::plans::ConstantExpr;
use crate::plans::Exchange;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;

/// Outcome of aligning materialized CTE placement with the distributed plan.
pub enum MaterializedCTEDistribution {
    Distributed(SExpr),
    RequiresLocal,
}

pub struct MaterializedCTEDistributionOptimizer;

impl MaterializedCTEDistributionOptimizer {
    pub const NAME: &'static str = "MaterializedCTEDistributionOptimizer";

    pub fn create() -> Self {
        Self
    }

    /// Resolve materialized CTE placement after distribution properties are
    /// settled, but before Exchange-sensitive operators are split into stages.
    pub fn optimize(&self, s_expr: &SExpr) -> Result<MaterializedCTEDistribution> {
        let result = s_expr
            .accept(&mut SerialProducerRedistributor)?
            .unwrap_or_else(|| s_expr.clone());

        let mut finder = SerialSequenceFinder::default();
        result.accept(&mut finder)?;
        if finder.found {
            return Ok(MaterializedCTEDistribution::RequiresLocal);
        }

        Ok(MaterializedCTEDistribution::Distributed(result))
    }
}

/// A Sequence producer must participate in the distributed fragment graph. When
/// its CTE definition is serial because it consumes a Merge exchange,
/// redistribute the result after that operator instead of forcing the entire
/// query to run without Exchanges. Other serial sources, such as
/// DummyTableScan, are not coordinator-only Merge consumers and require local
/// re-planning. Hashing by a constant preserves the producer's rows without
/// broadcasting or changing scalar empty-input behavior.
struct SerialProducerRedistributor;

impl SerialProducerRedistributor {
    fn match_merge_backed_producer(expr: &SExpr) -> Result<Option<&SExpr>> {
        let RelOperator::Sequence(_) = expr.plan() else {
            return Ok(None);
        };

        let producer = expr.left_child();
        let physical_prop = RelExpr::with_s_expr(producer).derive_physical_prop()?;
        if physical_prop.distribution != Distribution::Serial {
            return Ok(None);
        }

        let RelOperator::MaterializedCTE(_) = producer.plan() else {
            return Err(ErrorCode::Internal(
                "Sequence left child is expected to be MaterializedCTE".to_string(),
            ));
        };

        let mut expr = producer.unary_child();
        loop {
            let physical_prop = RelExpr::with_s_expr(expr).derive_physical_prop()?;
            if physical_prop.distribution != Distribution::Serial {
                return Ok(None);
            }

            match expr.plan() {
                RelOperator::Exchange(Exchange::Merge) => return Ok(Some(producer)),
                RelOperator::Exchange(_) => return Ok(None),
                _ if expr.arity() == 1 => expr = expr.unary_child(),
                _ => return Ok(None),
            }
        }
    }
}

impl SExprVisitor for SerialProducerRedistributor {
    fn visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
        Ok(VisitAction::Continue)
    }

    fn post_visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
        let Some(producer) = Self::match_merge_backed_producer(expr)? else {
            return Ok(VisitAction::Continue);
        };

        let hash_key = ScalarExpr::ConstantExpr(ConstantExpr {
            value: Scalar::Number(NumberScalar::UInt32(0)),
            span: None,
        });
        let exchange = producer
            .unary_child_arc()
            .ref_build_unary(Exchange::GlobalHash(vec![hash_key]));
        let producer = producer.replace_children([Arc::new(exchange)]);
        Ok(VisitAction::Replace(expr.replace_left_child(producer)))
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
