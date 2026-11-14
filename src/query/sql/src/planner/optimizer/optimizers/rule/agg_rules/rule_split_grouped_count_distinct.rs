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

use databend_common_exception::Result;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::StatContext;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;

/// At most one group per this many input rows for the rewrite.
const MIN_ROWS_PER_GROUP: f64 = 100.0;

/// Deduplicate before counting distinct values in few groups:
///
/// ```text
/// Aggregate: GROUP BY g; count(DISTINCT x)
/// ```
///
/// becomes
///
/// ```text
/// Aggregate: GROUP BY g; count(x)
/// └── Aggregate: GROUP BY g, x
/// ```
///
/// A group keeps a set of its distinct values, and with few groups the sets are large and
/// merging them leaves a few final aggregates with most of the work. The inner aggregate hashes
/// the pairs `(g, x)` among all the final aggregates instead. `count(x)` skips the pair of a NULL
/// `x`, as `count(DISTINCT x)` skips NULL.
///
/// Applies when `count(DISTINCT x)` of a column is the only aggregate function, the group keys
/// are columns, and their statistics expect at most one group per `MIN_ROWS_PER_GROUP` input
/// rows. Many groups keep small sets, which the rewrite only adds an aggregation to.
pub struct RuleSplitGroupedCountDistinct {
    id: RuleID,
    matchers: Vec<Matcher>,
    stat_context: StatContext,
}

impl RuleSplitGroupedCountDistinct {
    pub fn new(stat_context: StatContext) -> Self {
        Self {
            id: RuleID::SplitGroupedCountDistinct,
            stat_context,
            //  Aggregate
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Aggregate,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleSplitGroupedCountDistinct {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let agg: Aggregate = s_expr.plan().clone().try_into()?;
        if agg.group_items.is_empty() || agg.grouping_sets.is_some() || agg.rank_limit.is_some() {
            return Ok(());
        }
        let [item] = agg.aggregate_functions.as_slice() else {
            return Ok(());
        };
        let ScalarExpr::AggregateFunction(func) = &item.scalar else {
            return Ok(());
        };
        let is_count_distinct = (func.func_name.eq_ignore_ascii_case("count") && func.distinct)
            || func.func_name.eq_ignore_ascii_case("count_distinct");
        let [ScalarExpr::BoundColumnRef(arg)] = func.args.as_slice() else {
            return Ok(());
        };
        if !is_count_distinct || !func.params.is_empty() || !func.sort_descs.is_empty() {
            return Ok(());
        }
        let group_columns = agg.group_items.iter().all(|group| {
            matches!(&group.scalar, ScalarExpr::BoundColumnRef(c) if c.column.index == group.index)
        });
        if !group_columns || agg.group_items.iter().any(|g| g.index == arg.column.index) {
            return Ok(());
        }

        let rel_expr = RelExpr::with_s_expr(s_expr);
        let rows = rel_expr
            .derive_cardinality_child(0, &self.stat_context)?
            .cardinality;
        // Without the NDVs of the keys the estimate is a third of the rows, never few enough.
        let groups = rel_expr.derive_cardinality(&self.stat_context)?.cardinality;
        if groups.is_nan() || groups * MIN_ROWS_PER_GROUP > rows {
            return Ok(());
        }

        let mut group_items = agg.group_items.clone();
        group_items.push(ScalarItem {
            scalar: ScalarExpr::BoundColumnRef(arg.clone()),
            index: arg.column.index,
        });
        let pairs = Aggregate {
            group_items,
            ..Default::default()
        };
        let count = Aggregate {
            group_items: agg.group_items.clone(),
            aggregate_functions: vec![ScalarItem {
                scalar: ScalarExpr::AggregateFunction(AggregateFunction {
                    func_name: "count".to_string(),
                    distinct: false,
                    params: vec![],
                    sort_descs: vec![],
                    display_name: format!("count({})", arg.column.column_name),
                    ..func.clone()
                }),
                index: item.index,
            }],
            ..agg
        };
        state.add_result(SExpr::create_unary(
            Arc::new(count.into()),
            Arc::new(SExpr::create_unary(
                Arc::new(pairs.into()),
                Arc::new(s_expr.child(0)?.clone()),
            )),
        ));
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
