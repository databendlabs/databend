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
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::VectorDataType;
use itertools::Itertools;

use crate::MetadataRef;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::optimizers::rule::constant::is_falsy;
use crate::plans::ConstantTableScan;
use crate::plans::Filter;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;

fn may_contain_nan(data_type: &DataType) -> bool {
    match data_type {
        DataType::Number(NumberDataType::Float32 | NumberDataType::Float64) => true,
        DataType::Vector(VectorDataType::Float32(_)) => true,
        DataType::Nullable(inner) => may_contain_nan(inner),
        DataType::Array(inner) => may_contain_nan(inner),
        DataType::Map(inner) => may_contain_nan(inner),
        DataType::Tuple(inner) => inner.iter().any(may_contain_nan),
        _ => false,
    }
}

fn can_eliminate_self_eq(data_type: &DataType) -> bool {
    !data_type.is_nullable() && !may_contain_nan(data_type)
}

pub struct RuleEliminateFilter {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RuleEliminateFilter {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::EliminateFilter,
            // Filter
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::Leaf],
            }],
            metadata,
        }
    }
}

impl Rule for RuleEliminateFilter {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let eval_scalar: Filter = s_expr.plan().clone().try_into()?;
        // First, de-duplication predicates.
        let origin_predicates = eval_scalar.predicates.clone();
        let predicates = origin_predicates
            .clone()
            .into_iter()
            .unique()
            .collect::<Vec<ScalarExpr>>();

        // Rewrite false filter to be empty scan
        if predicates.iter().any(is_falsy) {
            let output_columns = eval_scalar
                .derive_relational_prop(&RelExpr::with_s_expr(s_expr))?
                .output_columns
                .clone();

            let metadata = self.metadata.read();
            let mut fields = Vec::with_capacity(output_columns.len());

            for col in output_columns.iter() {
                fields.push(DataField::new(
                    &col.to_string(),
                    metadata.column(*col).data_type(),
                ));
            }
            let empty_scan =
                ConstantTableScan::new_empty_scan(DataSchemaRefExt::create(fields), output_columns);
            let result = SExpr::create_leaf(Arc::new(RelOperator::ConstantTableScan(empty_scan)));
            state.add_result(result);
            return Ok(());
        }

        // Delete identically equal predicate
        // After constant fold is ready, we can delete the following code
        let predicates = predicates
            .into_iter()
            .filter(|predicate| match predicate {
                ScalarExpr::FunctionCall(func) if func.func_name == "eq" => {
                    if let (
                        ScalarExpr::BoundColumnRef(left_col),
                        ScalarExpr::BoundColumnRef(right_col),
                    ) = (&func.arguments[0], &func.arguments[1])
                    {
                        left_col.column.index != right_col.column.index
                            || !can_eliminate_self_eq(left_col.column.data_type.as_ref())
                    } else {
                        true
                    }
                }
                ScalarExpr::FunctionCall(func)
                    if func.func_name == "is_true"
                        && func.arguments.len() == 1
                        && matches!(func.arguments[0], ScalarExpr::FunctionCall(_)) =>
                {
                    let ScalarExpr::FunctionCall(inner) = &func.arguments[0] else {
                        return true;
                    };
                    if inner.func_name != "eq" || inner.arguments.len() != 2 {
                        return true;
                    }
                    if let (
                        ScalarExpr::BoundColumnRef(left_col),
                        ScalarExpr::BoundColumnRef(right_col),
                    ) = (&inner.arguments[0], &inner.arguments[1])
                    {
                        left_col.column.index != right_col.column.index
                            || !can_eliminate_self_eq(left_col.column.data_type.as_ref())
                    } else {
                        true
                    }
                }
                _ => true,
            })
            .collect::<Vec<ScalarExpr>>();

        if predicates.is_empty() {
            state.add_result(s_expr.child(0)?.clone());
        } else if origin_predicates.len() != predicates.len() {
            let filter = Filter { predicates };
            state.add_result(SExpr::create_unary(
                Arc::new(filter.into()),
                Arc::new(s_expr.child(0)?.clone()),
            ));
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
