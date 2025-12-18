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

use crate::MetadataRef;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::ConstantTableScan;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::UnionAll;

pub struct RuleEliminateUnion {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RuleEliminateUnion {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::EliminateUnion,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::UnionAll,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
            metadata,
        }
    }

    fn is_empty_scan(s_expr: &SExpr) -> Result<bool> {
        let child_num = s_expr.children.len();
        if child_num > 1 {
            return Ok(false);
        }
        if child_num == 0 {
            Ok(matches!(
                s_expr.plan(),
                RelOperator::ConstantTableScan(ConstantTableScan { num_rows: 0, .. })
            ))
        } else {
            Self::is_empty_scan(s_expr.child(0)?)
        }
    }
}

impl Rule for RuleEliminateUnion {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let union: UnionAll = s_expr.plan().clone().try_into()?;

        // Need to check that union's output indexes are the same as left child's output indexes
        // currently this is always !false now, so the following codes are not necessary
        if !union
            .left_outputs
            .iter()
            .all(|(idx, _)| union.output_indexes.contains(idx))
        {
            return Ok(());
        }

        let left_child = s_expr.child(0)?;
        let right_child = s_expr.child(1)?;

        // If left child is empty, we won't eliminate it
        // Because the schema of the union is from left child, and the union node's parent relies on the schema
        // And the parent's schema has been binded during the binder phase.
        if Self::is_empty_scan(left_child)? && Self::is_empty_scan(right_child)? {
            // If both children are empty, replace with EmptyResultScan
            let union_output_columns = union
                .derive_relational_prop(&RelExpr::with_s_expr(s_expr))?
                .output_columns
                .clone();
            let metadata = self.metadata.read();
            let mut fields = Vec::with_capacity(union_output_columns.len());
            for col in union_output_columns.iter() {
                fields.push(DataField::new(
                    &col.to_string(),
                    metadata.column(*col).data_type(),
                ));
            }

            let empty_scan = ConstantTableScan::new_empty_scan(
                DataSchemaRefExt::create(fields),
                union_output_columns,
            );
            let result = SExpr::create_leaf(Arc::new(RelOperator::ConstantTableScan(empty_scan)));
            state.add_result(result);
        } else if Self::is_empty_scan(right_child)? {
            // If right child is empty, use left child
            state.add_result(left_child.clone());
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
