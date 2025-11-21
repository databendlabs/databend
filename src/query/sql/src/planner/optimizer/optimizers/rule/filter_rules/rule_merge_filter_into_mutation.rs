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
use databend_common_expression::types::DataType;

use crate::binder::MutationType;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Filter;
use crate::plans::MutationSource;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::MetadataRef;

pub struct RuleMergeFilterIntoMutation {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RuleMergeFilterIntoMutation {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::MergeFilterIntoMutation,
            // Filter
            //  \
            //   MutationSource
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::MutationSource,
                    children: vec![],
                }],
            }],
            metadata,
        }
    }
}

impl Rule for RuleMergeFilterIntoMutation {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut mutation: MutationSource = s_expr.child(0)?.plan().clone().try_into()?;
        mutation
            .read_partition_columns
            .extend(filter.predicates.iter().flat_map(|v| v.used_columns()));
        mutation.predicates = filter.predicates;

        if mutation.mutation_type == MutationType::Update {
            let column_index = self
                .metadata
                .write()
                .add_derived_column("_predicate".to_string(), DataType::Boolean);
            mutation.predicate_column_index = Some(column_index);
        }

        let new_expr = SExpr::create_leaf(Arc::new(RelOperator::MutationSource(mutation)));
        state.add_result(new_expr);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
