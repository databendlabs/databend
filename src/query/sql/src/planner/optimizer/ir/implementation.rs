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

use crate::plans::AggregateMode;
use crate::plans::PhysicalAggregate;
use crate::plans::PhysicalRelOperator;
use crate::plans::RelOperator;

/// A local implementation of one logical expression. External inputs are slots,
/// not selected child plans or Memo groups. Only the root is equivalent to the
/// logical expression; intermediate nodes describe their own relations.
pub(crate) struct PhysicalFragment {
    pub operator: Arc<PhysicalRelOperator>,
    pub inputs: Vec<FragmentInput>,
}

pub(crate) enum FragmentInput {
    /// Position in the logical expression's children.
    Child(usize),
    Local(PhysicalFragment),
}

impl PhysicalFragment {
    pub fn new(operator: PhysicalRelOperator, inputs: Vec<FragmentInput>) -> Self {
        Self {
            operator: Arc::new(operator),
            inputs,
        }
    }
}

/// All physical implementations of a logical expression. The default is an
/// explicit policy for planning without cost search, not the first alternative.
/// Memo consumers must consider the default AND every alternative.
pub(crate) struct PhysicalImplementations {
    default: PhysicalFragment,
    alternatives: Vec<PhysicalFragment>,
}

impl PhysicalImplementations {
    pub fn new(default: PhysicalFragment, alternatives: Vec<PhysicalFragment>) -> Self {
        Self {
            default,
            alternatives,
        }
    }

    pub fn default(&self) -> &PhysicalFragment {
        &self.default
    }

    pub fn iter(&self) -> impl Iterator<Item = &PhysicalFragment> {
        std::iter::once(&self.default).chain(self.alternatives.iter())
    }
}

/// Enumerate implementations independently of storage and required properties.
/// Logical exploration has already decided the relational shape. This preserves
/// the existing execution strategies, without being a logical rewrite rule or a
/// conversion of an entire logical tree into a selected physical tree.
pub(crate) fn implement_operator(
    operator: &RelOperator,
    input_count: usize,
) -> PhysicalImplementations {
    let inputs = (0..input_count).map(FragmentInput::Child).collect();
    let root = match operator {
        RelOperator::Aggregate(aggregate) => {
            let partial = PhysicalFragment::new(
                PhysicalRelOperator::Aggregate(PhysicalAggregate::new(
                    aggregate.clone(),
                    AggregateMode::Partial,
                )),
                inputs,
            );
            return PhysicalImplementations::new(
                PhysicalFragment::new(
                    PhysicalRelOperator::Aggregate(PhysicalAggregate::new(
                        aggregate.clone(),
                        AggregateMode::Final,
                    )),
                    vec![FragmentInput::Local(partial)],
                ),
                vec![],
            );
        }
        RelOperator::Scan(op) => PhysicalRelOperator::Scan(op.clone()),
        RelOperator::Join(op) => PhysicalRelOperator::Join(op.clone()),
        RelOperator::EvalScalar(op) => PhysicalRelOperator::EvalScalar(op.clone()),
        RelOperator::Filter(op) => PhysicalRelOperator::Filter(op.clone()),
        RelOperator::Sort(op) => PhysicalRelOperator::Sort(op.clone()),
        RelOperator::Limit(op) => PhysicalRelOperator::Limit(op.clone()),
        RelOperator::TopN(op) => PhysicalRelOperator::TopN(op.clone()),
        RelOperator::UnionAll(op) => PhysicalRelOperator::UnionAll(op.clone()),
        RelOperator::DummyTableScan(op) => PhysicalRelOperator::DummyTableScan(op.clone()),
        RelOperator::Window(op) => PhysicalRelOperator::Window(op.clone()),
        RelOperator::WindowGroup(op) => PhysicalRelOperator::WindowGroup(op.clone()),
        RelOperator::ProjectSet(op) => PhysicalRelOperator::ProjectSet(op.clone()),
        RelOperator::ConstantTableScan(op) => PhysicalRelOperator::ConstantTableScan(op.clone()),
        RelOperator::ExpressionScan(op) => PhysicalRelOperator::ExpressionScan(op.clone()),
        RelOperator::CacheScan(op) => PhysicalRelOperator::CacheScan(op.clone()),
        RelOperator::Udf(op) => PhysicalRelOperator::Udf(op.clone()),
        RelOperator::RecursiveCteScan(op) => PhysicalRelOperator::RecursiveCteScan(op.clone()),
        RelOperator::AsyncFunction(op) => PhysicalRelOperator::AsyncFunction(op.clone()),
        RelOperator::Mutation(op) => PhysicalRelOperator::Mutation(op.clone()),
        RelOperator::CompactBlock(op) => PhysicalRelOperator::CompactBlock(op.clone()),
        RelOperator::MutationSource(op) => PhysicalRelOperator::MutationSource(op.clone()),
        RelOperator::MaterializedCTE(op) => PhysicalRelOperator::MaterializedCTE(op.clone()),
        RelOperator::MaterializedCTERef(op) => PhysicalRelOperator::MaterializedCTERef(op.clone()),
        RelOperator::Sequence(op) => PhysicalRelOperator::Sequence(op.clone()),
    };
    PhysicalImplementations::new(PhysicalFragment::new(root, inputs), vec![])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::Aggregate;
    use crate::plans::DummyTableScan;

    #[test]
    fn aggregate_implementation_is_a_local_fragment() {
        let implementations = implement_operator(&Aggregate::default().into(), 1);
        assert_eq!(implementations.iter().count(), 1);
        let root = implementations.default();
        assert_eq!(
            root.operator.as_aggregate().unwrap().mode,
            AggregateMode::Final
        );
        let [FragmentInput::Local(partial)] = root.inputs.as_slice() else {
            panic!("Final must consume the local Partial node");
        };
        assert_eq!(
            partial.operator.as_aggregate().unwrap().mode,
            AggregateMode::Partial
        );
        assert!(matches!(partial.inputs.as_slice(), [FragmentInput::Child(
            0
        )]));
    }

    #[test]
    fn leaf_implementation_has_no_external_inputs() {
        let implementations = implement_operator(&DummyTableScan::new().into(), 0);
        assert!(implementations.default().operator.is_dummy_table_scan());
        assert!(implementations.default().inputs.is_empty());
    }
}
