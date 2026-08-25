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
use databend_common_expression::StateSerdeType;
use databend_common_expression::types::DataType;

use super::AggrImpl;
use super::AggregateBoundOrderByItem;
use super::AggregateBoundOrderBySource;
use super::AggregateFunction;
use super::AggregateFunctionRef;
use super::AggregateFunctionSignature;
use super::AggregateStateDescription;
use super::FunctionFeatures;
use super::FunctionInputLayout;
use super::StateCombinatorPlan;
use super::distinct_combinator;
use super::if_combinator;
use super::sort_combinator;
use super::state_combinator;

pub(crate) trait CombinatorImpl {
    fn create_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl;

    fn create_ordered_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PlainCombinator;

#[derive(Debug, Clone)]
pub(crate) struct IfCombinator {
    pub(crate) nested_args_type: Vec<DataType>,
    pub(crate) condition_index: usize,
    pub(crate) always_false: bool,
    pub(crate) strip_nullable_input: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct DistinctCombinator {
    pub(crate) args_type: Vec<DataType>,
    pub(crate) skip_nulls: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StateCombinator {
    pub(crate) plan: StateCombinatorPlan,
}

fn finish<I>(
    signature: AggregateFunctionSignature,
    features: FunctionFeatures,
    state: AggregateStateDescription,
    implementation: I,
) -> AggregateFunctionRef
where
    I: AggrImpl,
{
    Arc::new(AggregateFunction::new(
        signature,
        FunctionInputLayout::Identity,
        features,
        state,
        implementation,
    ))
}

fn finish_with_input_layout<I>(
    signature: AggregateFunctionSignature,
    input_layout: FunctionInputLayout,
    features: FunctionFeatures,
    state: AggregateStateDescription,
    implementation: I,
) -> AggregateFunctionRef
where
    I: AggrImpl,
{
    Arc::new(AggregateFunction::new(
        signature,
        input_layout,
        features,
        state,
        implementation,
    ))
}

fn finish_with_order_by<I>(
    signature: AggregateFunctionSignature,
    features: FunctionFeatures,
    state: AggregateStateDescription,
    implementation: I,
) -> AggregateFunctionRef
where
    I: AggrImpl,
{
    if signature.order_by.is_empty() {
        return finish(signature, features, state, implementation);
    }

    let (input_types, order_by) =
        sort_combinator::sort_runtime_inputs(&signature.args_type, &signature.order_by);
    let state = sort_combinator::sort_state_description(&state);
    let implementation =
        sort_combinator::AggregateSortImplementation::new(implementation, input_types, order_by);
    finish(signature, features, state, implementation)
}

impl CombinatorImpl for PlainCombinator {
    fn create_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        Ok(finish(signature, features, state, implementation))
    }

    fn create_ordered_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        Ok(finish_with_order_by(
            signature,
            features,
            state,
            implementation,
        ))
    }
}

impl CombinatorImpl for IfCombinator {
    fn create_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let implementation = if_combinator::AggregateIfImplementation::new(
            implementation,
            self.condition_index,
            self.nested_args_type.len(),
            self.always_false,
            self.strip_nullable_input,
        );
        Ok(finish(signature, features, state, implementation))
    }

    fn create_ordered_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        if signature.order_by.is_empty() {
            return self.create_aggregate_function(signature, features, state, implementation);
        }

        // Runtime inputs place the condition last, making the nested ordered
        // inputs a contiguous prefix: [args..., derived keys..., condition].
        let derived_key_count = signature
            .order_by
            .iter()
            .filter(|item| matches!(item.source, AggregateBoundOrderBySource::Derived))
            .count();
        let logical_input_len = signature.args_type.len() + derived_key_count;
        let mut projection = Vec::with_capacity(logical_input_len);
        projection.extend(0..self.condition_index);
        projection.extend(signature.args_type.len()..logical_input_len);
        projection.push(self.condition_index);
        let input_layout = FunctionInputLayout::new(logical_input_len, projection)?;
        let runtime_condition_index = logical_input_len - 1;

        // After FILTER, ordering by the condition is constant and can be
        // removed. References after it shift left because condition is absent
        // from the nested Sort input.
        let nested_order_by = signature
            .order_by
            .iter()
            .filter_map(|item| {
                let source = match item.source {
                    AggregateBoundOrderBySource::Argument { index }
                        if index == self.condition_index =>
                    {
                        return None;
                    }
                    AggregateBoundOrderBySource::Argument { index } => {
                        AggregateBoundOrderBySource::Argument {
                            index: index - usize::from(index > self.condition_index),
                        }
                    }
                    AggregateBoundOrderBySource::Derived => AggregateBoundOrderBySource::Derived,
                };
                Some(AggregateBoundOrderByItem {
                    source,
                    ..item.clone()
                })
            })
            .collect::<Vec<_>>();
        if nested_order_by.is_empty() {
            let implementation = if_combinator::AggregateIfImplementation::new(
                implementation,
                runtime_condition_index,
                self.nested_args_type.len(),
                self.always_false,
                self.strip_nullable_input,
            );
            return Ok(finish_with_input_layout(
                signature,
                input_layout,
                features,
                state,
                implementation,
            ));
        }

        let (input_types, order_by) =
            sort_combinator::sort_runtime_inputs(&self.nested_args_type, &nested_order_by);
        let state = sort_combinator::sort_state_description(&state);
        let implementation = sort_combinator::AggregateSortImplementation::new(
            implementation,
            input_types,
            order_by,
        );
        let implementation = if_combinator::AggregateIfImplementation::new(
            implementation,
            runtime_condition_index,
            self.nested_args_type.len(),
            self.always_false,
            self.strip_nullable_input,
        );
        Ok(finish_with_input_layout(
            signature,
            input_layout,
            features,
            state,
            implementation,
        ))
    }
}

impl CombinatorImpl for DistinctCombinator {
    fn create_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let state = distinct_combinator::distinct_state_description(&state);
        if self.skip_nulls {
            Ok(finish(
                signature,
                features,
                state,
                distinct_combinator::AggregateDistinctImplementation::<true>::new(
                    implementation,
                    self.args_type,
                ),
            ))
        } else {
            Ok(finish(
                signature,
                features,
                state,
                distinct_combinator::AggregateDistinctImplementation::<false>::new(
                    implementation,
                    self.args_type,
                ),
            ))
        }
    }

    fn create_ordered_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let state = distinct_combinator::distinct_state_description(&state);
        if self.skip_nulls {
            Ok(finish_with_order_by(
                signature,
                features,
                state,
                distinct_combinator::AggregateDistinctImplementation::<true>::new(
                    implementation,
                    self.args_type,
                ),
            ))
        } else {
            Ok(finish_with_order_by(
                signature,
                features,
                state,
                distinct_combinator::AggregateDistinctImplementation::<false>::new(
                    implementation,
                    self.args_type,
                ),
            ))
        }
    }
}

impl CombinatorImpl for StateCombinator {
    fn create_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let (signature, state, implementation) = self.wrap(signature, state, implementation)?;
        Ok(finish(signature, features, state, implementation))
    }

    fn create_ordered_aggregate_function<I>(
        self,
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let (signature, state, implementation) = self.wrap(signature, state, implementation)?;
        Ok(finish_with_order_by(
            signature,
            features,
            state,
            implementation,
        ))
    }
}

impl StateCombinator {
    fn wrap<I>(
        self,
        signature: AggregateFunctionSignature,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<(
        AggregateFunctionSignature,
        AggregateStateDescription,
        state_combinator::AggregateStateImplementation<I>,
    )>
    where
        I: AggrImpl,
    {
        let state = if self.plan.nullable_input_result_flag {
            state_combinator::nullable_input_state_description(&state)
        } else {
            state
        };
        let physical_type = StateSerdeType::new(state.serde_items().to_vec()).data_type();
        let function_name = signature
            .name
            .strip_suffix("_state")
            .expect("state combinator names must end with _state");
        let return_type = state_combinator::aggregate_state_data_type(
            function_name,
            &signature.params,
            signature.args_type.clone(),
            physical_type,
        )?;
        let signature = AggregateFunctionSignature {
            return_type,
            ..signature
        };
        Ok((
            signature,
            state,
            state_combinator::AggregateStateImplementation::new(
                implementation,
                self.plan.strip_nullable_input,
                self.plan.nullable_input_result_flag,
            ),
        ))
    }
}
