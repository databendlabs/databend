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

use super::AggregateBoundOrderByItem;
use super::AggregateBoundOrderBySource;
use super::AggregateCallInstance;
use super::AggregateCallRef;
use super::AggregateEval;
use super::AggregateFeatures;
use super::AggregateSignature;
use super::AggregateStateDescription;
use super::FunctionInputLayout;
use super::StateCombinatorPlan;
use super::distinct_combinator;
use super::if_combinator;
use super::sort_combinator;
use super::state_combinator;

pub(crate) trait Combinator {
    fn create<const ORDERED: bool, I: AggregateEval>(
        self,
        signature: AggregateSignature,
        features: AggregateFeatures,
        state: AggregateStateDescription,
        eval: I,
    ) -> Result<AggregateCallRef>;
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
    signature: AggregateSignature,
    features: AggregateFeatures,
    state: AggregateStateDescription,
    eval: I,
) -> AggregateCallRef
where
    I: AggregateEval,
{
    Arc::new(AggregateCallInstance::new(
        signature,
        FunctionInputLayout::Identity,
        features,
        state,
        eval,
    ))
}

fn finish_with_input_layout<I>(
    signature: AggregateSignature,
    input_layout: FunctionInputLayout,
    features: AggregateFeatures,
    state: AggregateStateDescription,
    eval: I,
) -> AggregateCallRef
where
    I: AggregateEval,
{
    Arc::new(AggregateCallInstance::new(
        signature,
        input_layout,
        features,
        state,
        eval,
    ))
}

fn finish_with_order_by<I>(
    signature: AggregateSignature,
    features: AggregateFeatures,
    state: AggregateStateDescription,
    eval: I,
) -> AggregateCallRef
where
    I: AggregateEval,
{
    if signature.order_by.is_empty() {
        return finish(signature, features, state, eval);
    }

    let (input_types, order_by) =
        sort_combinator::sort_runtime_inputs(&signature.args_type, &signature.order_by);
    let state = sort_combinator::sort_state_description(&state);
    let eval = sort_combinator::SortEval::new(eval, input_types, order_by);
    finish(signature, features, state, eval)
}

impl Combinator for PlainCombinator {
    fn create<const ORDERED: bool, I>(
        self,
        signature: AggregateSignature,
        features: AggregateFeatures,
        state: AggregateStateDescription,
        eval: I,
    ) -> Result<AggregateCallRef>
    where
        I: AggregateEval,
    {
        if ORDERED {
            Ok(finish_with_order_by(signature, features, state, eval))
        } else {
            Ok(finish(signature, features, state, eval))
        }
    }
}

impl Combinator for IfCombinator {
    fn create<const ORDERED: bool, I>(
        self,
        signature: AggregateSignature,
        features: AggregateFeatures,
        state: AggregateStateDescription,
        eval: I,
    ) -> Result<AggregateCallRef>
    where
        I: AggregateEval,
    {
        if !ORDERED || signature.order_by.is_empty() {
            let eval = if_combinator::IfEval::new(
                eval,
                self.condition_index,
                self.nested_args_type.len(),
                self.always_false,
                self.strip_nullable_input,
            );
            return Ok(finish(signature, features, state, eval));
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
            let eval = if_combinator::IfEval::new(
                eval,
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
                eval,
            ));
        }

        let (input_types, order_by) =
            sort_combinator::sort_runtime_inputs(&self.nested_args_type, &nested_order_by);
        let state = sort_combinator::sort_state_description(&state);
        let eval = sort_combinator::SortEval::new(eval, input_types, order_by);
        let eval = if_combinator::IfEval::new(
            eval,
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
            eval,
        ))
    }
}

impl Combinator for DistinctCombinator {
    fn create<const ORDERED: bool, I>(
        self,
        signature: AggregateSignature,
        features: AggregateFeatures,
        state: AggregateStateDescription,
        eval: I,
    ) -> Result<AggregateCallRef>
    where
        I: AggregateEval,
    {
        let state = distinct_combinator::distinct_state_description(&state);
        if self.skip_nulls {
            let eval = distinct_combinator::DistinctEval::<true>::new(eval, self.args_type);
            if ORDERED {
                Ok(finish_with_order_by(signature, features, state, eval))
            } else {
                Ok(finish(signature, features, state, eval))
            }
        } else {
            let eval = distinct_combinator::DistinctEval::<false>::new(eval, self.args_type);
            if ORDERED {
                Ok(finish_with_order_by(signature, features, state, eval))
            } else {
                Ok(finish(signature, features, state, eval))
            }
        }
    }
}

impl Combinator for StateCombinator {
    fn create<const ORDERED: bool, I>(
        self,
        signature: AggregateSignature,
        features: AggregateFeatures,
        state: AggregateStateDescription,
        eval: I,
    ) -> Result<AggregateCallRef>
    where
        I: AggregateEval,
    {
        let (signature, state, eval) = self.wrap(signature, state, eval)?;
        if ORDERED {
            Ok(finish_with_order_by(signature, features, state, eval))
        } else {
            Ok(finish(signature, features, state, eval))
        }
    }
}

impl StateCombinator {
    fn wrap<I>(
        self,
        signature: AggregateSignature,
        state: AggregateStateDescription,
        eval: I,
    ) -> Result<(
        AggregateSignature,
        AggregateStateDescription,
        state_combinator::StateEval<I>,
    )>
    where
        I: AggregateEval,
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
        let signature = AggregateSignature {
            return_type,
            ..signature
        };
        Ok((
            signature,
            state,
            state_combinator::StateEval::new(
                eval,
                self.plan.strip_nullable_input,
                self.plan.nullable_input_result_flag,
            ),
        ))
    }
}
