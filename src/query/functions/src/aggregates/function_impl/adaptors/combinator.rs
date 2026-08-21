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
use super::AggregateFunction;
use super::AggregateFunctionRef;
use super::AggregateFunctionSignature;
use super::AggregateStateDescription;
use super::FunctionFeatures;
use super::NullPolicy;
use super::StateCombinatorPlan;
use super::distinct_combinator;
use super::if_combinator;
use super::state_combinator;

pub(crate) trait CombinatorImpl: Copy {
    fn create_aggregate_function<I>(
        self,
        args_type: &[DataType],
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct IfCombinator {
    pub(crate) null_policy: NullPolicy,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DistinctCombinator {
    pub(crate) null_policy: NullPolicy,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct StateCombinator {
    pub(crate) plan: StateCombinatorPlan,
}

impl CombinatorImpl for PlainCombinator {
    fn create_aggregate_function<I>(
        self,
        _args_type: &[DataType],
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        Ok(Arc::new(AggregateFunction::new(
            signature,
            features,
            state,
            implementation,
        )))
    }
}

impl CombinatorImpl for IfCombinator {
    fn create_aggregate_function<I>(
        self,
        args_type: &[DataType],
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let condition_index = args_type.len() - 1;
        let condition_type = args_type[condition_index].remove_nullable();
        let always_false = condition_type.is_null();
        let strip_nullable_input = self.null_policy != NullPolicy::Keep;
        Ok(Arc::new(AggregateFunction::new(
            signature,
            features,
            state,
            if_combinator::AggregateIfImplementation::new(
                implementation,
                condition_index,
                always_false,
                strip_nullable_input,
            ),
        )))
    }
}

impl CombinatorImpl for DistinctCombinator {
    fn create_aggregate_function<I>(
        self,
        args_type: &[DataType],
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let args_type = args_type.to_vec();
        let state = distinct_combinator::distinct_state_description(&state);
        let skip_nulls = self.null_policy != NullPolicy::Keep;
        if skip_nulls {
            Ok(Arc::new(AggregateFunction::new(
                signature,
                features,
                state,
                distinct_combinator::AggregateDistinctImplementation::<true>::new(
                    implementation,
                    args_type,
                ),
            )))
        } else {
            Ok(Arc::new(AggregateFunction::new(
                signature,
                features,
                state,
                distinct_combinator::AggregateDistinctImplementation::<false>::new(
                    implementation,
                    args_type,
                ),
            )))
        }
    }
}

impl CombinatorImpl for StateCombinator {
    fn create_aggregate_function<I>(
        self,
        _args_type: &[DataType],
        signature: AggregateFunctionSignature,
        features: FunctionFeatures,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
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
        Ok(Arc::new(AggregateFunction::new(
            signature,
            features,
            state,
            state_combinator::AggregateStateImplementation::new(
                implementation,
                self.plan.strip_nullable_input,
                self.plan.nullable_input_result_flag,
            ),
        )))
    }
}
