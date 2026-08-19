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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::aggregate::aggregate_function_v2::AggregateFunctionRef;
use databend_common_expression::aggregate::aggregate_function_v2::AggregateFunctionRequest;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;

use super::AggrImpl;
use super::AggregateFunctionSignature;
use super::AggregateMultiArgOrNullImplementation;
use super::AggregateStateDescription;
use super::CombinatorImpl;
use super::DirectBuildContext;
use super::DirectBuildFn;
use super::DistinctCombinator;
use super::FunctionFeatures;
use super::IfCombinator;
use super::MultiArgBuildContext;
use super::MultiArgBuildFn;
use super::NullPolicy;
use super::PlainCombinator;
use super::StateCombinator;
use super::StateCombinatorPlan;
use super::UnaryAggrImpl;
use super::UnaryBuildContext;
use super::UnaryBuildFn;
use super::UnaryState;
use super::sort_combinator;
use super::state_combinator;
use super::try_create_null_argument_result_function;

pub struct AggregateFunctionNameRoutePath<'a> {
    request: AggregateFunctionRequest<'a>,
}

pub struct MatchedNameStep<'a> {
    request: AggregateFunctionRequest<'a>,
}

pub struct PlainOrNullStep<'a> {
    request: AggregateFunctionRequest<'a>,
}

pub struct IfOrNullStep<'a> {
    request: AggregateFunctionRequest<'a>,
    args_type: Vec<DataType>,
    null_policy: NullPolicy,
}

pub struct StateOrNullStep<'a> {
    request: AggregateFunctionRequest<'a>,
    args_type: Option<Vec<DataType>>,
    state_plan: StateCombinatorPlan,
}

pub struct DistinctSkipNullStep<'a> {
    request: AggregateFunctionRequest<'a>,
    args_type: Vec<DataType>,
    null_policy: NullPolicy,
}

fn build_signature(
    request: &AggregateFunctionRequest<'_>,
    signature_args_type: &[DataType],
    return_type: DataType,
) -> AggregateFunctionSignature {
    AggregateFunctionSignature {
        name: request.name.to_string(),
        params: request.params.to_vec(),
        args_type: signature_args_type.to_vec(),
        distinct: request.distinct,
        order_by: request.order_by.to_vec(),
        return_type,
    }
}

impl<'a, C> UnaryBuildContext<'a, C>
where C: CombinatorImpl
{
    fn new(
        request: AggregateFunctionRequest<'a>,
        signature_args_type: &'a [DataType],
        combinator_args_type: &'a [DataType],
        features: FunctionFeatures,
        combinator: C,
    ) -> Result<Self> {
        let [arg_type] = request.args_type else {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects exactly one argument, got {}",
                request.name,
                request.args_type.len()
            )));
        };
        Ok(Self {
            request,
            signature_args_type,
            combinator_args_type,
            features,
            combinator,
            arg_type: arg_type.remove_nullable(),
        })
    }

    pub(crate) fn name(&self) -> &str {
        self.request.name
    }

    pub(crate) fn params(&self) -> &[databend_common_expression::Scalar] {
        self.request.params
    }

    pub(crate) fn arg_type(&self) -> &DataType {
        &self.arg_type
    }

    pub(crate) fn create_unary<S, I, R>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        function_info: S::FunctionInfo,
    ) -> Result<AggregateFunctionRef>
    where
        S: UnaryState<I, R>,
        I: AccessType,
        R: ValueType,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        if signature.args_type[0].is_nullable_or_null() {
            let implementation =
                super::UnaryAggregateImplementation::new(super::UnaryImpl::<S, I, R, true>::new(
                    function_info.into(),
                ));
            self.combinator.create_aggregate_function(
                self.combinator_args_type,
                signature,
                self.features,
                state,
                implementation,
            )
        } else {
            let implementation =
                super::UnaryAggregateImplementation::new(super::UnaryImpl::<S, I, R, false>::new(
                    function_info.into(),
                ));
            self.combinator.create_aggregate_function(
                self.combinator_args_type,
                signature,
                self.features,
                state,
                implementation,
            )
        }
    }

    pub(crate) fn create_unary_or_null<S, I, R>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        function_info: S::FunctionInfo,
    ) -> Result<AggregateFunctionRef>
    where
        S: UnaryState<I, R>,
        I: AccessType,
        R: ValueType,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        let inner = super::UnaryImpl::<S, I, R, false>::new(std::sync::Arc::new(function_info));
        let implementation =
            super::UnaryAggregateImplementation::new(super::UnaryOrNull::new(inner));
        let state = state.with_null_flag();
        self.combinator.create_aggregate_function(
            self.combinator_args_type,
            signature,
            self.features,
            state,
            implementation,
        )
    }

    pub(crate) fn create_unary_or_null_with_impl<I, R, U>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        implementation: U,
    ) -> Result<AggregateFunctionRef>
    where
        I: AccessType,
        R: ValueType,
        U: UnaryAggrImpl<I, R>,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        let implementation =
            super::UnaryAggregateImplementation::new(super::UnaryOrNull::new(implementation));
        let state = state.with_null_flag();
        self.combinator.create_aggregate_function(
            self.combinator_args_type,
            signature,
            self.features,
            state,
            implementation,
        )
    }
}

impl<'a, C> MultiArgBuildContext<'a, C>
where C: CombinatorImpl
{
    fn new(
        request: AggregateFunctionRequest<'a>,
        signature_args_type: &'a [DataType],
        combinator_args_type: &'a [DataType],
        features: FunctionFeatures,
        combinator: C,
    ) -> Self {
        let args_type = request
            .args_type
            .iter()
            .map(DataType::remove_nullable)
            .collect();
        Self {
            request,
            signature_args_type,
            combinator_args_type,
            features,
            combinator,
            args_type,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.request.name
    }

    pub(crate) fn params(&self) -> &[databend_common_expression::Scalar] {
        self.request.params
    }

    pub(crate) fn args_type(&self) -> &[DataType] {
        &self.args_type
    }

    pub(crate) fn create_multi_arg_or_null<I>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        debug_assert!(signature.order_by.is_empty());
        self.combinator.create_aggregate_function(
            self.combinator_args_type,
            signature,
            self.features,
            state.with_null_flag(),
            AggregateMultiArgOrNullImplementation::new(implementation),
        )
    }
}

impl<'a, C> DirectBuildContext<'a, C>
where C: CombinatorImpl
{
    fn new(
        request: AggregateFunctionRequest<'a>,
        signature_args_type: &'a [DataType],
        combinator_args_type: &'a [DataType],
        features: FunctionFeatures,
        combinator: C,
    ) -> Self {
        Self {
            request,
            signature_args_type,
            combinator_args_type,
            features,
            combinator,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.request.name
    }

    pub(crate) fn params(&self) -> &[databend_common_expression::Scalar] {
        self.request.params
    }

    pub(crate) fn args_type(&self) -> &[DataType] {
        self.request.args_type
    }

    pub(crate) fn create<I>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        debug_assert!(signature.order_by.is_empty());
        self.combinator.create_aggregate_function(
            self.combinator_args_type,
            signature,
            self.features,
            state,
            implementation,
        )
    }

    pub(crate) fn create_ordered<I>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        implementation: I,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        if signature.order_by.is_empty() {
            return self.combinator.create_aggregate_function(
                self.combinator_args_type,
                signature,
                self.features,
                state,
                implementation,
            );
        }

        let (input_types, order_by) =
            sort_combinator::sort_runtime_inputs(self.combinator_args_type, &signature.order_by);
        let state = sort_combinator::sort_state_description(&state);
        let implementation = sort_combinator::AggregateSortImplementation::new(
            implementation,
            input_types,
            order_by,
        );
        self.combinator.create_aggregate_function(
            self.combinator_args_type,
            signature,
            self.features,
            state,
            implementation,
        )
    }
}

impl<'a> AggregateFunctionNameRoutePath<'a> {
    pub fn root(request: AggregateFunctionRequest<'a>) -> Self {
        Self { request }
    }

    pub fn unknown(&self) -> Result<AggregateFunctionRef> {
        Err(ErrorCode::UnknownAggregateFunction(format!(
            "Unsupported AggregateFunction: {}",
            self.request.name
        )))
    }

    pub fn names(&self, names: &[&str]) -> Option<MatchedNameStep<'a>> {
        matches_name(names, self.request.name).then(|| MatchedNameStep {
            request: self.request.clone(),
        })
    }

    pub fn suffixed_names(&self, base_names: &[&str], suffix: &str) -> Option<MatchedNameStep<'a>> {
        matches_suffixed_name(base_names, suffix, self.request.name).then(|| MatchedNameStep {
            request: self.request.clone(),
        })
    }
}

impl<'a> MatchedNameStep<'a> {
    pub fn build_with_direct_input(
        self,
        features: FunctionFeatures,
        build: DirectBuildFn<PlainCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        build(DirectBuildContext::new(
            self.request,
            signature_args_type,
            signature_args_type,
            features,
            PlainCombinator,
        ))
    }

    pub fn plain_null_argument_result(
        &self,
        returns_default_when_only_null: bool,
    ) -> Result<Option<AggregateFunctionRef>> {
        if matches!(self.request.args_type, [DataType::Null]) {
            Ok(Some(try_create_null_argument_result_function(
                self.request.clone(),
                returns_default_when_only_null,
            )?))
        } else {
            Ok(None)
        }
    }

    pub fn plain_or_null(self) -> PlainOrNullStep<'a> {
        PlainOrNullStep {
            request: self.request,
        }
    }

    pub fn null_argument_result(
        &self,
        returns_default_when_only_null: bool,
    ) -> Result<Option<AggregateFunctionRef>> {
        if self.request.args_type.iter().any(DataType::is_null) {
            Ok(Some(try_create_null_argument_result_function(
                self.request.clone(),
                returns_default_when_only_null,
            )?))
        } else {
            Ok(None)
        }
    }

    pub fn if_nullable_input_null_argument_result(
        &self,
        returns_default_when_only_null: bool,
    ) -> Result<Option<AggregateFunctionRef>> {
        let has_null_argument = self.request.args_type.iter().any(DataType::is_null);
        let has_nested_null_after_nullable_removed = self
            .request
            .args_type
            .split_last()
            .map(|(_, nested_arg_types)| nested_arg_types.iter().any(DataType::is_null))
            .unwrap_or(false);
        if has_null_argument || has_nested_null_after_nullable_removed {
            Ok(Some(try_create_null_argument_result_function(
                self.request.clone(),
                returns_default_when_only_null,
            )?))
        } else {
            Ok(None)
        }
    }

    pub fn if_combinator(
        self,
        null_policy: NullPolicy,
        strip_nullable_input: bool,
    ) -> Result<IfOrNullStep<'a>> {
        let Some((condition_type, nested_arg_types)) = self.request.args_type.split_last() else {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects a condition argument",
                self.request.name
            )));
        };

        let condition_type = condition_type.remove_nullable();
        if !condition_type.is_null() && condition_type != DataType::Boolean {
            return Err(ErrorCode::BadArguments(format!(
                "The type of the last argument for {} must be boolean type, but got {:?}",
                self.request.name,
                self.request.args_type[self.request.args_type.len() - 1]
            )));
        }

        let args_type = if strip_nullable_input {
            nested_arg_types
                .iter()
                .map(DataType::remove_nullable)
                .collect()
        } else {
            nested_arg_types.to_vec()
        };

        Ok(IfOrNullStep {
            request: self.request,
            args_type,
            null_policy,
        })
    }

    pub fn state_null_argument_result(&self) -> Result<Option<AggregateFunctionRef>> {
        self.request
            .args_type
            .iter()
            .any(DataType::is_null)
            .then(|| state_combinator::create_state_null_result_function(self.request.clone()))
            .transpose()
    }

    pub fn state_nullable_input_plan(
        &self,
        returns_default_when_only_null: bool,
    ) -> StateCombinatorPlan {
        let strip_nullable_input = self
            .request
            .args_type
            .iter()
            .any(|data_type| matches!(data_type, DataType::Nullable(_)));
        StateCombinatorPlan {
            strip_nullable_input,
            nullable_input_result_flag: strip_nullable_input && !returns_default_when_only_null,
        }
    }

    pub fn state_combinator(self, state_plan: StateCombinatorPlan) -> StateOrNullStep<'a> {
        let args_type = if state_plan.strip_nullable_input {
            Some(
                self.request
                    .args_type
                    .iter()
                    .map(DataType::remove_nullable)
                    .collect(),
            )
        } else {
            None
        };

        StateOrNullStep {
            request: self.request,
            args_type,
            state_plan,
        }
    }

    pub fn distinct_combinator(
        self,
        null_policy: NullPolicy,
        strip_nullable_input: bool,
    ) -> DistinctSkipNullStep<'a> {
        let args_type = if strip_nullable_input {
            self.request
                .args_type
                .iter()
                .map(DataType::remove_nullable)
                .collect()
        } else {
            self.request.args_type.to_vec()
        };
        DistinctSkipNullStep {
            request: self.request,
            args_type,
            null_policy,
        }
    }
}

impl<'a> PlainOrNullStep<'a> {
    pub fn build_with_unary_input(
        self,
        features: FunctionFeatures,
        build: UnaryBuildFn<PlainCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        build(UnaryBuildContext::new(
            self.request,
            signature_args_type,
            signature_args_type,
            features,
            PlainCombinator,
        )?)
    }

    pub fn build_with_direct_input(
        self,
        features: FunctionFeatures,
        build: DirectBuildFn<PlainCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        build(DirectBuildContext::new(
            self.request,
            signature_args_type,
            signature_args_type,
            features,
            PlainCombinator,
        ))
    }

    pub fn build_with_multi_arg_input(
        self,
        features: FunctionFeatures,
        build: MultiArgBuildFn<PlainCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        build(MultiArgBuildContext::new(
            self.request,
            signature_args_type,
            signature_args_type,
            features,
            PlainCombinator,
        ))
    }
}

impl<'a> IfOrNullStep<'a> {
    pub fn build_with_unary_input(
        self,
        features: FunctionFeatures,
        build: UnaryBuildFn<IfCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        let combinator_args_type = self.request.args_type;
        let request = request_with_args_type(&self.request, &self.args_type, false);
        build(UnaryBuildContext::new(
            request,
            signature_args_type,
            combinator_args_type,
            features,
            IfCombinator {
                null_policy: self.null_policy,
            },
        )?)
    }

    pub fn build_with_multi_arg_input(
        self,
        features: FunctionFeatures,
        build: MultiArgBuildFn<IfCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        let combinator_args_type = self.request.args_type;
        let request = request_with_args_type(&self.request, &self.args_type, false);
        build(MultiArgBuildContext::new(
            request,
            signature_args_type,
            combinator_args_type,
            features,
            IfCombinator {
                null_policy: self.null_policy,
            },
        ))
    }

    pub fn build_with_direct_input(
        self,
        features: FunctionFeatures,
        build: DirectBuildFn<IfCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        let combinator_args_type = self.request.args_type;
        let request = request_with_args_type(&self.request, &self.args_type, false);
        build(DirectBuildContext::new(
            request,
            signature_args_type,
            combinator_args_type,
            features,
            IfCombinator {
                null_policy: self.null_policy,
            },
        ))
    }
}

impl<'a> StateOrNullStep<'a> {
    pub fn build_with_unary_input(
        self,
        features: FunctionFeatures,
        build: UnaryBuildFn<StateCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        let request = if let Some(args_type) = &self.args_type {
            request_with_args_type(&self.request, args_type, false)
        } else {
            self.request
        };
        let combinator_args_type = request.args_type;
        build(UnaryBuildContext::new(
            request,
            signature_args_type,
            combinator_args_type,
            features,
            StateCombinator {
                plan: self.state_plan,
            },
        )?)
    }

    pub fn build_with_multi_arg_input(
        self,
        features: FunctionFeatures,
        build: MultiArgBuildFn<StateCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        let request = if let Some(args_type) = &self.args_type {
            request_with_args_type(&self.request, args_type, false)
        } else {
            self.request
        };
        let combinator_args_type = request.args_type;
        build(MultiArgBuildContext::new(
            request,
            signature_args_type,
            combinator_args_type,
            features,
            StateCombinator {
                plan: self.state_plan,
            },
        ))
    }

    pub fn build_with_direct_input(
        self,
        features: FunctionFeatures,
        build: DirectBuildFn<StateCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        let request = if let Some(args_type) = &self.args_type {
            request_with_args_type(&self.request, args_type, false)
        } else {
            self.request
        };
        let combinator_args_type = request.args_type;
        build(DirectBuildContext::new(
            request,
            signature_args_type,
            combinator_args_type,
            features,
            StateCombinator {
                plan: self.state_plan,
            },
        ))
    }
}

impl<'a> DistinctSkipNullStep<'a> {
    pub fn build_with_unary_input(
        self,
        features: FunctionFeatures,
        build: UnaryBuildFn<DistinctCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        let request = request_with_args_type(&self.request, &self.args_type, true);
        let combinator_args_type = request.args_type;
        build(UnaryBuildContext::new(
            request,
            signature_args_type,
            combinator_args_type,
            features,
            DistinctCombinator {
                null_policy: self.null_policy,
            },
        )?)
    }

    pub fn build_with_multi_arg_input(
        self,
        features: FunctionFeatures,
        build: MultiArgBuildFn<DistinctCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        let request = request_with_args_type(&self.request, &self.args_type, true);
        let combinator_args_type = request.args_type;
        build(MultiArgBuildContext::new(
            request,
            signature_args_type,
            combinator_args_type,
            features,
            DistinctCombinator {
                null_policy: self.null_policy,
            },
        ))
    }

    pub fn build_with_direct_input(
        self,
        features: FunctionFeatures,
        build: DirectBuildFn<DistinctCombinator>,
    ) -> Result<AggregateFunctionRef> {
        let signature_args_type = self.request.args_type;
        let request = request_with_args_type(&self.request, &self.args_type, true);
        let combinator_args_type = request.args_type;
        build(DirectBuildContext::new(
            request,
            signature_args_type,
            combinator_args_type,
            features,
            DistinctCombinator {
                null_policy: self.null_policy,
            },
        ))
    }
}

fn request_with_args_type<'a, 'b>(
    request: &'b AggregateFunctionRequest<'a>,
    args_type: &'b [DataType],
    strip_distinct: bool,
) -> AggregateFunctionRequest<'b> {
    AggregateFunctionRequest {
        name: request.name,
        params: request.params,
        args_type,
        distinct: if strip_distinct {
            false
        } else {
            request.distinct
        },
        order_by: request.order_by,
    }
}

fn matches_name(names: &[&str], name: &str) -> bool {
    names
        .iter()
        .any(|candidate| candidate.eq_ignore_ascii_case(name))
}

fn matches_suffixed_name(base_names: &[&str], suffix: &str, name: &str) -> bool {
    base_names.iter().any(|candidate| {
        let Some(name_prefix) = strip_suffix_ignore_ascii_case(name, suffix) else {
            return false;
        };
        let Some(base_name) = name_prefix.strip_suffix('_') else {
            return false;
        };
        candidate.eq_ignore_ascii_case(base_name)
    })
}

fn strip_suffix_ignore_ascii_case<'a>(name: &'a str, suffix: &str) -> Option<&'a str> {
    if name.len() < suffix.len() {
        return None;
    }

    let (prefix, name_suffix) = name.split_at(name.len() - suffix.len());
    if name_suffix.eq_ignore_ascii_case(suffix) {
        Some(prefix)
    } else {
        None
    }
}
