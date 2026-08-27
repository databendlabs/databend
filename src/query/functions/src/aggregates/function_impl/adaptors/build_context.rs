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
use databend_common_expression::aggregate::aggregate_function::AggregateCallRef;
use databend_common_expression::aggregate::aggregate_function::RawAggregateCall;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;

use super::AggregateEval;
use super::AggregateFeatures;
use super::AggregateSignature;
use super::AggregateStateDescription;
use super::Combinator;
use super::DirectBuildContext;
use super::MultiArgBuildContext;
use super::MultiArgOrNullEval;
use super::UnaryBuildContext;
use super::UnaryEval;
use super::UnaryEvalAdapter;
use super::UnaryOrNull;
use super::UnaryState;
use super::UnaryStateEval;

fn build_signature(
    request: &RawAggregateCall<'_>,
    signature_args_type: &[DataType],
    return_type: DataType,
) -> AggregateSignature {
    AggregateSignature {
        name: request.name.to_string(),
        params: request.params.to_vec(),
        args_type: signature_args_type.to_vec(),
        distinct: request.distinct,
        order_by: request.order_by.to_vec(),
        return_type,
    }
}

impl<'a, C> UnaryBuildContext<'a, C>
where C: Combinator
{
    pub(super) fn new(
        request: RawAggregateCall<'a>,
        signature_args_type: &'a [DataType],
        features: AggregateFeatures,
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
    ) -> Result<AggregateCallRef>
    where
        S: UnaryState<I, R>,
        I: AccessType,
        R: ValueType,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        if signature.args_type[0].is_nullable_or_null() {
            let eval =
                UnaryEvalAdapter::new(UnaryStateEval::<S, I, R, true>::new(function_info.into()));
            self.combinator
                .create::<false>(signature, self.features, state, eval)
        } else {
            let eval =
                UnaryEvalAdapter::new(UnaryStateEval::<S, I, R, false>::new(function_info.into()));
            self.combinator
                .create::<false>(signature, self.features, state, eval)
        }
    }

    pub(crate) fn create_unary_or_null<S, I, R>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        function_info: S::FunctionInfo,
    ) -> Result<AggregateCallRef>
    where
        S: UnaryState<I, R>,
        I: AccessType,
        R: ValueType,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        let nested = UnaryStateEval::<S, I, R, false>::new(Arc::new(function_info));
        let eval = UnaryEvalAdapter::new(UnaryOrNull::new(nested));
        let state = state.with_null_flag();
        self.combinator
            .create::<false>(signature, self.features, state, eval)
    }

    pub(crate) fn create_unary_or_null_with_eval<I, R, U>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        eval: U,
    ) -> Result<AggregateCallRef>
    where
        I: AccessType,
        R: ValueType,
        U: UnaryEval<I, R>,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        let eval = UnaryEvalAdapter::new(UnaryOrNull::new(eval));
        let state = state.with_null_flag();
        self.combinator
            .create::<false>(signature, self.features, state, eval)
    }

    pub(crate) fn create_unary_distinct_or_null<S, I, R>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        function_info: S::FunctionInfo,
    ) -> Result<AggregateCallRef>
    where
        S: UnaryState<I, R>,
        I: AccessType,
        R: ValueType,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        let distinct_args_type = self
            .request
            .args_type
            .iter()
            .map(DataType::remove_nullable)
            .collect();
        super::create_unary_distinct_or_null_aggregate_function::<S, I, R, _>(
            self.combinator,
            signature,
            self.features,
            state,
            function_info,
            distinct_args_type,
        )
    }
}

impl<'a, C> MultiArgBuildContext<'a, C>
where C: Combinator
{
    pub(super) fn new(
        request: RawAggregateCall<'a>,
        signature_args_type: &'a [DataType],
        features: AggregateFeatures,
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
        eval: I,
    ) -> Result<AggregateCallRef>
    where
        I: AggregateEval,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        debug_assert!(signature.order_by.is_empty());
        self.combinator.create::<false>(
            signature,
            self.features,
            state.with_null_flag(),
            MultiArgOrNullEval::new(eval),
        )
    }
}

impl<'a, C> DirectBuildContext<'a, C>
where C: Combinator
{
    pub(super) fn new(
        request: RawAggregateCall<'a>,
        signature_args_type: &'a [DataType],
        features: AggregateFeatures,
        combinator: C,
    ) -> Self {
        Self {
            request,
            signature_args_type,
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
        eval: I,
    ) -> Result<AggregateCallRef>
    where
        I: AggregateEval,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        debug_assert!(signature.order_by.is_empty());
        self.combinator
            .create::<false>(signature, self.features, state, eval)
    }

    pub(crate) fn create_ordered<I>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        eval: I,
    ) -> Result<AggregateCallRef>
    where
        I: AggregateEval,
    {
        let signature = build_signature(&self.request, self.signature_args_type, return_type);
        self.combinator
            .create::<true>(signature, self.features, state, eval)
    }
}
