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
use super::AggregateMetadata;
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
use super::input_rows::InputRowsEval;
use super::input_rows::PRESERVE_V1_INPUT_ROWS_FLAG;

fn build_signature(request: &RawAggregateCall<'_>, return_type: DataType) -> AggregateSignature {
    AggregateSignature {
        name: request.name.to_string(),
        params: request.params.to_vec(),
        args_type: request.args_type.to_vec(),
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
        input_types: &'a [DataType],
        metadata: AggregateMetadata,
        combinator: C,
    ) -> Result<Self> {
        let [arg_type] = input_types else {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects exactly one argument, got {}",
                request.name,
                input_types.len()
            )));
        };
        Ok(Self {
            call: request,
            metadata,
            combinator,
            input_type: arg_type.remove_nullable(),
        })
    }

    pub(crate) fn name(&self) -> &str {
        self.call.name
    }

    pub(crate) fn params(&self) -> &[databend_common_expression::Scalar] {
        self.call.params
    }

    pub(crate) fn arg_type(&self) -> &DataType {
        &self.input_type
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
        // Native nullable results (stddev) still had v1's outer OrNull flag,
        // even for non-nullable input. Keep the kernel's own empty-state logic.
        let input_rows_flag = return_type.is_nullable();
        let state = if input_rows_flag {
            state.with_null_flag()
        } else {
            state
        };
        // Native nullable kernels need to see all input rows before filtering
        // NULLs themselves, including through _state and IF input adaptors.
        let combinator = if input_rows_flag {
            self.combinator.with_native_null_input()
        } else {
            self.combinator
        };
        let signature = build_signature(&self.call, return_type);
        if signature.args_type[0].is_nullable_or_null() {
            let eval =
                UnaryEvalAdapter::new(UnaryStateEval::<S, I, R, true>::new(function_info.into()));
            combinator.create::<false>(
                signature,
                self.metadata,
                state,
                InputRowsEval::new(eval, input_rows_flag),
            )
        } else {
            let eval =
                UnaryEvalAdapter::new(UnaryStateEval::<S, I, R, false>::new(function_info.into()));
            combinator.create::<false>(
                signature,
                self.metadata,
                state,
                InputRowsEval::new(eval, input_rows_flag),
            )
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
        let signature = build_signature(&self.call, return_type);
        let nested = UnaryStateEval::<S, I, R, false>::new(Arc::new(function_info));
        let eval = UnaryEvalAdapter::new(UnaryOrNull::new(nested));
        let input_rows_flag =
            PRESERVE_V1_INPUT_ROWS_FLAG && self.call.args_type.iter().any(DataType::is_nullable);
        let state = state.with_null_flag();
        let state = if input_rows_flag {
            state.with_null_flag()
        } else {
            state
        };
        let eval = InputRowsEval::new(eval, input_rows_flag);
        self.combinator
            .create::<false>(signature, self.metadata, state, eval)
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
        let signature = build_signature(&self.call, return_type);
        let eval = UnaryEvalAdapter::new(UnaryOrNull::new(eval));
        let input_rows_flag =
            PRESERVE_V1_INPUT_ROWS_FLAG && self.call.args_type.iter().any(DataType::is_nullable);
        let state = state.with_null_flag();
        let state = if input_rows_flag {
            state.with_null_flag()
        } else {
            state
        };
        let eval = InputRowsEval::new(eval, input_rows_flag);
        self.combinator
            .create::<false>(signature, self.metadata, state, eval)
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
        let signature = build_signature(&self.call, return_type);
        let distinct_args_type = vec![self.input_type.clone()];
        super::create_unary_distinct_or_null_aggregate_function::<S, I, R, _>(
            self.combinator,
            signature,
            self.metadata,
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
        input_types: &'a [DataType],
        metadata: AggregateMetadata,
        combinator: C,
    ) -> Self {
        let input_types = input_types.iter().map(DataType::remove_nullable).collect();
        Self {
            call: request,
            metadata,
            combinator,
            input_types,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.call.name
    }

    pub(crate) fn params(&self) -> &[databend_common_expression::Scalar] {
        self.call.params
    }

    pub(crate) fn args_type(&self) -> &[DataType] {
        &self.input_types
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
        let signature = build_signature(&self.call, return_type);
        debug_assert!(signature.order_by.is_empty());
        let input_rows_flag =
            PRESERVE_V1_INPUT_ROWS_FLAG && self.call.args_type.iter().any(DataType::is_nullable);
        let state = state.with_null_flag();
        let state = if input_rows_flag {
            state.with_null_flag()
        } else {
            state
        };
        self.combinator.create::<false>(
            signature,
            self.metadata,
            state,
            InputRowsEval::new(MultiArgOrNullEval::new(eval), input_rows_flag),
        )
    }
}

impl<'a, C> DirectBuildContext<'a, C>
where C: Combinator
{
    pub(super) fn new(
        request: RawAggregateCall<'a>,
        input_types: &'a [DataType],
        metadata: AggregateMetadata,
        combinator: C,
    ) -> Self {
        Self {
            call: request,
            metadata,
            combinator,
            input_types,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.call.name
    }

    pub(crate) fn params(&self) -> &[databend_common_expression::Scalar] {
        self.call.params
    }

    pub(crate) fn args_type(&self) -> &[DataType] {
        self.input_types
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
        let signature = build_signature(&self.call, return_type);
        self.combinator
            .create::<false>(signature, self.metadata, state, eval)
    }

    /// Native nullable kernels keep v1's input-presence flag while retaining
    /// ownership of nullable inputs and non-empty result semantics.
    pub(crate) fn create_native_nullable<I: AggregateEval>(
        self,
        return_type: DataType,
        state: AggregateStateDescription,
        eval: I,
    ) -> Result<AggregateCallRef> {
        debug_assert!(return_type.is_nullable());
        let enabled = PRESERVE_V1_INPUT_ROWS_FLAG;
        let state = if enabled {
            state.with_null_flag()
        } else {
            state
        };
        let combinator = self.combinator.with_native_null_input();
        combinator.create::<false>(
            build_signature(&self.call, return_type),
            self.metadata,
            state,
            InputRowsEval::new(eval, enabled),
        )
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
        // string_agg owns its non-null flag but still needs v1's outer flag.
        // Inspect the original call: _state may already have stripped input NULLs.
        let input_rows_flag = PRESERVE_V1_INPUT_ROWS_FLAG
            && return_type.is_nullable()
            && self.call.args_type.iter().any(DataType::is_nullable);
        let state = if input_rows_flag {
            state.with_null_flag()
        } else {
            state
        };
        let signature = build_signature(&self.call, return_type);
        self.combinator.create::<true>(
            signature,
            self.metadata,
            state,
            InputRowsEval::new(eval, input_rows_flag),
        )
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::UInt64Type;

    use super::*;
    use crate::aggregates::function_impl::adaptors::*;
    use crate::aggregates::function_impl::sum::AggregateSumUInt64State;

    fn build_probe<C: Combinator>(build: UnaryBuildContext<'_, C>) -> Result<AggregateCallRef> {
        // The implementation sees one non-null input while the complete call
        // retains its nullable argument and, for IF, its condition argument.
        assert_eq!(build.arg_type(), &UInt64Type::data_type());
        if build.call.name == "contract_probe" {
            assert_eq!(
                build.metadata.documentation.description,
                "intrinsic aggregate"
            );
        }
        if build.call.name.ends_with("_if") {
            assert_eq!(build.call.args_type, &[
                UInt64Type::data_type().wrap_nullable(),
                DataType::Boolean,
            ]);
        } else {
            assert_eq!(build.call.args_type.len(), 1);
        }
        build.create_unary_or_null::<AggregateSumUInt64State, UInt64Type, UInt64Type>(
            UInt64Type::data_type().wrap_nullable(),
            AggregateSumUInt64State::state_description(UInt64Type::data_type()),
            (),
        )
    }

    #[test]
    fn test_external_contract_survives_internal_build() -> Result<()> {
        let base_metadata = AggregateMetadata {
            documentation: AggregateDocumentation {
                description: "intrinsic aggregate",
                ..Default::default()
            },
            eager_aggregation: EagerAggregation::Sum,
            ..Default::default()
        };
        let descriptors = NameRoute::new(
            &["contract_probe"],
            ArgumentsPattern::fixed(vec![ArgumentPattern::any_numeric()]),
            base_metadata,
            NullInput::Filter,
        )
        .then(PlainRoute::unary(build_probe))
        .then(
            IfRoute::unary(build_probe).with_metadata(AggregateMetadata {
                eager_aggregation: EagerAggregation::Sum,
                documentation: AggregateDocumentation {
                    description: "intrinsic IF variant",
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
        .then(
            StateRoute::unary(build_probe).with_metadata(AggregateMetadata {
                eager_aggregation: EagerAggregation::Sum,
                documentation: AggregateDocumentation {
                    description: "intrinsic STATE variant",
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
        .then(DistinctRoute::<true>::unary(build_probe))
        .then(MergeRoute::unary(false, build_probe))
        .then(MergeRoute::unary(true, build_probe))
        .into_descriptors();
        let mut registry = AggregateRegistry::empty();
        for descriptor in descriptors {
            registry.register(descriptor);
        }

        let resolve = |name: &str, args_type: &[DataType]| -> Result<AggregateCallRef> {
            let call = registry.resolve(RawAggregateCall {
                name,
                params: &[],
                args_type,
                distinct: false,
                order_by: &[],
            })?;
            assert_eq!(call.signature().name, name);
            assert_eq!(call.signature().args_type, args_type);
            let expected_description = if name.ends_with("_if") {
                "intrinsic IF variant"
            } else if name.ends_with("_state") && !name.ends_with("_merge_state") {
                "intrinsic STATE variant"
            } else {
                "intrinsic aggregate"
            };
            assert_eq!(call.features().description, expected_description);
            let expected_strategy = if name == "contract_probe" {
                EagerAggregation::Sum
            } else {
                EagerAggregation::Unsupported
            };
            assert_eq!(call.features().eager_aggregation, expected_strategy);
            assert_eq!(
                registry
                    .descriptor(name)
                    .unwrap()
                    .features()
                    .eager_aggregation,
                expected_strategy
            );
            if name.ends_with("_merge") || name.ends_with("_merge_state") {
                assert_eq!(call.features().distinct_policy, DistinctPolicy::Unsupported);
            }
            Ok(call)
        };
        let nullable = UInt64Type::data_type().wrap_nullable();
        resolve("contract_probe", std::slice::from_ref(&nullable))?;
        resolve("contract_probe_if", &[nullable.clone(), DataType::Boolean])?;
        resolve("contract_probe_distinct", std::slice::from_ref(&nullable))?;
        let state = resolve("contract_probe_state", std::slice::from_ref(&nullable))?;
        resolve(
            "contract_probe_merge",
            std::slice::from_ref(&state.signature().return_type),
        )?;
        resolve(
            "contract_probe_merge_state",
            std::slice::from_ref(&state.signature().return_type),
        )?;
        // Short-circuits must keep the same external metadata as normal builds.
        resolve("contract_probe", &[DataType::Null])?;
        resolve("contract_probe_if", &[DataType::Null, DataType::Boolean])?;
        resolve("contract_probe_state", &[DataType::Null])?;
        Ok(())
    }
}
