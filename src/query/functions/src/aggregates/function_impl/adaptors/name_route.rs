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
use databend_common_expression::Scalar;
use databend_common_expression::aggregate_function::EagerAggregation;
use databend_common_expression::types::DataType;

use super::AggregateCallRef;
use super::ArgumentPattern;
use super::ArgumentsPattern;
use super::Combinator;
use super::DirectBuildContext;
use super::DirectBuildFn;
use super::DistinctCombinator;
use super::IfCombinator;
use super::LegacySignatureResolver;
use super::MultiArgBuildContext;
use super::MultiArgBuildFn;
use super::NullInput;
use super::PlainCombinator;
use super::RawAggregateCall;
use super::StateCombinator;
use super::StateCombinatorPlan;
use super::UnaryBuildContext;
use super::UnaryBuildFn;
use super::merge_combinator;
use super::state_combinator;
use super::try_create_null_argument_result_function;

mod metadata;
mod registration;

pub(crate) use metadata::AggregateDocumentation;
pub(crate) use metadata::AggregateMetadata;
pub(crate) use metadata::NullArgumentResult;

/// An ordered sequence of direct aggregate name routes.
///
/// Registration metadata is fixed when the route is created. Each route node
/// provides the metadata transformation for its own descriptor.
pub(crate) struct NameRoute {
    names: &'static [&'static str],
    arguments: ArgumentsPattern,
    metadata: AggregateMetadata,
    distinct_target: Option<String>,
    null_input: NullInput,
    validate: Option<DirectRouteValidateFn>,
    routes: Vec<Box<dyn RouteNode>>,
}

type DirectRouteValidateFn = for<'a> fn(&RawAggregateCall<'a>) -> Result<()>;

pub(crate) struct DirectRouteContext<'request, 'route> {
    request: RawAggregateCall<'request>,
    names: &'route [&'route str],
    arguments: &'route ArgumentsPattern,
    metadata: &'route AggregateMetadata,
    null_input: NullInput,
}

pub(crate) trait RouteNode: Send + Sync {
    fn suffix(&self) -> Option<&'static str> {
        None
    }

    fn arguments(&self, base: &ArgumentsPattern) -> ArgumentsPattern {
        base.clone()
    }

    fn metadata(&self, base: &AggregateMetadata) -> AggregateMetadata {
        *base
    }

    fn distinct_target(&self, _base_name: &str) -> Option<String> {
        None
    }

    fn distinct_is_idempotent(&self) -> bool {
        false
    }

    fn try_build(&self, context: &DirectRouteContext<'_, '_>) -> Result<Option<AggregateCallRef>>;
}

impl NameRoute {
    pub(crate) fn new(
        names: &'static [&'static str],
        arguments: ArgumentsPattern,
        metadata: AggregateMetadata,
        null_input: NullInput,
    ) -> Self {
        assert!(!names.is_empty(), "a direct name route requires a name");
        Self {
            names,
            arguments,
            metadata,
            distinct_target: None,
            null_input,
            validate: None,
            routes: Vec::new(),
        }
    }

    pub(crate) fn with_validator(mut self, validate: DirectRouteValidateFn) -> Self {
        self.validate = Some(validate);
        self
    }

    pub(crate) fn with_distinct_target(mut self, target: impl Into<String>) -> Self {
        self.distinct_target = Some(target.into());
        self
    }

    pub(crate) fn then(mut self, next: impl RouteNode + 'static) -> Self {
        self.routes.push(Box::new(next));
        self
    }

    pub(crate) fn build(&self, request: RawAggregateCall<'_>) -> Result<AggregateCallRef> {
        if let Some(validate) = self.validate {
            validate(&request)?;
        }
        let context = DirectRouteContext {
            request,
            names: self.names,
            arguments: &self.arguments,
            metadata: &self.metadata,
            null_input: self.null_input,
        };
        for route in &self.routes {
            if let Some(function) = route.try_build(&context)? {
                return Ok(function);
            }
        }
        Err(ErrorCode::UnknownAggregateFunction(format!(
            "Unsupported AggregateFunction: {}",
            context.request.name
        )))
    }
}

fn suffixed_name(name: &str, suffix: Option<&str>) -> String {
    match suffix {
        Some(suffix) => format!("{}_{}", name, suffix),
        None => name.to_string(),
    }
}

impl DirectRouteContext<'_, '_> {
    fn matching_name_index(&self, suffix: Option<&str>) -> Option<usize> {
        let name = match suffix {
            Some(suffix) => {
                let name_prefix = strip_suffix_ignore_ascii_case(self.request.name, suffix)?;
                name_prefix.strip_suffix('_')?
            }
            None => self.request.name,
        };
        self.names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))
    }
}

enum RouteBuild<C> {
    Unary(UnaryBuildFn<C>),
    MultiArg(MultiArgBuildFn<C>),
    Direct(DirectBuildFn<C>),
}

impl<C: Combinator> RouteBuild<C> {
    fn null_argument_mode(&self) -> NullArgumentMode {
        match self {
            Self::MultiArg(_) => NullArgumentMode::Any,
            Self::Unary(_) | Self::Direct(_) => NullArgumentMode::Only,
        }
    }

    fn build<'a>(
        &self,
        request: RawAggregateCall<'a>,
        input_types: &'a [DataType],
        metadata: AggregateMetadata,
        combinator: C,
    ) -> Result<AggregateCallRef> {
        match self {
            Self::Unary(build) => build(UnaryBuildContext::new(
                request,
                input_types,
                metadata,
                combinator,
            )?),
            Self::MultiArg(build) => build(MultiArgBuildContext::new(
                request,
                input_types,
                metadata,
                combinator,
            )),
            Self::Direct(build) => build(DirectBuildContext::new(
                request,
                input_types,
                metadata,
                combinator,
            )),
        }
    }
}

#[derive(Clone, Copy)]
enum NullArgumentMode {
    Only,
    Any,
}

fn null_argument_result(
    request: &RawAggregateCall<'_>,
    metadata: &AggregateMetadata,
    mode: NullArgumentMode,
) -> Result<Option<AggregateCallRef>> {
    let has_null_argument = match mode {
        NullArgumentMode::Only => matches!(request.args_type, [DataType::Null]),
        NullArgumentMode::Any => request.args_type.iter().any(DataType::is_null),
    };
    has_null_argument
        .then(|| try_create_null_argument_result_function(request.clone(), *metadata))
        .transpose()
}

fn strip_suffix_ignore_ascii_case<'a>(name: &'a str, suffix: &str) -> Option<&'a str> {
    if name.len() < suffix.len() {
        return None;
    }

    let (prefix, name_suffix) = name.split_at(name.len() - suffix.len());
    name_suffix.eq_ignore_ascii_case(suffix).then_some(prefix)
}

pub(crate) struct MergeRoute {
    returns_state: bool,
    build: RouteBuild<PlainCombinator>,
    legacy_signature_resolver: Option<LegacySignatureResolver>,
}

impl MergeRoute {
    pub(crate) fn new(returns_state: bool, build: DirectBuildFn<PlainCombinator>) -> Self {
        Self {
            returns_state,
            build: RouteBuild::Direct(build),
            legacy_signature_resolver: None,
        }
    }

    pub(crate) fn unary(returns_state: bool, build: UnaryBuildFn<PlainCombinator>) -> Self {
        Self {
            returns_state,
            build: RouteBuild::Unary(build),
            legacy_signature_resolver: None,
        }
    }

    pub(crate) fn multi_arg(returns_state: bool, build: MultiArgBuildFn<PlainCombinator>) -> Self {
        Self {
            returns_state,
            build: RouteBuild::MultiArg(build),
            legacy_signature_resolver: None,
        }
    }

    pub(crate) fn with_legacy_signature_resolver(
        mut self,
        resolver: LegacySignatureResolver,
    ) -> Self {
        self.legacy_signature_resolver = Some(resolver);
        self
    }
}

impl RouteNode for MergeRoute {
    fn metadata(&self, base: &AggregateMetadata) -> AggregateMetadata {
        AggregateMetadata {
            eager_aggregation: EagerAggregation::Unsupported,
            ..*base
        }
    }

    fn suffix(&self) -> Option<&'static str> {
        Some(if self.returns_state {
            "merge_state"
        } else {
            "merge"
        })
    }

    fn arguments(&self, _base: &ArgumentsPattern) -> ArgumentsPattern {
        ArgumentsPattern::fixed(vec![ArgumentPattern::any()])
    }

    fn try_build(&self, context: &DirectRouteContext<'_, '_>) -> Result<Option<AggregateCallRef>> {
        let suffix = if self.returns_state {
            "merge_state"
        } else {
            "merge"
        };
        let Some(matched_name_index) = context.matching_name_index(Some(suffix)) else {
            return Ok(None);
        };
        let request = context.request.clone();
        let nested_name = context.names[matched_name_index];
        let order_by = request.order_by;
        let null_argument_mode = self.build.null_argument_mode();
        let metadata = *context.metadata;
        let nested_build = |params: &[Scalar], args_type: &[DataType]| {
            let nested_request = RawAggregateCall {
                name: nested_name,
                params,
                args_type,
                distinct: false,
                order_by,
            };
            if context.null_input != NullInput::Native
                && let Some(function) =
                    null_argument_result(&nested_request, &metadata, null_argument_mode)?
            {
                return Ok(function);
            }
            self.build
                .build(nested_request, args_type, metadata, PlainCombinator)
        };
        merge_combinator::create(
            request,
            self.metadata(context.metadata),
            nested_name,
            context.names,
            context.arguments,
            self.legacy_signature_resolver,
            &nested_build,
            self.returns_state,
        )
        .map(Some)
    }
}

pub(crate) struct PlainRoute {
    validate: Option<DirectRouteValidateFn>,
    build: RouteBuild<PlainCombinator>,
}

impl PlainRoute {
    pub(crate) fn new(build: DirectBuildFn<PlainCombinator>) -> Self {
        Self {
            validate: None,
            build: RouteBuild::Direct(build),
        }
    }

    pub(crate) fn unary(build: UnaryBuildFn<PlainCombinator>) -> Self {
        Self {
            validate: None,
            build: RouteBuild::Unary(build),
        }
    }

    pub(crate) fn multi_arg(build: MultiArgBuildFn<PlainCombinator>) -> Self {
        Self {
            validate: None,
            build: RouteBuild::MultiArg(build),
        }
    }

    pub(crate) fn with_validator(mut self, validate: DirectRouteValidateFn) -> Self {
        self.validate = Some(validate);
        self
    }
}

impl RouteNode for PlainRoute {
    fn try_build(&self, context: &DirectRouteContext<'_, '_>) -> Result<Option<AggregateCallRef>> {
        if context.matching_name_index(None).is_none() {
            return Ok(None);
        }
        if let Some(validate) = self.validate {
            validate(&context.request)?;
        }
        let metadata = *context.metadata;
        if context.null_input != NullInput::Native
            && let Some(function) = null_argument_result(
                &context.request,
                context.metadata,
                self.build.null_argument_mode(),
            )?
        {
            return Ok(Some(function));
        }
        let request = context.request.clone();
        let args_type = request.args_type;
        let function = self
            .build
            .build(request, args_type, metadata, PlainCombinator)?;
        Ok(Some(function))
    }
}

pub(crate) struct IfRoute {
    metadata: Option<AggregateMetadata>,
    build: RouteBuild<IfCombinator>,
}

impl IfRoute {
    pub(crate) fn direct(build: DirectBuildFn<IfCombinator>) -> Self {
        Self {
            metadata: None,
            build: RouteBuild::Direct(build),
        }
    }

    pub(crate) fn unary(build: UnaryBuildFn<IfCombinator>) -> Self {
        Self {
            metadata: None,
            build: RouteBuild::Unary(build),
        }
    }

    pub(crate) fn multi_arg(build: MultiArgBuildFn<IfCombinator>) -> Self {
        Self {
            metadata: None,
            build: RouteBuild::MultiArg(build),
        }
    }

    pub(crate) fn with_metadata(mut self, metadata: AggregateMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

impl RouteNode for IfRoute {
    fn suffix(&self) -> Option<&'static str> {
        Some("if")
    }

    fn arguments(&self, base: &ArgumentsPattern) -> ArgumentsPattern {
        ArgumentsPattern::if_condition(base.clone())
    }

    fn metadata(&self, base: &AggregateMetadata) -> AggregateMetadata {
        AggregateMetadata {
            eager_aggregation: EagerAggregation::Unsupported,
            ..self.metadata.unwrap_or(*base)
        }
    }

    fn try_build(&self, context: &DirectRouteContext<'_, '_>) -> Result<Option<AggregateCallRef>> {
        if context.matching_name_index(Some("if")).is_none() {
            return Ok(None);
        }
        let native_null_input = context.null_input == NullInput::Native;
        if !native_null_input
            && let Some(function) = null_argument_result(
                &context.request,
                &self.metadata(context.metadata),
                NullArgumentMode::Any,
            )?
        {
            return Ok(Some(function));
        }

        let Some((condition_type, nested_arg_types)) = context.request.args_type.split_last()
        else {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects a condition argument",
                context.request.name
            )));
        };
        let condition_index = nested_arg_types.len();
        let condition_type = condition_type.remove_nullable();
        if !condition_type.is_null() && condition_type != DataType::Boolean {
            return Err(ErrorCode::BadArguments(format!(
                "The type of the last argument for {} must be boolean type, but got {:?}",
                context.request.name,
                context.request.args_type[context.request.args_type.len() - 1]
            )));
        }
        let args_type = if native_null_input {
            nested_arg_types.to_vec()
        } else {
            nested_arg_types
                .iter()
                .map(DataType::remove_nullable)
                .collect()
        };
        let metadata = self.metadata(context.metadata);
        let request = context.request.clone();
        let function = self
            .build
            .build(request, &args_type, metadata, IfCombinator {
                nested_args_type: args_type.clone(),
                condition_index,
                always_false: condition_type.is_null(),
                strip_nullable_input: !native_null_input,
            })?;
        Ok(Some(function))
    }
}

pub(crate) struct StateRoute {
    arguments: Option<ArgumentsPattern>,
    metadata: Option<AggregateMetadata>,
    build: RouteBuild<StateCombinator>,
}

impl StateRoute {
    pub(crate) fn direct(build: DirectBuildFn<StateCombinator>) -> Self {
        Self {
            arguments: None,
            metadata: None,
            build: RouteBuild::Direct(build),
        }
    }

    pub(crate) fn unary(build: UnaryBuildFn<StateCombinator>) -> Self {
        Self {
            arguments: None,
            metadata: None,
            build: RouteBuild::Unary(build),
        }
    }

    pub(crate) fn multi_arg(build: MultiArgBuildFn<StateCombinator>) -> Self {
        Self {
            arguments: None,
            metadata: None,
            build: RouteBuild::MultiArg(build),
        }
    }

    pub(crate) fn with_arguments(mut self, arguments: ArgumentsPattern) -> Self {
        self.arguments = Some(arguments);
        self
    }

    pub(crate) fn with_metadata(mut self, metadata: AggregateMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

impl RouteNode for StateRoute {
    fn suffix(&self) -> Option<&'static str> {
        Some("state")
    }

    fn arguments(&self, base: &ArgumentsPattern) -> ArgumentsPattern {
        self.arguments.as_ref().unwrap_or(base).clone()
    }

    fn metadata(&self, base: &AggregateMetadata) -> AggregateMetadata {
        AggregateMetadata {
            eager_aggregation: EagerAggregation::Unsupported,
            ..self.metadata.unwrap_or(*base)
        }
    }

    fn try_build(&self, context: &DirectRouteContext<'_, '_>) -> Result<Option<AggregateCallRef>> {
        if context.matching_name_index(Some("state")).is_none() {
            return Ok(None);
        }
        let state_plan = if context.null_input == NullInput::Native {
            StateCombinatorPlan::default()
        } else {
            if context.request.args_type.iter().any(DataType::is_null) {
                return Ok(Some(state_combinator::create_state_null_result_function(
                    context.request.clone(),
                    self.metadata(context.metadata),
                )?));
            }
            let strip_nullable_input = context
                .request
                .args_type
                .iter()
                .any(|data_type| matches!(data_type, DataType::Nullable(_)));
            StateCombinatorPlan {
                strip_nullable_input,
                // The nested aggregate state already records whether it has
                // seen a non-null value. Adding a second flag here would make
                // `_state` incompatible with the nested state rebuilt by
                // `_merge`.
                nullable_input_result_flag: false,
            }
        };
        let metadata = self.metadata(context.metadata);
        let args_type = state_plan.strip_nullable_input.then(|| {
            context
                .request
                .args_type
                .iter()
                .map(DataType::remove_nullable)
                .collect::<Vec<_>>()
        });
        let input_types = args_type.as_deref().unwrap_or(context.request.args_type);
        let request = context.request.clone();
        let function = self
            .build
            .build(request, input_types, metadata, StateCombinator {
                plan: state_plan,
            })?;
        Ok(Some(function))
    }
}

pub(crate) struct DistinctAliasRoute {
    build: RouteBuild<PlainCombinator>,
}

impl DistinctAliasRoute {
    pub(crate) fn direct(build: DirectBuildFn<PlainCombinator>) -> Self {
        Self {
            build: RouteBuild::Direct(build),
        }
    }

    pub(crate) fn unary(build: UnaryBuildFn<PlainCombinator>) -> Self {
        Self {
            build: RouteBuild::Unary(build),
        }
    }

    pub(crate) fn multi_arg(build: MultiArgBuildFn<PlainCombinator>) -> Self {
        Self {
            build: RouteBuild::MultiArg(build),
        }
    }
}

impl RouteNode for DistinctAliasRoute {
    fn suffix(&self) -> Option<&'static str> {
        Some("distinct")
    }

    fn distinct_is_idempotent(&self) -> bool {
        true
    }

    fn try_build(&self, context: &DirectRouteContext<'_, '_>) -> Result<Option<AggregateCallRef>> {
        if context.matching_name_index(Some("distinct")).is_none() {
            return Ok(None);
        }
        if context.null_input != NullInput::Native
            && let Some(function) = null_argument_result(
                &context.request,
                context.metadata,
                self.build.null_argument_mode(),
            )?
        {
            return Ok(Some(function));
        }
        let metadata = *context.metadata;
        let request = context.request.clone();
        let args_type = request.args_type;
        let function = self
            .build
            .build(request, args_type, metadata, PlainCombinator)?;
        Ok(Some(function))
    }
}

pub(crate) struct DistinctRoute<const SKIP_NULLS: bool> {
    build: RouteBuild<DistinctCombinator<SKIP_NULLS>>,
}

impl<const SKIP_NULLS: bool> DistinctRoute<SKIP_NULLS> {
    pub(crate) fn direct(build: DirectBuildFn<DistinctCombinator<SKIP_NULLS>>) -> Self {
        Self {
            build: RouteBuild::Direct(build),
        }
    }

    pub(crate) fn unary(build: UnaryBuildFn<DistinctCombinator<SKIP_NULLS>>) -> Self {
        Self {
            build: RouteBuild::Unary(build),
        }
    }

    pub(crate) fn multi_arg(build: MultiArgBuildFn<DistinctCombinator<SKIP_NULLS>>) -> Self {
        Self {
            build: RouteBuild::MultiArg(build),
        }
    }
}

impl<const SKIP_NULLS: bool> RouteNode for DistinctRoute<SKIP_NULLS> {
    fn metadata(&self, base: &AggregateMetadata) -> AggregateMetadata {
        AggregateMetadata {
            eager_aggregation: EagerAggregation::Unsupported,
            ..*base
        }
    }

    fn suffix(&self) -> Option<&'static str> {
        Some("distinct")
    }

    fn distinct_target(&self, base_name: &str) -> Option<String> {
        Some(suffixed_name(base_name, self.suffix()))
    }

    fn try_build(&self, context: &DirectRouteContext<'_, '_>) -> Result<Option<AggregateCallRef>> {
        if context.matching_name_index(Some("distinct")).is_none() {
            return Ok(None);
        }
        if matches!(self.build, RouteBuild::MultiArg(_))
            && let Some(function) = null_argument_result(
                &context.request,
                &self.metadata(context.metadata),
                NullArgumentMode::Any,
            )?
        {
            return Ok(Some(function));
        }
        let metadata = self.metadata(context.metadata);
        let args_type = context
            .request
            .args_type
            .iter()
            .map(DataType::remove_nullable)
            .collect::<Vec<_>>();
        let request = RawAggregateCall {
            distinct: false,
            ..context.request.clone()
        };
        let combinator = DistinctCombinator {
            args_type: args_type.clone(),
        };
        let function = self
            .build
            .build(request, &args_type, metadata, combinator)?;
        Ok(Some(function))
    }
}
