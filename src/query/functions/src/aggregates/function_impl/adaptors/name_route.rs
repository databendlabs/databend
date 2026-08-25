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
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;

use super::AggregateArgumentPattern;
use super::AggregateArgumentsPattern;
use super::AggregateFunctionBuilder;
use super::AggregateFunctionDescriptor;
use super::AggregateFunctionRef;
use super::AggregateFunctionRegistry;
use super::AggregateFunctionRequest;
use super::CombinatorImpl;
use super::DirectBuildContext;
use super::DirectBuildFn;
use super::DistinctCombinator;
use super::DistinctPolicy;
use super::FunctionFeatures;
use super::IfCombinator;
use super::LegacySignatureResolver;
use super::MultiArgBuildContext;
use super::MultiArgBuildFn;
use super::NullPolicy;
use super::PlainCombinator;
use super::StateCombinator;
use super::StateCombinatorPlan;
use super::UnaryBuildContext;
use super::UnaryBuildFn;
use super::merge_combinator;
use super::state_combinator;
use super::try_create_null_argument_result_function;

/// An ordered sequence of direct aggregate name routes.
///
/// Registration metadata is fixed when the route is created. Each route node
/// provides the metadata transformation for its own descriptor.
pub(crate) struct DirectNameRoute {
    names: &'static [&'static str],
    arguments: AggregateArgumentsPattern,
    features: FunctionFeatures,
    distinct_target: Option<String>,
    null_policy: NullPolicy,
    validate: Option<DirectRouteValidateFn>,
    routes: Vec<Box<dyn DirectRouteNode>>,
}

type DirectRouteValidateFn = for<'a> fn(&AggregateFunctionRequest<'a>) -> Result<()>;

pub(crate) struct DirectRouteContext<'request, 'route> {
    request: AggregateFunctionRequest<'request>,
    names: &'route [&'route str],
    arguments: &'route AggregateArgumentsPattern,
    features: &'route FunctionFeatures,
    null_policy: NullPolicy,
}

pub(crate) trait DirectRouteNode: Send + Sync {
    fn suffix(&self) -> Option<&'static str> {
        None
    }

    fn arguments(&self, base: &AggregateArgumentsPattern) -> AggregateArgumentsPattern {
        base.clone()
    }

    fn features(&self, base: &FunctionFeatures) -> FunctionFeatures {
        base.clone()
    }

    fn distinct_target(&self, _base_name: &str) -> Option<String> {
        None
    }

    fn distinct_is_idempotent(&self) -> bool {
        false
    }

    fn try_build(
        &self,
        context: &DirectRouteContext<'_, '_>,
    ) -> Result<Option<AggregateFunctionRef>>;
}

impl DirectNameRoute {
    pub(crate) fn new(
        names: &'static [&'static str],
        arguments: AggregateArgumentsPattern,
        features: FunctionFeatures,
        null_policy: NullPolicy,
    ) -> Self {
        assert!(!names.is_empty(), "a direct name route requires a name");
        Self {
            names,
            arguments,
            features,
            distinct_target: None,
            null_policy,
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

    pub(crate) fn then(mut self, next: impl DirectRouteNode + 'static) -> Self {
        self.routes.push(Box::new(next));
        self
    }

    pub(crate) fn into_descriptors(self) -> Vec<AggregateFunctionDescriptor> {
        let route = Arc::new(self);
        let supports_filter = route.routes.iter().any(|node| node.suffix() == Some("if"));
        let routed_distinct = route.routes.iter().find_map(|node| {
            node.distinct_target(route.names[0]).map(|target| {
                let aliases = route.names[1..]
                    .iter()
                    .filter_map(|alias| {
                        node.distinct_target(alias)
                            .map(|target| ((*alias).to_string(), target))
                    })
                    .collect::<Vec<_>>();
                (target, aliases)
            })
        });
        let distinct_is_idempotent = route
            .routes
            .iter()
            .any(|node| node.distinct_is_idempotent());
        assert!(
            !distinct_is_idempotent || route.distinct_target.is_none() && routed_distinct.is_none(),
            "conflicting DISTINCT policies for aggregate function {}",
            route.names[0]
        );
        if let (Some(explicit), Some((routed, _))) = (&route.distinct_target, &routed_distinct) {
            assert_eq!(
                explicit, routed,
                "conflicting DISTINCT targets for aggregate function {}",
                route.names[0]
            );
        }
        let distinct_policy = match (&route.distinct_target, routed_distinct) {
            _ if distinct_is_idempotent => Some(DistinctPolicy::Idempotent),
            (Some(target), _) => Some(DistinctPolicy::redirect(target.clone())),
            (None, Some((target, aliases))) => {
                Some(DistinctPolicy::redirect_with_aliases(target, aliases))
            }
            (None, None) => None,
        };
        route
            .routes
            .iter()
            .map(|node| {
                let suffix = node.suffix();
                let name = suffixed_name(route.names[0], suffix);
                let aliases = route.names[1..]
                    .iter()
                    .map(|alias| suffixed_name(alias, suffix))
                    .collect::<Vec<_>>();
                let builder: Arc<dyn AggregateFunctionBuilder> = route.clone();
                let mut features = node.features(&route.features);
                features.supports_filter = suffix.is_none() && supports_filter;
                if suffix.is_none()
                    && let Some(policy) = &distinct_policy
                {
                    features.distinct_policy = policy.clone();
                }
                let mut descriptor = AggregateFunctionDescriptor::from_builder(name, builder)
                    .with_metadata(node.arguments(&route.arguments), features);
                if !aliases.is_empty() {
                    descriptor = descriptor.with_aliases(aliases);
                }
                descriptor
            })
            .collect()
    }

    pub(crate) fn register(self, registry: &mut AggregateFunctionRegistry) {
        for descriptor in self.into_descriptors() {
            registry.register(descriptor);
        }
    }

    pub(crate) fn build(
        &self,
        request: AggregateFunctionRequest<'_>,
    ) -> Result<AggregateFunctionRef> {
        if let Some(validate) = self.validate {
            validate(&request)?;
        }
        let context = DirectRouteContext {
            request,
            names: self.names,
            arguments: &self.arguments,
            features: &self.features,
            null_policy: self.null_policy,
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

impl AggregateFunctionBuilder for DirectNameRoute {
    fn arguments(&self) -> &AggregateArgumentsPattern {
        &self.arguments
    }

    fn features(&self) -> &FunctionFeatures {
        &self.features
    }

    fn build(&self, request: AggregateFunctionRequest<'_>) -> Result<AggregateFunctionRef> {
        DirectNameRoute::build(self, request)
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

impl<C> RouteBuild<C>
where C: CombinatorImpl
{
    fn null_argument_mode(&self) -> NullArgumentMode {
        match self {
            Self::MultiArg(_) => NullArgumentMode::Any,
            Self::Unary(_) | Self::Direct(_) => NullArgumentMode::Only,
        }
    }

    fn build<'a>(
        &self,
        request: AggregateFunctionRequest<'a>,
        signature_args_type: &'a [DataType],
        features: FunctionFeatures,
        combinator: C,
    ) -> Result<AggregateFunctionRef> {
        match self {
            Self::Unary(build) => build(UnaryBuildContext::new(
                request,
                signature_args_type,
                features,
                combinator,
            )?),
            Self::MultiArg(build) => build(MultiArgBuildContext::new(
                request,
                signature_args_type,
                features,
                combinator,
            )),
            Self::Direct(build) => build(DirectBuildContext::new(
                request,
                signature_args_type,
                features,
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

fn request_with_args_type<'a, 'b>(
    request: &'b AggregateFunctionRequest<'a>,
    args_type: &'b [DataType],
    strip_distinct: bool,
) -> AggregateFunctionRequest<'b> {
    AggregateFunctionRequest {
        name: request.name,
        params: request.params,
        args_type,
        distinct: !strip_distinct && request.distinct,
        order_by: request.order_by,
    }
}

fn null_argument_result(
    request: &AggregateFunctionRequest<'_>,
    mode: NullArgumentMode,
    returns_default_when_only_null: bool,
) -> Result<Option<AggregateFunctionRef>> {
    let has_null_argument = match mode {
        NullArgumentMode::Only => matches!(request.args_type, [DataType::Null]),
        NullArgumentMode::Any => request.args_type.iter().any(DataType::is_null),
    };
    has_null_argument
        .then(|| {
            try_create_null_argument_result_function(
                request.clone(),
                returns_default_when_only_null,
            )
        })
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

impl DirectRouteNode for MergeRoute {
    fn suffix(&self) -> Option<&'static str> {
        Some(if self.returns_state {
            "merge_state"
        } else {
            "merge"
        })
    }

    fn arguments(&self, _base: &AggregateArgumentsPattern) -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any()])
    }

    fn features(&self, base: &FunctionFeatures) -> FunctionFeatures {
        let mut features = base.clone();
        features.distinct_policy = DistinctPolicy::Unsupported;
        features
    }

    fn try_build(
        &self,
        context: &DirectRouteContext<'_, '_>,
    ) -> Result<Option<AggregateFunctionRef>> {
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
        let returns_default_when_only_null =
            context.null_policy == NullPolicy::ReturnsDefaultWhenOnlyNull;
        let null_argument_mode = self.build.null_argument_mode();
        let features = self.features(context.features);
        let nested_build = |params: &[Scalar], args_type: &[DataType]| {
            let nested_request = AggregateFunctionRequest {
                name: nested_name,
                params,
                args_type,
                distinct: false,
                order_by,
            };
            if context.null_policy != NullPolicy::Keep
                && let Some(function) = null_argument_result(
                    &nested_request,
                    null_argument_mode,
                    returns_default_when_only_null,
                )?
            {
                return Ok(function);
            }
            self.build
                .build(nested_request, args_type, features.clone(), PlainCombinator)
        };
        merge_combinator::create(
            request,
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

impl DirectRouteNode for PlainRoute {
    fn try_build(
        &self,
        context: &DirectRouteContext<'_, '_>,
    ) -> Result<Option<AggregateFunctionRef>> {
        if context.matching_name_index(None).is_none() {
            return Ok(None);
        }
        if let Some(validate) = self.validate {
            validate(&context.request)?;
        }
        let features = context.features.clone();
        if context.null_policy != NullPolicy::Keep
            && let Some(function) = null_argument_result(
                &context.request,
                self.build.null_argument_mode(),
                context.null_policy == NullPolicy::ReturnsDefaultWhenOnlyNull,
            )?
        {
            return Ok(Some(function));
        }
        let request = context.request.clone();
        let args_type = request.args_type;
        let function = self
            .build
            .build(request, args_type, features, PlainCombinator)?;
        Ok(Some(function))
    }
}

pub(crate) struct IfRoute {
    features: Option<FunctionFeatures>,
    build: RouteBuild<IfCombinator>,
}

impl IfRoute {
    pub(crate) fn new(build: DirectBuildFn<IfCombinator>) -> Self {
        Self {
            features: None,
            build: RouteBuild::Direct(build),
        }
    }

    pub(crate) fn unary(build: UnaryBuildFn<IfCombinator>) -> Self {
        Self {
            features: None,
            build: RouteBuild::Unary(build),
        }
    }

    pub(crate) fn multi_arg(build: MultiArgBuildFn<IfCombinator>) -> Self {
        Self {
            features: None,
            build: RouteBuild::MultiArg(build),
        }
    }

    pub(crate) fn with_features(mut self, features: FunctionFeatures) -> Self {
        self.features = Some(features);
        self
    }
}

impl DirectRouteNode for IfRoute {
    fn suffix(&self) -> Option<&'static str> {
        Some("if")
    }

    fn arguments(&self, base: &AggregateArgumentsPattern) -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::if_condition(base.clone())
    }

    fn features(&self, base: &FunctionFeatures) -> FunctionFeatures {
        self.features.clone().unwrap_or_else(|| base.clone())
    }

    fn try_build(
        &self,
        context: &DirectRouteContext<'_, '_>,
    ) -> Result<Option<AggregateFunctionRef>> {
        if context.matching_name_index(Some("if")).is_none() {
            return Ok(None);
        }
        let keep_null = context.null_policy == NullPolicy::Keep;
        if !keep_null {
            let returns_default = context.null_policy == NullPolicy::ReturnsDefaultWhenOnlyNull;
            if let Some(function) =
                null_argument_result(&context.request, NullArgumentMode::Any, returns_default)?
            {
                return Ok(Some(function));
            }
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
        let args_type = if keep_null {
            nested_arg_types.to_vec()
        } else {
            nested_arg_types
                .iter()
                .map(DataType::remove_nullable)
                .collect()
        };
        let features = self
            .features
            .clone()
            .unwrap_or_else(|| context.features.clone());
        let signature_args_type = context.request.args_type;
        let request = request_with_args_type(&context.request, &args_type, false);
        let function = self
            .build
            .build(request, signature_args_type, features, IfCombinator {
                nested_args_type: args_type.clone(),
                condition_index,
                always_false: condition_type.is_null(),
                strip_nullable_input: !keep_null,
            })?;
        Ok(Some(function))
    }
}

pub(crate) struct StateRoute {
    arguments: Option<AggregateArgumentsPattern>,
    features: Option<FunctionFeatures>,
    build: RouteBuild<StateCombinator>,
}

impl StateRoute {
    pub(crate) fn new(build: DirectBuildFn<StateCombinator>) -> Self {
        Self {
            arguments: None,
            features: None,
            build: RouteBuild::Direct(build),
        }
    }

    pub(crate) fn unary(build: UnaryBuildFn<StateCombinator>) -> Self {
        Self {
            arguments: None,
            features: None,
            build: RouteBuild::Unary(build),
        }
    }

    pub(crate) fn multi_arg(build: MultiArgBuildFn<StateCombinator>) -> Self {
        Self {
            arguments: None,
            features: None,
            build: RouteBuild::MultiArg(build),
        }
    }

    pub(crate) fn with_arguments(mut self, arguments: AggregateArgumentsPattern) -> Self {
        self.arguments = Some(arguments);
        self
    }

    pub(crate) fn with_features(mut self, features: FunctionFeatures) -> Self {
        self.features = Some(features);
        self
    }
}

impl DirectRouteNode for StateRoute {
    fn suffix(&self) -> Option<&'static str> {
        Some("state")
    }

    fn arguments(&self, base: &AggregateArgumentsPattern) -> AggregateArgumentsPattern {
        self.arguments.clone().unwrap_or_else(|| base.clone())
    }

    fn features(&self, base: &FunctionFeatures) -> FunctionFeatures {
        self.features.clone().unwrap_or_else(|| base.clone())
    }

    fn try_build(
        &self,
        context: &DirectRouteContext<'_, '_>,
    ) -> Result<Option<AggregateFunctionRef>> {
        if context.matching_name_index(Some("state")).is_none() {
            return Ok(None);
        }
        let state_plan = if context.null_policy == NullPolicy::Keep {
            StateCombinatorPlan::default()
        } else {
            let returns_default = context.null_policy == NullPolicy::ReturnsDefaultWhenOnlyNull;
            if context.request.args_type.iter().any(DataType::is_null) {
                return Ok(Some(state_combinator::create_state_null_result_function(
                    context.request.clone(),
                    returns_default,
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
        let features = self
            .features
            .clone()
            .unwrap_or_else(|| context.features.clone());
        let args_type = state_plan.strip_nullable_input.then(|| {
            context
                .request
                .args_type
                .iter()
                .map(DataType::remove_nullable)
                .collect::<Vec<_>>()
        });
        let signature_args_type = context.request.args_type;
        let request = match &args_type {
            Some(args_type) => request_with_args_type(&context.request, args_type, false),
            None => context.request.clone(),
        };
        let function =
            self.build
                .build(request, signature_args_type, features, StateCombinator {
                    plan: state_plan,
                })?;
        Ok(Some(function))
    }
}

pub(crate) struct DistinctAliasRoute {
    build: RouteBuild<PlainCombinator>,
}

impl DistinctAliasRoute {
    pub(crate) fn new(build: DirectBuildFn<PlainCombinator>) -> Self {
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

impl DirectRouteNode for DistinctAliasRoute {
    fn suffix(&self) -> Option<&'static str> {
        Some("distinct")
    }

    fn distinct_is_idempotent(&self) -> bool {
        true
    }

    fn try_build(
        &self,
        context: &DirectRouteContext<'_, '_>,
    ) -> Result<Option<AggregateFunctionRef>> {
        if context.matching_name_index(Some("distinct")).is_none() {
            return Ok(None);
        }
        if context.null_policy != NullPolicy::Keep
            && let Some(function) = null_argument_result(
                &context.request,
                self.build.null_argument_mode(),
                context.null_policy == NullPolicy::ReturnsDefaultWhenOnlyNull,
            )?
        {
            return Ok(Some(function));
        }
        let features = context.features.clone();
        let request = context.request.clone();
        let args_type = request.args_type;
        let function = self
            .build
            .build(request, args_type, features, PlainCombinator)?;
        Ok(Some(function))
    }
}

pub(crate) struct DistinctRoute {
    build: RouteBuild<DistinctCombinator>,
}

impl DistinctRoute {
    pub(crate) fn new(build: DirectBuildFn<DistinctCombinator>) -> Self {
        Self {
            build: RouteBuild::Direct(build),
        }
    }

    pub(crate) fn unary(build: UnaryBuildFn<DistinctCombinator>) -> Self {
        Self {
            build: RouteBuild::Unary(build),
        }
    }

    pub(crate) fn multi_arg(build: MultiArgBuildFn<DistinctCombinator>) -> Self {
        Self {
            build: RouteBuild::MultiArg(build),
        }
    }
}

impl DirectRouteNode for DistinctRoute {
    fn suffix(&self) -> Option<&'static str> {
        Some("distinct")
    }

    fn distinct_target(&self, base_name: &str) -> Option<String> {
        Some(suffixed_name(base_name, self.suffix()))
    }

    fn try_build(
        &self,
        context: &DirectRouteContext<'_, '_>,
    ) -> Result<Option<AggregateFunctionRef>> {
        if context.matching_name_index(Some("distinct")).is_none() {
            return Ok(None);
        }
        let returns_default = context.null_policy == NullPolicy::ReturnsDefaultWhenOnlyNull;
        if matches!(self.build, RouteBuild::MultiArg(_))
            && let Some(function) =
                null_argument_result(&context.request, NullArgumentMode::Any, returns_default)?
        {
            return Ok(Some(function));
        }
        let features = context.features.clone();
        let args_type = context
            .request
            .args_type
            .iter()
            .map(DataType::remove_nullable)
            .collect::<Vec<_>>();
        let signature_args_type = context.request.args_type;
        let request = request_with_args_type(&context.request, &args_type, true);
        let combinator = DistinctCombinator {
            args_type: args_type.clone(),
            skip_nulls: context.null_policy != NullPolicy::Keep,
        };
        let function = self
            .build
            .build(request, signature_args_type, features, combinator)?;
        Ok(Some(function))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_exception::ErrorCode;

    use super::*;

    struct FixedResultBuilder {
        arguments: AggregateArgumentsPattern,
        features: FunctionFeatures,
    }

    impl AggregateFunctionBuilder for FixedResultBuilder {
        fn arguments(&self) -> &AggregateArgumentsPattern {
            &self.arguments
        }

        fn features(&self) -> &FunctionFeatures {
            &self.features
        }

        fn build(&self, request: AggregateFunctionRequest<'_>) -> Result<AggregateFunctionRef> {
            try_create_null_argument_result_function(request, false)
        }
    }

    struct Miss {
        count: Arc<AtomicUsize>,
        arguments: AggregateArgumentsPattern,
        features: FunctionFeatures,
    }

    impl DirectRouteNode for Miss {
        fn arguments(&self, _base: &AggregateArgumentsPattern) -> AggregateArgumentsPattern {
            self.arguments.clone()
        }

        fn features(&self, _base: &FunctionFeatures) -> FunctionFeatures {
            self.features.clone()
        }

        fn try_build(
            &self,
            _context: &DirectRouteContext<'_, '_>,
        ) -> Result<Option<AggregateFunctionRef>> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
    }

    struct Stop {
        count: Arc<AtomicUsize>,
        arguments: AggregateArgumentsPattern,
        features: FunctionFeatures,
    }

    impl DirectRouteNode for Stop {
        fn arguments(&self, _base: &AggregateArgumentsPattern) -> AggregateArgumentsPattern {
            self.arguments.clone()
        }

        fn features(&self, _base: &FunctionFeatures) -> FunctionFeatures {
            self.features.clone()
        }

        fn try_build(
            &self,
            _context: &DirectRouteContext<'_, '_>,
        ) -> Result<Option<AggregateFunctionRef>> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Err(ErrorCode::Internal("stop"))
        }
    }

    struct MustNotRun {
        arguments: AggregateArgumentsPattern,
        features: FunctionFeatures,
    }

    impl DirectRouteNode for MustNotRun {
        fn arguments(&self, _base: &AggregateArgumentsPattern) -> AggregateArgumentsPattern {
            self.arguments.clone()
        }

        fn features(&self, _base: &FunctionFeatures) -> FunctionFeatures {
            self.features.clone()
        }

        fn try_build(
            &self,
            _context: &DirectRouteContext<'_, '_>,
        ) -> Result<Option<AggregateFunctionRef>> {
            panic!("route evaluation must stop after the first result")
        }
    }

    #[test]
    fn test_direct_name_route_is_linear_and_short_circuits() {
        let misses = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let arguments = AggregateArgumentsPattern::fixed(vec![]);
        let features = FunctionFeatures::default();
        let rule = DirectNameRoute::new(
            &["test"],
            arguments.clone(),
            features.clone(),
            NullPolicy::Skip,
        )
        .then(Miss {
            count: misses.clone(),
            arguments: arguments.clone(),
            features: features.clone(),
        })
        .then(Stop {
            count: stops.clone(),
            arguments: arguments.clone(),
            features: features.clone(),
        })
        .then(MustNotRun {
            arguments,
            features,
        });
        let request = AggregateFunctionRequest {
            name: "test",
            params: &[],
            args_type: &[],
            distinct: false,
            order_by: &[],
        };

        let error = match rule.build(request) {
            Ok(_) => panic!("route must stop with the marker error"),
            Err(error) => error,
        };

        assert_eq!(misses.load(Ordering::Relaxed), 1);
        assert_eq!(stops.load(Ordering::Relaxed), 1);
        assert!(error.message().contains("stop"));
    }

    struct DescriptorNode {
        suffix: Option<&'static str>,
        arguments: AggregateArgumentsPattern,
        features: FunctionFeatures,
    }

    impl DirectRouteNode for DescriptorNode {
        fn suffix(&self) -> Option<&'static str> {
            self.suffix
        }

        fn arguments(&self, _base: &AggregateArgumentsPattern) -> AggregateArgumentsPattern {
            self.arguments.clone()
        }

        fn features(&self, _base: &FunctionFeatures) -> FunctionFeatures {
            self.features.clone()
        }

        fn try_build(
            &self,
            _context: &DirectRouteContext<'_, '_>,
        ) -> Result<Option<AggregateFunctionRef>> {
            Ok(None)
        }
    }

    #[test]
    fn test_direct_name_route_produces_descriptors() {
        let base_arguments = AggregateArgumentsPattern::fixed(vec![]);
        let if_arguments = AggregateArgumentsPattern::if_condition(base_arguments.clone());
        let base_features = FunctionFeatures {
            is_decomposable: true,
            ..Default::default()
        };

        let descriptors = DirectNameRoute::new(
            &["test", "test_alias"],
            base_arguments.clone(),
            base_features.clone(),
            NullPolicy::Skip,
        )
        .then(DescriptorNode {
            suffix: None,
            arguments: base_arguments.clone(),
            features: base_features.clone(),
        })
        .then(DescriptorNode {
            suffix: Some("if"),
            arguments: if_arguments.clone(),
            features: base_features,
        })
        .into_descriptors();

        assert_eq!(descriptors.len(), 2);
        assert_eq!(descriptors[0].name, "test");
        assert_eq!(descriptors[0].aliases, ["test_alias"]);
        assert_eq!(descriptors[0].arguments(), &base_arguments);
        assert!(descriptors[0].features().is_decomposable);
        assert!(descriptors[0].features().supports_filter);
        assert_eq!(descriptors[1].name, "test_if");
        assert_eq!(descriptors[1].aliases, ["test_alias_if"]);
        assert_eq!(descriptors[1].arguments(), &if_arguments);
        assert!(descriptors[1].features().is_decomposable);
        assert!(!descriptors[1].features().supports_filter);
    }

    #[test]
    fn test_direct_name_route_registers_descriptor_names_and_aliases() {
        let mut registry = AggregateFunctionRegistry::empty();
        let arguments = AggregateArgumentsPattern::fixed(vec![]);
        let features = FunctionFeatures::default();
        DirectNameRoute::new(
            &["test", "test_alias"],
            arguments.clone(),
            features.clone(),
            NullPolicy::Skip,
        )
        .then(DescriptorNode {
            suffix: None,
            arguments: arguments.clone(),
            features: features.clone(),
        })
        .then(DescriptorNode {
            suffix: Some("if"),
            arguments: AggregateArgumentsPattern::if_condition(arguments.clone()),
            features: features.clone(),
        })
        .then(DescriptorNode {
            suffix: Some("state"),
            arguments,
            features,
        })
        .register(&mut registry);

        assert!(registry.contains("test"));
        assert!(registry.contains("test_alias"));
        assert!(registry.contains("test_if"));
        assert!(registry.contains("test_alias_if"));
        assert!(registry.contains("test_state"));
        assert!(registry.contains("test_alias_state"));
        assert!(!registry.contains("test_distinct"));
        for name in ["test", "test_alias"] {
            assert!(registry.descriptors(name)[0].features().supports_filter);
        }
        for name in ["test_if", "test_alias_if", "test_state", "test_alias_state"] {
            assert!(!registry.descriptors(name)[0].features().supports_filter);
        }
    }

    #[test]
    fn test_registry_redirects_distinct_without_name_route_or_suffix() {
        let mut registry = AggregateFunctionRegistry::empty();
        let arguments = AggregateArgumentsPattern::fixed(vec![]);
        let builder = Arc::new(FixedResultBuilder {
            arguments: arguments.clone(),
            features: FunctionFeatures::default(),
        });
        registry.register(AggregateFunctionDescriptor::from_builder(
            "deduplicated_test",
            builder.clone(),
        ));

        let source_features = FunctionFeatures {
            distinct_policy: DistinctPolicy::redirect("deduplicated_test"),
            ..Default::default()
        };
        registry.register(
            AggregateFunctionDescriptor::from_builder("test", builder)
                .with_metadata(arguments, source_features),
        );

        let function = registry
            .resolve(AggregateFunctionRequest {
                name: "test",
                params: &[],
                args_type: &[],
                distinct: true,
                order_by: &[],
            })
            .unwrap();

        assert_eq!(function.signature().name, "deduplicated_test");
        assert!(!function.signature().distinct);
    }

    #[test]
    fn test_direct_name_route_without_matcher_returns_unknown() {
        let rule = DirectNameRoute::new(
            &["test"],
            AggregateArgumentsPattern::fixed(vec![]),
            FunctionFeatures::default(),
            NullPolicy::Skip,
        );
        let request = AggregateFunctionRequest {
            name: "test_distinct",
            params: &[],
            args_type: &[],
            distinct: false,
            order_by: &[],
        };

        let error = match rule.build(request) {
            Ok(_) => panic!("an unmatched route must return an error"),
            Err(error) => error,
        };

        assert!(error.message().contains("Unsupported AggregateFunction"));
        assert!(error.message().contains("test_distinct"));
    }
}
