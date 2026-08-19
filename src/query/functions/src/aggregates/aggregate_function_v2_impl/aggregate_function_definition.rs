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

use super::adaptors_v2 as v2;

#[derive(Clone)]
pub(super) struct AggregateFunctionDefinition {
    pub(super) name: &'static str,
    aliases: &'static [&'static str],
    pub(super) arguments: v2::AggregateArgumentsPattern,
    pub(super) features: v2::FunctionFeatures,
    builder: v2::AggregateFunctionBuildFn,
}

impl AggregateFunctionDefinition {
    pub(super) fn new(
        name: &'static str,
        arguments: v2::AggregateArgumentsPattern,
        features: v2::FunctionFeatures,
        builder: v2::AggregateFunctionBuildFn,
    ) -> Self {
        Self {
            name,
            aliases: &[],
            arguments,
            features,
            builder,
        }
    }

    pub(super) fn with_aliases(mut self, aliases: &'static [&'static str]) -> Self {
        self.aliases = aliases;
        self
    }

    pub(super) fn register(&self, registry: &mut v2::AggregateFunctionRegistry) {
        let builder = Arc::new(self.clone());
        register_descriptor(
            registry,
            self.name.to_string(),
            aliases_to_strings(self.aliases),
            self.arguments.clone(),
            self.features.clone(),
            builder,
        )
    }

    pub(super) fn register_with_combinators(
        &self,
        registry: &mut v2::AggregateFunctionRegistry,
        register_distinct: bool,
    ) {
        self.register_with_merge_combinators(registry);
        let aliases = aliases_to_strings(self.aliases);
        let builder = Arc::new(self.clone());
        register_descriptor(
            registry,
            suffixed_name(self.name, "if"),
            suffixed_aliases(&aliases, "if"),
            v2::AggregateArgumentsPattern::if_condition(self.arguments.clone()),
            self.features.clone(),
            builder.clone(),
        );
        register_descriptor(
            registry,
            suffixed_name(self.name, "state"),
            suffixed_aliases(&aliases, "state"),
            self.arguments.clone(),
            self.features.clone(),
            builder.clone(),
        );
        if register_distinct {
            register_descriptor(
                registry,
                suffixed_name(self.name, "distinct"),
                suffixed_aliases(&aliases, "distinct"),
                self.arguments.clone(),
                self.features.clone(),
                builder,
            );
        }
    }

    pub(super) fn register_with_merge_combinators(
        &self,
        registry: &mut v2::AggregateFunctionRegistry,
    ) {
        let aliases = aliases_to_strings(self.aliases);
        let builder = Arc::new(self.clone());
        register_descriptor(
            registry,
            self.name.to_string(),
            aliases.clone(),
            self.arguments.clone(),
            self.features.clone(),
            builder.clone(),
        );
        let arguments =
            v2::AggregateArgumentsPattern::fixed(vec![v2::AggregateArgumentPattern::any()]);
        let mut features = self.features.clone();
        features.distinct_policy = v2::DistinctPolicy::Unsupported;
        register_descriptor(
            registry,
            suffixed_name(self.name, "merge"),
            suffixed_aliases(&aliases, "merge"),
            arguments.clone(),
            features.clone(),
            builder.clone(),
        );
        register_descriptor(
            registry,
            suffixed_name(self.name, "merge_state"),
            suffixed_aliases(&aliases, "merge_state"),
            arguments,
            features,
            builder,
        );
    }

    pub(super) fn build_with_unary_input(
        &self,
        request: v2::AggregateFunctionRequest<'_>,
        returns_default_when_only_null: bool,
        build: v2::UnaryAggregateFunctionBuildInputFns,
    ) -> Result<v2::AggregateFunctionRef> {
        let names = self.names();
        v2::build_default_name_route_with_unary_input(
            request,
            &names,
            self.features.clone(),
            returns_default_when_only_null,
            build,
        )
    }

    pub(super) fn build_with_multi_arg_input(
        &self,
        request: v2::AggregateFunctionRequest<'_>,
        returns_default_when_only_null: bool,
        build: v2::MultiArgAggregateFunctionBuildInputFns,
    ) -> Result<v2::AggregateFunctionRef> {
        let names = self.names();
        v2::build_default_name_route_with_multi_arg_build_input(
            request,
            &names,
            self.features.clone(),
            returns_default_when_only_null,
            build,
        )
    }

    pub(super) fn build_with_direct_input(
        &self,
        request: v2::AggregateFunctionRequest<'_>,
        returns_default_when_only_null: bool,
        build: v2::DirectAggregateFunctionBuildInputFns,
    ) -> Result<v2::AggregateFunctionRef> {
        let names = self.names();
        v2::build_default_name_route_with_direct_input(
            request,
            &names,
            self.features.clone(),
            returns_default_when_only_null,
            build,
        )
    }

    fn names(&self) -> Vec<&str> {
        std::iter::once(self.name)
            .chain(self.aliases.iter().copied())
            .collect()
    }
}

impl v2::AggregateFunctionBuilder for AggregateFunctionDefinition {
    fn arguments(&self) -> &v2::AggregateArgumentsPattern {
        &self.arguments
    }

    fn features(&self) -> &v2::FunctionFeatures {
        &self.features
    }

    fn build(&self, request: v2::AggregateFunctionRequest<'_>) -> Result<v2::AggregateFunctionRef> {
        if request
            .name
            .eq_ignore_ascii_case(&suffixed_name(self.name, "merge"))
        {
            return v2::merge_combinator::try_create(
                request,
                self.name,
                self.aliases,
                self.builder,
                false,
            );
        }
        if request
            .name
            .eq_ignore_ascii_case(&suffixed_name(self.name, "merge_state"))
        {
            return v2::merge_combinator::try_create(
                request,
                self.name,
                self.aliases,
                self.builder,
                true,
            );
        }
        (self.builder)(request)
    }
}

fn register_descriptor(
    registry: &mut v2::AggregateFunctionRegistry,
    name: String,
    aliases: Vec<String>,
    arguments: v2::AggregateArgumentsPattern,
    features: v2::FunctionFeatures,
    builder: Arc<dyn v2::AggregateFunctionBuilder>,
) {
    let mut descriptor = v2::AggregateFunctionDescriptor::from_builder(name, builder)
        .with_metadata(arguments, features);
    if !aliases.is_empty() {
        descriptor = descriptor.with_aliases(aliases);
    }
    registry.register(descriptor);
}

fn suffixed_name(name: &str, suffix: &str) -> String {
    format!("{name}_{suffix}")
}

fn aliases_to_strings(aliases: &'static [&'static str]) -> Vec<String> {
    aliases.iter().map(|alias| (*alias).to_string()).collect()
}

fn suffixed_aliases(aliases: &[String], suffix: &str) -> Vec<String> {
    aliases
        .iter()
        .map(|alias| suffixed_name(alias.as_str(), suffix))
        .collect()
}
