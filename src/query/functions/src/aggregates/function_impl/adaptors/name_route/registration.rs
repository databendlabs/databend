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

use super::super::AggregateCallBuilder;
use super::super::AggregateCallRef;
use super::super::AggregateDescriptor;
use super::super::AggregateFeatures;
use super::super::AggregateRegistry;
use super::super::ArgumentsPattern;
use super::super::DistinctPolicy;
use super::super::RawAggregateCall;
use super::NameRoute;
use super::suffixed_name;

/// The adapter between internal name routing and the external registration API.
struct RegisteredAggregate {
    route: Arc<NameRoute>,
    arguments: ArgumentsPattern,
    features: AggregateFeatures,
}

impl AggregateCallBuilder for RegisteredAggregate {
    fn arguments(&self) -> &ArgumentsPattern {
        &self.arguments
    }

    fn features(&self) -> &AggregateFeatures {
        &self.features
    }

    fn build(&self, request: RawAggregateCall<'_>) -> Result<AggregateCallRef> {
        self.route.build(request)
    }
}

impl NameRoute {
    pub(crate) fn into_descriptors(self) -> Vec<AggregateDescriptor> {
        let route = Arc::new(self);
        let supports_state = route
            .routes
            .iter()
            .any(|node| node.suffix() == Some("state"));
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
            _ if distinct_is_idempotent => DistinctPolicy::Idempotent,
            (Some(target), _) => DistinctPolicy::redirect(target.clone()),
            (None, Some((target, aliases))) => {
                DistinctPolicy::redirect_with_aliases(target, aliases)
            }
            (None, None) => DistinctPolicy::Unsupported,
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
                let mut features = node.metadata(&route.metadata).into_features();
                features.supports_filter = suffix.is_none() && supports_filter;
                features.supports_state = suffix.is_none() && supports_state;
                if suffix.is_none() {
                    features.distinct_policy = distinct_policy.clone();
                }
                let builder = Arc::new(RegisteredAggregate {
                    arguments: node.arguments(&route.arguments),
                    features,
                    route: route.clone(),
                });
                AggregateDescriptor::from_builder(name, builder).with_aliases(aliases)
            })
            .collect()
    }

    pub(crate) fn register(self, registry: &mut AggregateRegistry) {
        for descriptor in self.into_descriptors() {
            registry.register(descriptor);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_exception::ErrorCode;

    use super::super::super::DirectBuildContext;
    use super::super::super::DirectBuildFn;
    use super::super::*;
    use super::*;

    struct FixedResultBuilder {
        arguments: ArgumentsPattern,
        features: AggregateFeatures,
    }

    impl AggregateCallBuilder for FixedResultBuilder {
        fn arguments(&self) -> &ArgumentsPattern {
            &self.arguments
        }

        fn features(&self) -> &AggregateFeatures {
            &self.features
        }

        fn build(&self, request: RawAggregateCall<'_>) -> Result<AggregateCallRef> {
            try_create_null_argument_result_function(request, AggregateMetadata::default())
        }
    }

    struct Miss {
        count: Arc<AtomicUsize>,
        arguments: ArgumentsPattern,
        metadata: AggregateMetadata,
    }

    impl RouteNode for Miss {
        fn arguments(&self, _base: &ArgumentsPattern) -> ArgumentsPattern {
            self.arguments.clone()
        }

        fn metadata(&self, _base: &AggregateMetadata) -> AggregateMetadata {
            self.metadata
        }

        fn try_build(
            &self,
            _context: &DirectRouteContext<'_, '_>,
        ) -> Result<Option<AggregateCallRef>> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
    }

    struct Stop {
        count: Arc<AtomicUsize>,
        arguments: ArgumentsPattern,
        metadata: AggregateMetadata,
    }

    impl RouteNode for Stop {
        fn arguments(&self, _base: &ArgumentsPattern) -> ArgumentsPattern {
            self.arguments.clone()
        }

        fn metadata(&self, _base: &AggregateMetadata) -> AggregateMetadata {
            self.metadata
        }

        fn try_build(
            &self,
            _context: &DirectRouteContext<'_, '_>,
        ) -> Result<Option<AggregateCallRef>> {
            self.count.fetch_add(1, Ordering::Relaxed);
            Err(ErrorCode::Internal("stop"))
        }
    }

    struct MustNotRun {
        arguments: ArgumentsPattern,
        metadata: AggregateMetadata,
    }

    impl RouteNode for MustNotRun {
        fn arguments(&self, _base: &ArgumentsPattern) -> ArgumentsPattern {
            self.arguments.clone()
        }

        fn metadata(&self, _base: &AggregateMetadata) -> AggregateMetadata {
            self.metadata
        }

        fn try_build(
            &self,
            _context: &DirectRouteContext<'_, '_>,
        ) -> Result<Option<AggregateCallRef>> {
            panic!("route evaluation must stop after the first result")
        }
    }

    #[test]
    fn test_direct_name_route_is_linear_and_short_circuits() {
        let misses = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let arguments = ArgumentsPattern::fixed(vec![]);
        let metadata = AggregateMetadata::default();
        let rule = NameRoute::new(&["test"], arguments.clone(), metadata, NullInput::Filter)
            .then(Miss {
                count: misses.clone(),
                arguments: arguments.clone(),
                metadata,
            })
            .then(Stop {
                count: stops.clone(),
                arguments: arguments.clone(),
                metadata,
            })
            .then(MustNotRun {
                arguments,
                metadata,
            });
        let request = RawAggregateCall {
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
        arguments: ArgumentsPattern,
        metadata: AggregateMetadata,
    }

    impl RouteNode for DescriptorNode {
        fn suffix(&self) -> Option<&'static str> {
            self.suffix
        }

        fn arguments(&self, _base: &ArgumentsPattern) -> ArgumentsPattern {
            self.arguments.clone()
        }

        fn metadata(&self, _base: &AggregateMetadata) -> AggregateMetadata {
            self.metadata
        }

        fn try_build(
            &self,
            _context: &DirectRouteContext<'_, '_>,
        ) -> Result<Option<AggregateCallRef>> {
            Ok(None)
        }
    }

    #[test]
    fn test_direct_name_route_produces_descriptors() {
        let base_arguments = ArgumentsPattern::fixed(vec![]);
        let if_arguments = ArgumentsPattern::if_condition(base_arguments.clone());
        let base_metadata = AggregateMetadata {
            eager_aggregation: EagerAggregation::Sum,
            ..Default::default()
        };

        let descriptors = NameRoute::new(
            &["test", "test_alias"],
            base_arguments.clone(),
            base_metadata,
            NullInput::Filter,
        )
        .then(DescriptorNode {
            suffix: None,
            arguments: base_arguments.clone(),
            metadata: base_metadata,
        })
        .then(DescriptorNode {
            suffix: Some("if"),
            arguments: if_arguments.clone(),
            metadata: base_metadata,
        })
        .into_descriptors();

        assert_eq!(descriptors.len(), 2);
        assert_eq!(descriptors[0].name, "test");
        assert_eq!(descriptors[0].aliases, ["test_alias"]);
        assert_eq!(descriptors[0].arguments(), &base_arguments);
        assert_eq!(
            descriptors[0].features().eager_aggregation,
            EagerAggregation::Sum
        );
        assert!(descriptors[0].features().supports_filter);
        assert!(!descriptors[0].features().supports_state);
        assert_eq!(descriptors[1].name, "test_if");
        assert_eq!(descriptors[1].aliases, ["test_alias_if"]);
        assert_eq!(descriptors[1].arguments(), &if_arguments);
        assert_eq!(
            descriptors[1].features().eager_aggregation,
            EagerAggregation::Sum
        );
        assert!(!descriptors[1].features().supports_filter);
    }

    #[test]
    fn test_direct_name_route_registers_descriptor_names_and_aliases() {
        let mut registry = AggregateRegistry::empty();
        let arguments = ArgumentsPattern::fixed(vec![]);
        let metadata = AggregateMetadata::default();
        NameRoute::new(
            &["test", "test_alias"],
            arguments.clone(),
            metadata,
            NullInput::Filter,
        )
        .then(DescriptorNode {
            suffix: None,
            arguments: arguments.clone(),
            metadata,
        })
        .then(DescriptorNode {
            suffix: Some("if"),
            arguments: ArgumentsPattern::if_condition(arguments.clone()),
            metadata,
        })
        .then(DescriptorNode {
            suffix: Some("state"),
            arguments,
            metadata,
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
            assert!(registry.descriptor(name).unwrap().features().supports_state);
            assert!(
                registry
                    .descriptor(name)
                    .unwrap()
                    .features()
                    .supports_filter
            );
        }
        for name in ["test_if", "test_alias_if", "test_state", "test_alias_state"] {
            assert!(!registry.descriptor(name).unwrap().features().supports_state);
            assert!(
                !registry
                    .descriptor(name)
                    .unwrap()
                    .features()
                    .supports_filter
            );
        }
    }

    #[test]
    fn test_null_result_is_independent_of_input_handling() -> Result<()> {
        fn native(build: DirectBuildContext<'_, PlainCombinator>) -> Result<AggregateCallRef> {
            assert_eq!(build.args_type(), &[DataType::Null]);
            try_create_null_argument_result_function(build.call, build.metadata)
        }
        fn filtered(_build: DirectBuildContext<'_, PlainCombinator>) -> Result<AggregateCallRef> {
            panic!("the pure NULL shortcut must bypass the filtered implementation")
        }
        for result in [NullArgumentResult::Null, NullArgumentResult::UInt64Zero] {
            for (input, build) in [
                (NullInput::Native, native as DirectBuildFn<PlainCombinator>),
                (
                    NullInput::Filter,
                    filtered as DirectBuildFn<PlainCombinator>,
                ),
            ] {
                let route = NameRoute::new(
                    &["null_contract"],
                    ArgumentsPattern::fixed(vec![ArgumentPattern::any()]),
                    AggregateMetadata {
                        null_argument_result: result,
                        ..Default::default()
                    },
                    input,
                )
                .then(PlainRoute::new(build));
                let call = route.build(RawAggregateCall {
                    name: "null_contract",
                    params: &[],
                    args_type: &[DataType::Null],
                    distinct: false,
                    order_by: &[],
                })?;
                assert_eq!(call.signature().return_type, result.value().0);
            }
        }
        Ok(())
    }

    #[test]
    fn test_descriptor_override_does_not_reconfigure_internal_route() -> Result<()> {
        let intrinsic = AggregateMetadata {
            eager_aggregation: EagerAggregation::Sum,
            documentation: super::super::super::AggregateDocumentation {
                description: "intrinsic aggregate",
                ..Default::default()
            },
            ..Default::default()
        };
        let mut descriptors = NameRoute::new(
            &["metadata_probe"],
            ArgumentsPattern::fixed(vec![]),
            intrinsic,
            NullInput::Native,
        )
        .then(PlainRoute::new(|build| {
            try_create_null_argument_result_function(build.call, build.metadata)
        }))
        .into_descriptors();
        let descriptor = descriptors.pop().unwrap();
        let arguments = descriptor.arguments().clone();
        let mut declared = descriptor.features().clone();
        declared.description = "external declaration";
        declared.eager_aggregation = EagerAggregation::Unsupported;
        let mut registry = AggregateRegistry::empty();
        registry.register(descriptor.with_metadata(arguments, declared.clone()));

        assert_eq!(
            registry.descriptor("metadata_probe").unwrap().features(),
            &declared
        );
        let call = registry.resolve(RawAggregateCall {
            name: "metadata_probe",
            params: &[],
            args_type: &[],
            distinct: false,
            order_by: &[],
        })?;
        assert_eq!(call.features().description, "intrinsic aggregate");
        assert_eq!(call.features().eager_aggregation, EagerAggregation::Sum);
        Ok(())
    }

    #[test]
    fn test_registry_redirects_distinct_without_name_route_or_suffix() {
        let mut registry = AggregateRegistry::empty();
        let arguments = ArgumentsPattern::fixed(vec![]);
        let builder = Arc::new(FixedResultBuilder {
            arguments: arguments.clone(),
            features: AggregateFeatures::default(),
        });
        registry.register(AggregateDescriptor::from_builder(
            "deduplicated_test",
            builder.clone(),
        ));

        let source_features = AggregateFeatures {
            distinct_policy: DistinctPolicy::redirect("deduplicated_test"),
            ..Default::default()
        };
        registry.register(
            AggregateDescriptor::from_builder("test", builder)
                .with_metadata(arguments, source_features),
        );

        let function = registry
            .resolve(RawAggregateCall {
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
        let rule = NameRoute::new(
            &["test"],
            ArgumentsPattern::fixed(vec![]),
            AggregateMetadata::default(),
            NullInput::Filter,
        );
        let request = RawAggregateCall {
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
