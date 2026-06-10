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

use std::alloc::Layout;

use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;

use super::AggregateFunctionDefinition;
use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::adaptors_v2::UnaryState;

struct BooleanBuilder;

impl BooleanBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        let bool_and = AggregateFunctionDefinition::new(
            "bool_and",
            BooleanBuilder::boolean_arguments(),
            BooleanBuilder::BOOL_AND_FEATURES,
            BooleanBuilder::try_create::<true>,
        );
        bool_and.register_with_combinators(registry, true);
        let bool_or = AggregateFunctionDefinition::new(
            "bool_or",
            BooleanBuilder::boolean_arguments(),
            BooleanBuilder::BOOL_OR_FEATURES,
            BooleanBuilder::try_create::<false>,
        );
        bool_or.register_with_combinators(registry, true);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: BooleanBuilder::register,
    }
}

impl BooleanBuilder {
    fn boolean_arguments() -> v2::AggregateArgumentsPattern {
        v2::AggregateArgumentsPattern::fixed(vec![v2::AggregateArgumentPattern::exact(
            DataType::Boolean,
        )])
    }

    const BOOL_AND_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: true,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns true when all non-null input values are true",
        definition: "bool_and(expr)",
        example: "select bool_and(flag) from t",
    };

    const BOOL_OR_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: true,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns true when any non-null input value is true",
        definition: "bool_or(expr)",
        example: "select bool_or(flag) from t",
    };
}

pub struct AggregateBooleanState<const IS_AND: bool> {
    value: bool,
}

impl<const IS_AND: bool> Default for AggregateBooleanState<IS_AND> {
    fn default() -> Self {
        Self { value: IS_AND }
    }
}

impl<const IS_AND: bool> AggregateBooleanState<IS_AND> {
    pub fn state_description() -> v2::AggregateStateDescription {
        v2::AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<Self>())],
            vec![StateSerdeItem::DataType(DataType::Boolean)],
        )
    }
}

impl<const IS_AND: bool> UnaryState<BooleanType, BooleanType> for AggregateBooleanState<IS_AND> {
    type FunctionInfo = ();

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: bool, _function_info: &Self::FunctionInfo) -> Result<()> {
        if IS_AND {
            self.value &= value;
        } else {
            self.value |= value;
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.add(rhs.value, &())
    }

    fn merge_result(
        &mut self,
        mut builder: <BooleanType as ValueType>::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push_item(self.value);
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push(ScalarRef::Boolean(self.value));
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let ScalarRef::Boolean(value) = value else {
            unreachable!()
        };
        self.add(value, &())
    }
}

impl BooleanBuilder {
    fn try_create<const IS_AND: bool>(
        request: v2::AggregateFunctionRequest<'_>,
    ) -> Result<v2::AggregateFunctionRef> {
        let features = if IS_AND {
            Self::BOOL_AND_FEATURES
        } else {
            Self::BOOL_OR_FEATURES
        };
        let names = if IS_AND {
            &["bool_and"][..]
        } else {
            &["bool_or"][..]
        };
        v2::build_default_name_route_with_unary_input(
            request,
            names,
            features,
            false,
            v2::UnaryAggregateFunctionBuildInputFns::new(
                Self::create::<IS_AND>,
                Self::create::<IS_AND>,
                Self::create::<IS_AND>,
                v2::UnaryDistinctBuildFn::PlainAlias(Self::create::<IS_AND>),
            ),
        )
    }

    fn create<const IS_AND: bool>(
        build: v2::UnaryBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<v2::AggregateFunctionRef> {
        debug_assert_eq!(build.arg_type(), &DataType::Boolean);
        build.create_unary_or_null::<AggregateBooleanState<IS_AND>, BooleanType, BooleanType>(
            DataType::Boolean.wrap_nullable(),
            AggregateBooleanState::<IS_AND>::state_description(),
            (),
        )
    }
}
