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

use super::AggregateRegistration;
use super::adaptors::*;

struct BooleanBuilder;

impl BooleanBuilder {
    fn register(registry: &mut AggregateRegistry) {
        DirectNameRoute::new(
            &["bool_and"],
            BooleanBuilder::boolean_arguments(),
            BooleanBuilder::BOOL_AND_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, Self::create::<true>))
        .then(MergeRoute::unary(true, Self::create::<true>))
        .then(PlainRoute::unary(Self::create::<true>))
        .then(IfRoute::unary(Self::create::<true>))
        .then(StateRoute::unary(Self::create::<true>))
        .then(DistinctAliasRoute::unary(Self::create::<true>))
        .register(registry);
        DirectNameRoute::new(
            &["bool_or"],
            BooleanBuilder::boolean_arguments(),
            BooleanBuilder::BOOL_OR_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, Self::create::<false>))
        .then(MergeRoute::unary(true, Self::create::<false>))
        .then(PlainRoute::unary(Self::create::<false>))
        .then(IfRoute::unary(Self::create::<false>))
        .then(StateRoute::unary(Self::create::<false>))
        .then(DistinctAliasRoute::unary(Self::create::<false>))
        .register(registry);
    }
}

inventory::submit! {
    AggregateRegistration {
        register: BooleanBuilder::register,
    }
}

impl BooleanBuilder {
    fn boolean_arguments() -> ArgumentsPattern {
        ArgumentsPattern::fixed(vec![ArgumentPattern::exact(DataType::Boolean)])
    }

    const BOOL_AND_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns true when all non-null input values are true",
        definition: "bool_and(expr)",
        example: "select bool_and(flag) from t",
    };

    const BOOL_OR_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
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
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(DataType::Boolean),
        ])
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
    fn create<const IS_AND: bool>(
        build: UnaryBuildContext<'_, impl Combinator>,
    ) -> Result<AggregateCallRef> {
        debug_assert_eq!(build.arg_type(), &DataType::Boolean);
        build.create_unary_or_null::<AggregateBooleanState<IS_AND>, BooleanType, BooleanType>(
            DataType::Boolean.wrap_nullable(),
            AggregateBooleanState::<IS_AND>::state_description(),
            (),
        )
    }
}
