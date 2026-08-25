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
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::ops::AddAssign;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::i256;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;

use super::AggregateRegistration;
use super::adaptors::*;

struct ModeBuilder;

impl ModeBuilder {
    fn register(registry: &mut AggregateRegistry) {
        DirectNameRoute::new(
            &["mode"],
            ModeBuilder::mode_arguments(),
            ModeBuilder::MODE_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, ModeBuilder::create))
        .then(MergeRoute::unary(true, ModeBuilder::create))
        .then(PlainRoute::unary(ModeBuilder::create))
        .then(IfRoute::unary(ModeBuilder::create))
        .then(StateRoute::unary(ModeBuilder::create))
        .register(registry);
    }
}

inventory::submit! {
    AggregateRegistration {
        register: ModeBuilder::register,
    }
}

impl ModeBuilder {
    fn mode_arguments() -> ArgumentsPattern {
        ArgumentsPattern::fixed(vec![ArgumentPattern::any()])
    }

    const MODE_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the most frequent input value",
        definition: "mode(expr)",
        example: "select mode(number) from numbers(10)",
    };
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct AggregateModeState<T>
where
    T: ValueType,
    T::Scalar: Ord + Hash + BorshSerialize + BorshDeserialize,
{
    frequency_map: HashMap<T::Scalar, u64>,
}

impl<T> Default for AggregateModeState<T>
where
    T: ValueType,
    T::Scalar: Ord + Hash + BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self {
            frequency_map: HashMap::new(),
        }
    }
}

impl<T> AggregateModeState<T>
where
    T: ValueType,
    T::Scalar: Ord + Hash + BorshSerialize + BorshDeserialize,
{
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }

    fn add_value(&mut self, value: T::ScalarRef<'_>) {
        let value = T::to_owned_scalar(value);
        match self.frequency_map.entry(value) {
            Entry::Occupied(entry) => *entry.into_mut() += 1,
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
        }
    }

    fn merge_state(&mut self, rhs: &Self) {
        for (key, value) in rhs.frequency_map.iter() {
            match self.frequency_map.get_mut(key) {
                Some(entry) => entry.add_assign(value),
                None => {
                    self.frequency_map.insert(key.clone(), *value);
                }
            }
        }
    }

    fn merge_owned_state(&mut self, rhs: &mut Self) {
        for (key, value) in std::mem::take(&mut rhs.frequency_map) {
            match self.frequency_map.entry(key) {
                Entry::Occupied(entry) => *entry.into_mut() += value,
                Entry::Vacant(entry) => {
                    entry.insert(value);
                }
            }
        }
    }
}

impl<T> UnaryState<T, T> for AggregateModeState<T>
where
    T: ValueType,
    T::Scalar: Ord + Hash + BorshSerialize + BorshDeserialize,
{
    type FunctionInfo = ();

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.add_value(value);
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.merge_state(rhs);
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned_state(rhs);
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: T::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        match self
            .frequency_map
            .iter()
            .max_by_key(|&(_, frequency)| frequency)
        {
            Some((value, _)) => builder.push_item(T::to_scalar_ref(value)),
            None => builder.push_default(),
        }
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(self, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let ScalarRef::Binary(mut data) = value else {
            unreachable!()
        };
        let rhs = Self::deserialize_reader(&mut data)?;
        self.merge_state(&rhs);
        Ok(())
    }
}

impl ModeBuilder {
    fn create(build: UnaryBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        let data_type = build.arg_type().clone();

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                Self::create_instance::<NumberType<NUM>>(build, data_type.clone())
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => {
                        Self::create_instance::<DecimalType<DECIMAL>>(build, data_type.clone())
                    }
                })
            }
            data_type => Self::create_instance::<AnyType>(build, data_type.clone()),
        })
    }

    fn create_instance<T>(
        build: UnaryBuildContext<'_, impl Combinator>,
        return_type: DataType,
    ) -> Result<AggregateCallRef>
    where
        T: AccessType + ValueType,
        T::Scalar: Ord + Hash + BorshSerialize + BorshDeserialize,
    {
        let state = AggregateModeState::<T>::state_description();

        build.create_unary_or_null::<AggregateModeState<T>, T, T>(
            return_type.wrap_nullable(),
            state,
            (),
        )
    }
}
