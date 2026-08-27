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

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::array::ArrayColumnBuilderMut;
use databend_common_expression::types::i256;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;

use super::super::common::get_levels;
use super::AggregateRegistration;
use super::adaptors::*;

struct QuantileDiscBuilder;

impl QuantileDiscBuilder {
    fn register(registry: &mut AggregateRegistry) {
        NameRoute::new(
            &["quantile_disc", "quantile"],
            Self::quantile_disc_arguments(),
            Self::QUANTILE_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::unary(false, Self::create))
        .then(MergeRoute::unary(true, Self::create))
        .then(PlainRoute::unary(Self::create))
        .then(IfRoute::unary(Self::create))
        .then(StateRoute::unary(Self::create))
        .register(registry);
    }
}

inventory::submit! {
    AggregateRegistration {
        register: QuantileDiscBuilder::register,
    }
}

impl QuantileDiscBuilder {
    fn quantile_disc_arguments() -> ArgumentsPattern {
        ArgumentsPattern::fixed(vec![ArgumentPattern::any_numeric()])
    }

    const QUANTILE_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns a discrete quantile value",
        definition: "quantile(level)(expr)",
        example: "select quantile(0.5)(number) from numbers(10)",
    };
}

pub struct QuantileDiscData {
    levels: Vec<f64>,
}

pub struct AggregateQuantileDiscState<T: ValueType> {
    value: Vec<T::Scalar>,
}

impl<T> Default for AggregateQuantileDiscState<T>
where T: ValueType
{
    fn default() -> Self {
        Self { value: vec![] }
    }
}

impl<T> AggregateQuantileDiscState<T>
where T: ValueType
{
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }

    fn merge_state(&mut self, rhs: &Self) {
        self.value.extend(
            rhs.value
                .iter()
                .map(|v| T::to_owned_scalar(T::to_scalar_ref(v))),
        );
    }

    fn merge_owned_state(&mut self, rhs: &mut Self) {
        self.value.append(&mut rhs.value);
    }
}

impl<T> BorshSerialize for AggregateQuantileDiscState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize,
{
    fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
        BorshSerialize::serialize(&self.value, writer)
    }
}

impl<T> BorshDeserialize for AggregateQuantileDiscState<T>
where
    T: ValueType,
    T::Scalar: BorshDeserialize,
{
    fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
        Ok(Self {
            value: Vec::<T::Scalar>::deserialize_reader(reader)?,
        })
    }
}

impl<T> UnaryState<T, ArrayType<T>> for AggregateQuantileDiscState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Ord,
{
    type FunctionInfo = QuantileDiscData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(value));
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
        mut builder: ArrayColumnBuilderMut<'_, T>,
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value_len = self.value.len();
        if function_info.levels.len() > 1 {
            let indices = function_info
                .levels
                .iter()
                .map(|level| ((value_len - 1) as f64 * (*level)).floor() as usize)
                .collect::<Vec<_>>();
            for idx in indices {
                if idx < value_len {
                    self.value.as_mut_slice().select_nth_unstable(idx);
                    let value = self.value.get(idx).unwrap();
                    builder.put_item(T::to_scalar_ref(value));
                } else {
                    builder.push_default();
                }
            }
            builder.commit_row();
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

impl<T> UnaryState<T, T> for AggregateQuantileDiscState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Ord,
{
    type FunctionInfo = QuantileDiscData;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.value.push(T::to_owned_scalar(value));
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
        function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let value_len = self.value.len();
        let idx = ((value_len - 1) as f64 * function_info.levels[0]).floor() as usize;
        if idx >= value_len {
            builder.push_default();
        } else {
            self.value.as_mut_slice().select_nth_unstable(idx);
            let value = self.value.get(idx).unwrap();
            builder.push_item(T::to_scalar_ref(value));
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

impl QuantileDiscBuilder {
    fn create(build: UnaryBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        let data_type = build.arg_type().clone();
        let display_name = build.name().to_string();
        let levels = get_levels(build.params())?;

        with_number_mapped_type!(|NUM| match &data_type {
            DataType::Number(NumberDataType::NUM) => {
                Self::create_instance::<NumberType<NUM>>(build, data_type.clone(), levels)
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => {
                        Self::create_instance::<DecimalType<DECIMAL>>(
                            build,
                            data_type.clone(),
                            levels,
                        )
                    }
                })
            }
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                display_name, data_type
            ))),
        })
    }

    fn create_instance<T>(
        build: UnaryBuildContext<'_, impl Combinator>,
        data_type: DataType,
        levels: Vec<f64>,
    ) -> Result<AggregateCallRef>
    where
        T: AccessType + ValueType,
        T::Scalar: BorshSerialize + BorshDeserialize + Ord,
    {
        if levels.len() > 1 {
            Self::create_typed::<T, ArrayType<T>>(
                build,
                DataType::Array(Box::new(data_type)),
                levels,
            )
        } else {
            Self::create_typed::<T, T>(build, data_type, levels)
        }
    }

    fn create_typed<I, R>(
        build: UnaryBuildContext<'_, impl Combinator>,
        return_type: DataType,
        levels: Vec<f64>,
    ) -> Result<AggregateCallRef>
    where
        I: AccessType + ValueType,
        I::Scalar: BorshSerialize + BorshDeserialize,
        R: ValueType,
        AggregateQuantileDiscState<I>: UnaryState<I, R, FunctionInfo = QuantileDiscData>,
    {
        let state = AggregateQuantileDiscState::<I>::state_description();

        build.create_unary_or_null::<AggregateQuantileDiscState<I>, I, R>(
            return_type.wrap_nullable(),
            state,
            QuantileDiscData { levels },
        )
    }
}
