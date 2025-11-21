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

use std::marker::PhantomData;
use std::sync::Arc;

use boolean::TrueIdxIter;
use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::SELECTIVITY_THRESHOLD;

use super::aggregate_min_max_any_decimal::MinMaxAnyDecimalState;
use super::aggregate_scalar_state::ChangeIf;
use super::aggregate_scalar_state::CmpAny;
use super::aggregate_scalar_state::CmpMax;
use super::aggregate_scalar_state::CmpMin;
use super::aggregate_scalar_state::TYPE_ANY;
use super::aggregate_scalar_state::TYPE_MAX;
use super::aggregate_scalar_state::TYPE_MIN;
use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge1;
use super::batch_merge2;
use super::batch_serialize1;
use super::batch_serialize2;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionFeatures;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::SerializeInfo;
use super::StateSerde;
use super::StateSerdeItem;
use super::UnaryState;
use crate::with_compare_mapped_type;
use crate::with_simple_no_number_no_string_mapped_type;

#[derive(Default)]
pub struct MinMaxStringState<C>
where C: ChangeIf<StringType>
{
    pub value: Option<String>,
    _c: PhantomData<C>,
}

impl<C> UnaryState<StringType, StringType> for MinMaxStringState<C>
where C: ChangeIf<StringType>
{
    fn add(&mut self, other: &str, _: &Self::FunctionInfo) -> Result<()> {
        match &self.value {
            Some(v) => {
                if C::change_if(&StringType::to_scalar_ref(v), &other) {
                    self.value = Some(StringType::to_owned_scalar(other));
                }
            }
            None => {
                self.value = Some(StringType::to_owned_scalar(other));
            }
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: ColumnView<StringType>,
        validity: Option<&Bitmap>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        let column_len = other.len();
        if column_len == 0 {
            return Ok(());
        }

        let column_iter = 0..column_len;
        if let Some(validity) = validity {
            if validity.null_count() == column_len {
                return Ok(());
            }
            let v = column_iter
                .zip(validity)
                .filter(|(_, valid)| *valid)
                .map(|(idx, _)| idx)
                .reduce(|l, r| {
                    let ordering = match &other {
                        ColumnView::Const(_, _) => std::cmp::Ordering::Equal,
                        ColumnView::Column(other) => StringColumn::compare(other, l, other, r),
                    };
                    if !C::change_if_ordering(ordering) {
                        l
                    } else {
                        r
                    }
                });
            if let Some(v) = v {
                self.add(other.index(v).unwrap(), &())?;
            }
        } else {
            let v = column_iter.reduce(|l, r| {
                let ordering = match &other {
                    ColumnView::Const(_, _) => std::cmp::Ordering::Equal,
                    ColumnView::Column(other) => StringColumn::compare(other, l, other, r),
                };
                if !C::change_if_ordering(ordering) {
                    l
                } else {
                    r
                }
            });
            if let Some(v) = v {
                self.add(other.index(v).unwrap(), &())?;
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(v) = &rhs.value {
            self.add(v.as_str(), &())?;
        }
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: BuilderMut<'_, StringType>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        if let Some(v) = &self.value {
            builder.push_item(v.as_str());
        } else {
            builder.push_default();
        }
        Ok(())
    }
}

impl<C> StateSerde for MinMaxStringState<C>
where C: ChangeIf<StringType>
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::String.wrap_nullable().into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<NullableType<StringType>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                builder.push_item(state.value.as_deref());
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<NullableType<StringType>, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, value| {
                let rhs = Self {
                    value: value.map(String::from),
                    _c: PhantomData,
                };
                state.merge(&rhs)
            },
        )
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct MinMaxAnyState<T, C>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    pub value: Option<T::Scalar>,
    #[borsh(skip)]
    _c: PhantomData<C>,
}

impl<T, C> Default for MinMaxAnyState<T, C>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
    C: ChangeIf<T>,
{
    fn default() -> Self {
        Self {
            value: None,
            _c: PhantomData,
        }
    }
}

impl<T, C> UnaryState<T, T> for MinMaxAnyState<T, C>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
    C: ChangeIf<T>,
{
    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        match &self.value {
            Some(v) => {
                if C::change_if(&T::to_scalar_ref(v), &other) {
                    self.value = Some(T::to_owned_scalar(other));
                }
            }
            None => {
                self.value = Some(T::to_owned_scalar(other));
            }
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: ColumnView<T>,
        validity: Option<&Bitmap>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        let column_len = other.len();
        if column_len == 0 {
            return Ok(());
        }

        let column_iter = other.iter();
        if let Some(v) = validity {
            if v.true_count() as f64 / v.len() as f64 >= SELECTIVITY_THRESHOLD {
                let value = column_iter
                    .zip(v.iter())
                    .filter(|(_, v)| *v)
                    .map(|(v, _)| v)
                    .reduce(|l, r| if !C::change_if(&l, &r) { l } else { r });

                if let Some(value) = value {
                    self.add(value, &())?;
                }
            } else {
                for idx in TrueIdxIter::new(v.len(), Some(v)) {
                    self.add(unsafe { other.index_unchecked(idx) }, &())?;
                }
            };
        } else {
            let v = column_iter.reduce(|l, r| if !C::change_if(&l, &r) { l } else { r });
            if let Some(v) = v {
                self.add(v, &())?;
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(v) = &rhs.value {
            self.add(T::to_scalar_ref(v), &())?;
        }
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: T::ColumnBuilderMut<'_>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        if let Some(v) = &self.value {
            builder.push_item(T::to_scalar_ref(v));
        } else {
            builder.push_default();
        }

        Ok(())
    }
}

impl<T, C> StateSerde for MinMaxAnyState<T, C>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
    C: ChangeIf<T>,
{
    fn serialize_type(info: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        let data_type = info
            .and_then(|data| data.as_any().downcast_ref::<DataType>())
            .cloned()
            .unwrap();
        vec![DataType::Boolean.into(), data_type.into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize2::<BooleanType, T, Self, _>(
            places,
            loc,
            builders,
            |state, (flag_builder, value_builder)| {
                match &state.value {
                    Some(v) => {
                        flag_builder.push_item(true);
                        value_builder.push_item(T::to_scalar_ref(v));
                    }
                    None => {
                        flag_builder.push_item(false);
                        value_builder.push_default();
                    }
                }
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge2::<BooleanType, T, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, (flag, value)| {
                let rhs = Self {
                    value: flag.then_some(T::to_owned_scalar(value)),
                    _c: PhantomData,
                };
                state.merge(&rhs)
            },
        )
    }
}

fn need_manual_drop_state(data_type: &DataType) -> bool {
    match data_type {
        DataType::String | DataType::Variant => true,
        DataType::Nullable(t) | DataType::Array(t) | DataType::Map(t) => need_manual_drop_state(t),
        DataType::Tuple(ts) => ts.iter().any(need_manual_drop_state),
        _ => false,
    }
}

pub fn try_create_aggregate_min_max_any_function<const CMP_TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, argument_types.len())?;
    let mut data_type = argument_types[0].clone();
    let need_drop = need_manual_drop_state(&data_type);

    // null use dummy func, it's already covered in `AggregateNullResultFunction`
    if data_type.is_null() {
        data_type = DataType::String;
    }

    with_compare_mapped_type!(|CMP| match CMP_TYPE {
        CMP => {
            with_simple_no_number_no_string_mapped_type!(|T| match data_type {
                DataType::T => {
                    let return_type = data_type.clone();
                    AggregateUnaryFunction::<MinMaxAnyState<T, CMP>, T, T>::new(
                        display_name,
                        return_type,
                    )
                    .with_serialize_info(Box::new(data_type))
                    .with_need_drop(need_drop)
                    .finish()
                }
                DataType::String => {
                    let return_type = data_type.clone();
                    AggregateUnaryFunction::<MinMaxStringState<CMP>, StringType, StringType>::new(
                        display_name,
                        return_type,
                    )
                    .with_need_drop(need_drop)
                    .finish()
                }
                DataType::Number(num_type) => {
                    with_number_mapped_type!(|NUM| match num_type {
                        NumberDataType::NUM => {
                            let return_type = data_type.clone();
                            AggregateUnaryFunction::<
                                MinMaxAnyState<NumberType<NUM>, CMP>,
                                NumberType<NUM>,
                                NumberType<NUM>,
                            >::new(display_name, return_type)
                            .with_serialize_info(Box::new(data_type))
                            .finish()
                        }
                    })
                }
                DataType::Decimal(size) => {
                    with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                        DecimalDataKind::DECIMAL => {
                            let return_type = DataType::Decimal(size);
                            AggregateUnaryFunction::<
                                MinMaxAnyDecimalState<DecimalType<DECIMAL>, CMP>,
                                DecimalType<DECIMAL>,
                                DecimalType<DECIMAL>,
                            >::create(display_name, return_type)
                        }
                    })
                }
                _ => {
                    let return_type = data_type.clone();
                    AggregateUnaryFunction::<MinMaxAnyState<AnyType, CMP>, AnyType, AnyType>::new(
                        display_name,
                        return_type,
                    )
                    .with_serialize_info(Box::new(data_type))
                    .with_need_drop(need_drop)
                    .finish()
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare type for aggregate function {} (type number: {})",
            display_name, CMP_TYPE
        ))),
    })
}

pub fn aggregate_min_function_desc() -> AggregateFunctionDescription {
    let features = AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_min_max_any_function::<TYPE_MIN>),
        features,
    )
}

pub fn aggregate_max_function_desc() -> AggregateFunctionDescription {
    let features = AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_min_max_any_function::<TYPE_MAX>),
        features,
    )
}

pub fn aggregate_any_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_min_max_any_function::<TYPE_ANY>,
    ))
}
