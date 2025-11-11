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

use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::*;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::StateAddr;

use super::aggregate_scalar_state::ChangeIf;
use super::batch_merge1;
use super::batch_serialize1;
use super::SerializeInfo;
use super::StateSerde;
use super::StateSerdeItem;
use super::UnaryState;

pub struct MinMaxAnyDecimalState<T, C>
where
    T: ValueType,
    T::Scalar: Decimal,
    C: ChangeIf<T>,
{
    pub value: Option<<T::Scalar as Decimal>::U64Array>,
    _c: PhantomData<C>,
}

impl<T, C> Default for MinMaxAnyDecimalState<T, C>
where
    T: ValueType,
    T::Scalar: Decimal,
    C: ChangeIf<T>,
{
    fn default() -> Self {
        Self {
            value: None,
            _c: PhantomData,
        }
    }
}

impl<T, C> UnaryState<T, T> for MinMaxAnyDecimalState<T, C>
where
    T: ValueType,
    T::Scalar: Decimal,
    C: ChangeIf<T>,
{
    fn add(&mut self, other: T::ScalarRef<'_>, _: &Self::FunctionInfo) -> Result<()> {
        match self.value {
            Some(v) => {
                let v = T::Scalar::from_u64_array(v);
                if C::change_if(&T::to_scalar_ref(&v), &other) {
                    self.value = Some(T::Scalar::to_u64_array(T::to_owned_scalar(other)));
                }
            }
            None => {
                self.value = Some(T::Scalar::to_u64_array(T::to_owned_scalar(other)));
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
        match validity {
            Some(validity)
                if validity.null_count() > 0 && validity.null_count() < validity.len() =>
            {
                let v = column_iter
                    .zip(validity)
                    .filter(|(_, valid)| *valid)
                    .map(|(v, _)| v)
                    .reduce(|l, r| if !C::change_if(&l, &r) { l } else { r });
                if let Some(v) = v {
                    self.add(v, &())?;
                }
            }
            Some(validity) if validity.null_count() == validity.len() => {}
            _ => {
                let v = column_iter.reduce(|l, r| if !C::change_if(&l, &r) { l } else { r });
                if let Some(v) = v {
                    self.add(v, &())?;
                }
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(v) = rhs.value {
            let v = T::Scalar::from_u64_array(v);
            self.add(T::to_scalar_ref(&v), &())?;
        }
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: T::ColumnBuilderMut<'_>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        if let Some(v) = self.value {
            let v = T::Scalar::from_u64_array(v);
            builder.push_item(T::to_scalar_ref(&v));
        } else {
            builder.push_default();
        }
        Ok(())
    }
}

impl<T, C> StateSerde for MinMaxAnyDecimalState<T, C>
where
    T: ValueType,
    T::Scalar: Decimal,
    C: ChangeIf<T>,
{
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Decimal(T::Scalar::default_decimal_size())
            .wrap_nullable()
            .into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<NullableType<DecimalType<T::Scalar>>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                builder.push_item(state.value.map(T::Scalar::from_u64_array));
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
        batch_merge1::<NullableType<DecimalType<T::Scalar>>, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, value| {
                let rhs = Self {
                    value: value.map(T::Scalar::to_u64_array),
                    _c: PhantomData,
                };
                <Self as UnaryState<T, T>>::merge(state, &rhs)
            },
        )
    }
}
