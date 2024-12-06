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

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;

use super::aggregate_scalar_state::ChangeIf;
use super::FunctionData;
use super::UnaryState;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct MinMaxAnyDecimalState<T, C>
where
    T: ValueType,
    T::Scalar: Decimal,
    <T::Scalar as Decimal>::U64Array: BorshSerialize + BorshDeserialize,
    C: ChangeIf<T>,
{
    pub value: Option<<T::Scalar as Decimal>::U64Array>,
    #[borsh(skip)]
    _c: PhantomData<C>,
}

impl<T, C> Default for MinMaxAnyDecimalState<T, C>
where
    T: ValueType,
    T::Scalar: Decimal,
    <T::Scalar as Decimal>::U64Array: BorshSerialize + BorshDeserialize,
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
    <T::Scalar as Decimal>::U64Array: BorshSerialize + BorshDeserialize,
    C: ChangeIf<T> + Default,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
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
        other: T::Column,
        validity: Option<&Bitmap>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let column_len = T::column_len(&other);
        if column_len == 0 {
            return Ok(());
        }

        let column_iter = T::iter_column(&other);
        match validity {
            Some(validity)
                if validity.unset_bits() > 0 && validity.unset_bits() < validity.len() =>
            {
                let v = column_iter
                    .zip(validity)
                    .filter(|(_, valid)| *valid)
                    .map(|(v, _)| v)
                    .reduce(|l, r| if !C::change_if(&l, &r) { l } else { r });
                if let Some(v) = v {
                    let _ = self.add(v, function_data);
                }
            }
            Some(validity) if validity.unset_bits() == validity.len() => {}
            _ => {
                let v = column_iter.reduce(|l, r| if !C::change_if(&l, &r) { l } else { r });
                if let Some(v) = v {
                    let _ = self.add(v, function_data);
                }
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(v) = rhs.value {
            let v = T::Scalar::from_u64_array(v);
            self.add(T::to_scalar_ref(&v), None)?;
        }
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut T::ColumnBuilder,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        if let Some(v) = self.value {
            let v = T::Scalar::from_u64_array(v);
            T::push_item(builder, T::to_scalar_ref(&v));
        } else {
            T::push_default(builder);
        }
        Ok(())
    }
}
