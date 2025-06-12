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

use boolean::TrueIdxIter;
use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BuilderMut;
use databend_common_expression::types::*;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::Scalar;
use databend_common_expression::SELECTIVITY_THRESHOLD;

use super::assert_unary_arguments;
use super::FunctionData;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregate_function_factory::AggregateFunctionSortDesc;
use crate::aggregates::aggregate_unary::UnaryState;
use crate::aggregates::AggregateUnaryFunction;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct BooleanState<const IS_AND: bool> {
    pub value: bool,
}

impl<const IS_AND: bool> Default for BooleanState<IS_AND> {
    fn default() -> Self {
        BooleanState { value: IS_AND }
    }
}

#[inline]
pub fn boolean_batch<const IS_AND: bool>(inner: Bitmap, validity: Option<&Bitmap>) -> bool {
    match validity {
        Some(v) => {
            let mut res = IS_AND;
            if v.true_count() as f64 / v.len() as f64 >= SELECTIVITY_THRESHOLD {
                inner.iter().zip(v.iter()).for_each(|(t, b)| {
                    if b {
                        if IS_AND {
                            res &= t;
                        } else {
                            res |= t;
                        }
                    }
                });
            } else {
                TrueIdxIter::new(v.len(), Some(v)).for_each(|idx| {
                    if IS_AND {
                        res &= unsafe { inner.get_bit_unchecked(idx) };
                    } else {
                        res |= unsafe { inner.get_bit_unchecked(idx) };
                    }
                });
            }
            res
        }
        _ => {
            if IS_AND {
                inner.iter().all(|t| t)
            } else {
                inner.iter().any(|t| t)
            }
        }
    }
}

impl<const IS_AND: bool> UnaryState<BooleanType, BooleanType> for BooleanState<IS_AND> {
    fn add(&mut self, other: bool, _function_data: Option<&dyn FunctionData>) -> Result<()> {
        if IS_AND {
            self.value &= other;
        } else {
            self.value |= other;
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: Bitmap,
        validity: Option<&Bitmap>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        if IS_AND {
            self.value &= boolean_batch::<IS_AND>(other, validity);
        } else {
            self.value |= boolean_batch::<IS_AND>(other, validity);
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if IS_AND {
            self.value &= rhs.value;
        } else {
            self.value |= rhs.value;
        }
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: BuilderMut<'_, BooleanType>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        builder.push(self.value);
        Ok(())
    }
}

pub fn try_create_aggregate_boolean_function<const IS_AND: bool>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    let mut data_type = arguments[0].clone();
    // null use dummy func, it's already covered in `AggregateNullResultFunction`
    if data_type.is_null() {
        data_type = BooleanType::data_type();
    }

    match &data_type {
        DataType::Boolean => {
            let return_type = DataType::Boolean;
            AggregateUnaryFunction::<BooleanState<IS_AND>, BooleanType, BooleanType>::try_create_unary(
                display_name,
                return_type,
                params,
                arguments[0].clone(),
            )
        }

        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}', must be boolean type",
            display_name, arguments[0]
        ))),
    }
}

pub fn aggregate_boolean_function_desc<const IS_AND: bool>() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_boolean_function::<IS_AND>),
        features,
    )
}
