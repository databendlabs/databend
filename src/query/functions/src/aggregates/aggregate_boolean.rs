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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BuilderMut;
use databend_common_expression::types::*;
use databend_common_expression::AggrStateLoc;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::SELECTIVITY_THRESHOLD;

use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge1;
use super::AggrState;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::AggregateUnaryFunction;
use super::SerializeInfo;
use super::StateSerde;
use super::UnaryState;

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
    fn add(&mut self, other: bool, _: &Self::FunctionInfo) -> Result<()> {
        if IS_AND {
            self.value &= other;
        } else {
            self.value |= other;
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: ColumnView<BooleanType>,
        validity: Option<&Bitmap>,
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        let other = match other {
            ColumnView::Const(b, n) => Bitmap::new_constant(b, n),
            ColumnView::Column(column) => column,
        };
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
        _: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push(self.value);
        Ok(())
    }
}

impl<const IS_AND: bool> StateSerde for BooleanState<IS_AND> {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![DataType::Boolean.into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let builder = builders[0].as_boolean_mut().unwrap();
        for place in places {
            let state: &mut Self = AggrState::new(*place, loc).get();
            builder.push(state.value);
        }
        Ok(())
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<BooleanType, Self, _>(places, loc, state, filter, |state, value| {
            state.merge(&Self { value })
        })
    }
}

pub fn try_create_aggregate_boolean_function<const IS_AND: bool>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, arguments.len())?;

    let mut data_type = arguments[0].clone();
    // null use dummy func, it's already covered in `AggregateNullResultFunction`
    if data_type.is_null() {
        data_type = BooleanType::data_type();
    }

    match &data_type {
        DataType::Boolean => {
            let return_type = DataType::Boolean;
            AggregateUnaryFunction::<BooleanState<IS_AND>, BooleanType, BooleanType>::create(
                display_name,
                return_type,
            )
        }

        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}', must be boolean type",
            display_name, arguments[0]
        ))),
    }
}

pub fn aggregate_boolean_function_desc<const IS_AND: bool>() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_boolean_function::<IS_AND>),
        features,
    )
}
