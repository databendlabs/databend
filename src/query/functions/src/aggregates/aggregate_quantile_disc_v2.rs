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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_number;
use common_expression::types::decimal::*;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::AggregateFunction;
use common_expression::ColumnBuilder;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use ethnum::i256;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use crate::aggregates::aggregate_unary::AggregateUnaryFunction;
use crate::aggregates::aggregate_unary::UnaryState;
use crate::with_simple_no_number_mapped_type;
use crate::BUILTIN_FUNCTIONS;

#[derive(Serialize, Deserialize, Clone)]
pub struct QuantileStateV2<T>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned + Sync + Send,
{
    #[serde(bound(deserialize = "T::Scalar: DeserializeOwned"))]
    pub value: Vec<T::Scalar>,
    pub levels: Vec<f64>,
}

impl<T> Default for QuantileStateV2<T>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned + Sync + Send,
{
    fn default() -> Self {
        Self {
            value: vec![],
            levels: vec![0.5],
        }
    }
}

impl<T> QuantileStateV2<T>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned + Sync + Send + Ord,
{
    pub fn new(params: Vec<Scalar>) -> Result<Self> {
        let levels = if params.len() == 1 {
            let level: F64 = check_number(
                None,
                &FunctionContext::default(),
                &Expr::<usize>::Constant {
                    span: None,
                    scalar: params[0].clone(),
                    data_type: params[0].as_ref().infer_data_type(),
                },
                &BUILTIN_FUNCTIONS,
            )?;
            let level = level.0;
            if !(0.0..=1.0).contains(&level) {
                return Err(ErrorCode::BadDataValueType(format!(
                    "level range between [0, 1], got: {:?}",
                    level
                )));
            }
            vec![level]
        } else if params.is_empty() {
            vec![0.5f64]
        } else {
            let mut levels = Vec::with_capacity(params.len());
            for param in params {
                let level: F64 = check_number(
                    None,
                    &FunctionContext::default(),
                    &Expr::<usize>::Cast {
                        span: None,
                        is_try: false,
                        expr: Box::new(Expr::Constant {
                            span: None,
                            scalar: param.clone(),
                            data_type: param.as_ref().infer_data_type(),
                        }),
                        dest_type: DataType::Number(NumberDataType::Float64),
                    },
                    &BUILTIN_FUNCTIONS,
                )?;
                let level = level.0;
                if !(0.0..=1.0).contains(&level) {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "level range between [0, 1], got: {:?} in levels",
                        level
                    )));
                }
                levels.push(level);
            }
            levels
        };

        Ok(Self {
            value: vec![],
            levels,
        })
    }
}

impl<T> UnaryState<T> for QuantileStateV2<T>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned + Sync + Send + Ord,
{
    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        if let Some(v) = other {
            self.value.push(T::to_owned_scalar(v));
        }
    }

    fn add_batch(&mut self, column: &T::Column, validity: Option<&Bitmap>) -> Result<()> {
        let column_len = T::column_len(column);
        if column_len == 0 {
            return Ok(());
        }
        let column_iter = T::iter_column(column);
        if let Some(validity) = validity {
            if validity.unset_bits() == column_len {
                return Ok(());
            }
            for (data, valid) in column_iter.zip(validity.iter()) {
                if !valid {
                    continue;
                }
                self.add(Some(data));
            }
        } else {
            self.value
                .extend(column_iter.map(|data| T::to_owned_scalar(data)));
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(
            rhs.value
                .iter()
                .map(|v| T::to_owned_scalar(T::to_scalar_ref(v))),
        );
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let value_len = self.value.len();
        if self.levels.len() > 1 {
            let builder = match builder {
                ColumnBuilder::Array(box b) => b,
                _ => unreachable!(),
            };
            let indices = self
                .levels
                .iter()
                .map(|level| ((value_len - 1) as f64 * (*level)).floor() as usize)
                .collect::<Vec<usize>>();
            for idx in indices {
                if idx < value_len {
                    self.value.as_mut_slice().select_nth_unstable(idx);
                    let value = self.value.get(idx).unwrap();
                    builder.put_item(T::upcast_scalar(value.clone()).as_ref());
                } else {
                    builder.push_default();
                }
            }
            builder.commit_row();
        } else {
            let builder = T::try_downcast_builder(builder).unwrap();
            let idx = ((value_len - 1) as f64 * self.levels[0]).floor() as usize;
            if idx >= value_len {
                T::push_default(builder);
            } else {
                self.value.as_mut_slice().select_nth_unstable(idx);
                let value = self.value.get(idx).unwrap();
                T::push_item(builder, T::to_scalar_ref(value));
            }
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, self)
    }

    fn deserialize(&self, reader: &mut &[u8]) -> Result<Self>
    where Self: Sized {
        deserialize_state::<Self>(reader)
    }

    fn need_manual_drop_state() -> bool {
        true
    }
}

pub(crate) fn create_quantile_disc(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    data_type: DataType,
) -> Result<Arc<dyn AggregateFunction>> {
    with_simple_no_number_mapped_type!(|T| match &arguments[0] {
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    let return_type = if params.len() > 1 {
                        DataType::Array(Box::new(data_type))
                    } else {
                        data_type
                    };
                    let state = QuantileStateV2::<NumberType<NUM>>::new(params.clone())?;
                    AggregateUnaryFunction::<
                                QuantileStateV2<NumberType<NUM>>,
                                NumberType<NUM>,
                            >::try_create(
                                display_name,
                                return_type,
                                params,
                                arguments[0].clone(),
                                state,
                            )
                }
            })
        }
        DataType::Decimal(DecimalDataType::Decimal128(s)) => {
            let decimal_size = DecimalSize {
                precision: s.precision,
                scale: s.scale,
            };
            let data_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
            let return_type = if params.len() > 1 {
                DataType::Array(Box::new(data_type))
            } else {
                data_type
            };
            let state = QuantileStateV2::<DecimalType<i128>>::new(params.clone())?;
            AggregateUnaryFunction::<QuantileStateV2<DecimalType<i128>>, DecimalType<i128>>::try_create(
                        display_name,
                        return_type,
                        params,
                        arguments[0].clone(),
                        state,
                    )
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let decimal_size = DecimalSize {
                precision: s.precision,
                scale: s.scale,
            };
            let data_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
            let return_type = if params.len() > 1 {
                DataType::Array(Box::new(data_type))
            } else {
                data_type
            };
            let state = QuantileStateV2::<DecimalType<i256>>::new(params.clone())?;
            AggregateUnaryFunction::<QuantileStateV2<DecimalType<i256>>, DecimalType<i256>>::try_create(
                        display_name,
                        return_type,
                        params,
                        arguments[0].clone(),
                        state,
                    )
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, data_type
        ))),
    })
}
