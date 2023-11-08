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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_number;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use crate::aggregates::aggregate_unary::UnaryState;
use crate::aggregates::StateAddr;
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
    T::Scalar: Serialize + DeserializeOwned + Sync + Send,
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

    #[inline]
    fn add(&mut self, other: T::ScalarRef<'_>) {
        self.value.push(T::to_owned_scalar(other));
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
                self.add(data.clone());
            }
        } else {
            self.value
                .extend(column_iter.map(|data| T::to_owned_scalar(data)));
        }
        Ok(())
    }
}

impl<T> UnaryState<T> for QuantileStateV2<T>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned + Sync + Send + Ord,
{
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

    fn accumulate(
        &mut self,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        self.add_batch(&column, validity)
    }

    fn accumulate_row(&mut self, columns: &[Column], row: usize) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let v = T::index_column(&column, row);
        if let Some(v) = v {
            self.add(v)
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let column_iter = T::iter_column(&column);
        column_iter.zip(places.iter()).for_each(|(v, place)| {
            let addr = place.next(offset);
            let state = addr.get::<Self>();
            state.add(v.clone())
        });
        Ok(())
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, self)
    }

    fn deserialize(&self, reader: &mut &[u8]) -> Result<Self>
    where Self: Sized {
        deserialize_state::<Self>(reader)
    }
}
