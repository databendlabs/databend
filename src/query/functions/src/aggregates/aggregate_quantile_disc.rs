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
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_number;
use common_expression::types::decimal::*;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::Column;
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
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;
use crate::with_simple_no_number_mapped_type;
use crate::BUILTIN_FUNCTIONS;

pub trait QuantileStateFunc<T: ValueType>:
    Serialize + DeserializeOwned + Send + Sync + 'static
{
    fn new() -> Self;
    fn add(&mut self, other: T::ScalarRef<'_>);
    fn add_batch(&mut self, column: &T::Column, validity: Option<&Bitmap>) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_result(&mut self, builder: &mut ColumnBuilder, levels: Vec<f64>) -> Result<()>;
}
#[derive(Serialize, Deserialize)]
struct QuantileState<T>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned,
{
    #[serde(bound(deserialize = "T::Scalar: DeserializeOwned"))]
    pub value: Vec<T::Scalar>,
}
impl<T> Default for QuantileState<T>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned,
{
    fn default() -> Self {
        Self { value: vec![] }
    }
}
impl<T> QuantileStateFunc<T> for QuantileState<T>
where
    T: ValueType,
    T::Scalar: Serialize + DeserializeOwned + Send + Sync + Ord,
{
    fn new() -> Self {
        Self::default()
    }
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
    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.value.extend(
            rhs.value
                .iter()
                .map(|v| T::to_owned_scalar(T::to_scalar_ref(v))),
        );
        Ok(())
    }
    fn merge_result(&mut self, builder: &mut ColumnBuilder, levels: Vec<f64>) -> Result<()> {
        let value_len = self.value.len();
        if levels.len() > 1 {
            let builder = match builder {
                ColumnBuilder::Array(box b) => b,
                _ => unreachable!(),
            };
            let indices = levels
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
            let idx = ((value_len - 1) as f64 * levels[0]).floor() as usize;
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
}
#[derive(Clone)]
pub struct AggregateQuantileDiscFunction<T, State> {
    display_name: String,
    return_type: DataType,
    levels: Vec<f64>,
    _arguments: Vec<DataType>,
    _t: PhantomData<T>,
    _state: PhantomData<State>,
}
impl<T, State> Display for AggregateQuantileDiscFunction<T, State>
where
    State: QuantileStateFunc<T>,
    T: Send + Sync + ValueType,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
impl<T, State> AggregateFunction for AggregateQuantileDiscFunction<T, State>
where
    T: ValueType + Send + Sync,
    State: QuantileStateFunc<T>,
{
    fn name(&self) -> &str {
        "AggregateQuantileDiscFunction"
    }
    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn init_state(&self, place: StateAddr) {
        place.write(|| State::new())
    }
    fn state_layout(&self) -> Layout {
        Layout::new::<State>()
    }
    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<State>();
        state.add_batch(&column, validity)
    }
    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let v = T::index_column(&column, row);
        if let Some(v) = v {
            let state = place.get::<State>();
            state.add(v)
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
            let state = addr.get::<State>();
            state.add(v.clone())
        });
        Ok(())
    }
    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<State>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        let rhs: State = deserialize_state(reader)?;
        state.merge(&rhs)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<State>();
        let other = rhs.get::<State>();
        state.merge(other)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder, self.levels.clone())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}
impl<T, State> AggregateQuantileDiscFunction<T, State>
where
    State: QuantileStateFunc<T>,
    T: Send + Sync + ValueType,
{
    fn try_create(
        display_name: &str,
        return_type: DataType,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<Arc<dyn AggregateFunction>> {
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
        let func = AggregateQuantileDiscFunction::<T, State> {
            display_name: display_name.to_string(),
            return_type,
            levels,
            _arguments: arguments,
            _t: PhantomData,
            _state: PhantomData,
        };
        Ok(Arc::new(func))
    }
}
pub fn try_create_aggregate_quantile_disc_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;
    let data_type = arguments[0].clone();
    with_simple_no_number_mapped_type!(|T| match data_type {
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    type State = QuantileState<NumberType<NUM>>;
                    let return_type = if params.len() > 1 {
                        DataType::Array(Box::new(data_type))
                    } else {
                        data_type
                    };
                    AggregateQuantileDiscFunction::<NumberType<NUM>, State>::try_create(
                        display_name,
                        return_type,
                        params,
                        arguments,
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
            type State = QuantileState<DecimalType<i128>>;
            AggregateQuantileDiscFunction::<DecimalType<i128>, State>::try_create(
                display_name,
                return_type,
                params,
                arguments,
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
            type State = QuantileState<DecimalType<i256>>;
            AggregateQuantileDiscFunction::<DecimalType<i256>, State>::try_create(
                display_name,
                return_type,
                params,
                arguments,
            )
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, data_type
        ))),
    })
}
pub fn aggregate_quantile_disc_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_quantile_disc_function))
}
