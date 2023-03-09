// Copyright 2023 Datafuse Labs.
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
use common_expression::types::decimal::*;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_io::prelude::deserialize_from_slice;
use common_io::prelude::serialize_into_buf;
use ethnum::i256;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::assert_unary_params;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;
use crate::with_simple_no_number_mapped_type;

const MEDIAN: u8 = 0;
const QUANTILE: u8 = 1;

pub trait QuantileStateFunc<T: ValueType>: Send + Sync + 'static {
    fn new() -> Self;
    fn add(&mut self, other: T::ScalarRef<'_>);
    fn add_batch(&mut self, column: &T::Column, validity: Option<&Bitmap>) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_result(&mut self, builder: &mut ColumnBuilder, level: f64) -> Result<()>;
    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
}

#[derive(Serialize, Deserialize)]
pub struct QuantileState<T>
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

    fn merge_result(&mut self, builder: &mut ColumnBuilder, level: f64) -> Result<()> {
        let builder = T::try_downcast_builder(builder).unwrap();
        let value_len = self.value.len();
        let idx = ((value_len - 1) as f64 * level).floor() as usize;
        if idx >= value_len {
            T::push_default(builder);
        } else {
            self.value.as_mut_slice().select_nth_unstable(idx);
            let value = self.value.get(idx).unwrap();
            T::push_item(builder, T::to_scalar_ref(value));
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_into_buf(writer, self)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.value = deserialize_from_slice(reader)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateQuantileContFunction<T, State> {
    display_name: String,
    return_type: DataType,
    level: f64,
    _arguments: Vec<DataType>,
    _t: PhantomData<T>,
    _state: PhantomData<State>,
}

impl<T, State> Display for AggregateQuantileContFunction<T, State>
where
    State: QuantileStateFunc<T>,
    T: Send + Sync + ValueType,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, State> AggregateFunction for AggregateQuantileContFunction<T, State>
where
    T: ValueType + Send + Sync,
    State: QuantileStateFunc<T>,
{
    fn name(&self) -> &str {
        "AggregateQuantileContFunction"
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

        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let rhs = rhs.get::<State>();
        let state = place.get::<State>();
        state.merge(rhs)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder, self.level)
    }
}

impl<T, State> AggregateQuantileContFunction<T, State>
where
    State: QuantileStateFunc<T>,
    T: Send + Sync + ValueType,
{
    fn try_create(
        display_name: &str,
        return_type: DataType,
        level: f64,
        arguments: Vec<DataType>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateQuantileContFunction::<T, State> {
            display_name: display_name.to_string(),
            return_type,
            level,
            _arguments: arguments,
            _t: PhantomData,
            _state: PhantomData,
        };

        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_quantile_function<const TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    let level = if TYPE == MEDIAN {
        0.5f64
    } else {
        assert_unary_params(display_name, params.len())?;
        let param = params[0].clone();
        match param {
            Scalar::Decimal(d) => {
                let f = d.to_float64();
                if f <= 0.01 || f >= 0.99 {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "level range between 0.01 to 0.99, got: {:?}",
                        f
                    )));
                }
                f
            }
            Scalar::Number(NumberScalar::UInt64(i)) => {
                if i == 0 {
                    0.01f64
                } else if i == 1 {
                    0.99f64
                } else {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "level range between 0.01 to 0.99, got: {:?}",
                        i
                    )));
                }
            }
            _ => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "level param just support float type, got: {:?}",
                    param
                )));
            }
        }
    };

    let data_type = arguments[0].clone();
    with_simple_no_number_mapped_type!(|T| match data_type {
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    type State = QuantileState<NumberType<NUM>>;
                    AggregateQuantileContFunction::<NumberType<NUM>, State>::try_create(
                        display_name,
                        data_type,
                        level,
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
            type State = QuantileState<DecimalType<i128>>;
            AggregateQuantileContFunction::<DecimalType<i128>, State>::try_create(
                display_name,
                DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                level,
                arguments,
            )
        }
        DataType::Decimal(DecimalDataType::Decimal256(s)) => {
            let decimal_size = DecimalSize {
                precision: s.precision,
                scale: s.scale,
            };
            type State = QuantileState<DecimalType<i256>>;
            AggregateQuantileContFunction::<DecimalType<i256>, State>::try_create(
                display_name,
                DataType::Decimal(DecimalDataType::from_size(decimal_size)?),
                level,
                arguments,
            )
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
            display_name, data_type
        ))),
    })
}

pub fn aggregate_quantile_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_function::<QUANTILE>,
    ))
}

pub fn aggregate_median_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_function::<MEDIAN>,
    ))
}
