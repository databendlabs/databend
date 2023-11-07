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
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::with_number_mapped_type;
use common_expression::AggregateFunction;
use common_expression::AggregateFunctionRef;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_expression::StateAddr;

use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::SkewnessStateV2;

pub trait UnaryState<T>: Send + Sync + Clone {
    fn merge(&mut self, rhs: &Self);
    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()>;

    fn accumulate(
        &mut self,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()>;
    fn accumulate_row(&mut self, columns: &[Column], row: usize) -> Result<()>;
    fn accumulate_keys(
        &mut self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        input_rows: usize,
    ) -> Result<()>;

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()>;

    fn deserialize(&self, reader: &mut &[u8]) -> Result<Self>
    where Self: Sized;
}

pub struct AggregateUnaryFunction<S, T>
where S: UnaryState<T>
{
    display_name: String,
    state: S,
    _arguments: Vec<DataType>,
    return_type: DataType,
    _phantom: PhantomData<T>,
}

impl<S, T> Display for AggregateUnaryFunction<S, T>
where S: UnaryState<T>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<S, T> AggregateUnaryFunction<S, T>
where
    S: UnaryState<T> + 'static,
    T: Send + Sync + 'static,
{
    fn try_create(
        display_name: &str,
        return_type: DataType,
        _params: Vec<Scalar>,
        _arguments: Vec<DataType>,
        state: S,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateUnaryFunction {
            display_name: display_name.to_string(),
            return_type,
            _arguments,
            state,
            _phantom: Default::default(),
        };

        Ok(Arc::new(func))
    }
}

impl<S, T> AggregateFunction for AggregateUnaryFunction<S, T>
where
    S: UnaryState<T>,
    T: Send + Sync,
{
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write_state(self.state.clone())
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<S>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let state: &mut S = place.get::<S>();
        state.accumulate(columns, validity, input_rows)
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let state: &mut S = place.get::<S>();
        state.accumulate_row(columns, row)
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state: &mut S = place.get::<S>();
        state.serialize(place, writer)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state: &mut S = place.get::<S>();
        let rhs = self.state.deserialize(reader)?;
        state.merge(&rhs);
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state: &mut S = place.get::<S>();
        let other: &mut S = rhs.get::<S>();
        state.merge(other);
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state: &mut S = place.get::<S>();
        state.merge_result(builder)
    }
}

pub fn try_create_aggregate_unary_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    match display_name {
        "skewness_v2" => {
            with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
                DataType::Number(NumberDataType::NUM_TYPE) => {
                    let return_type =
                        DataType::Nullable(Box::new(DataType::Number(NumberDataType::Float64)));
                    let state = SkewnessStateV2::default();
                    AggregateUnaryFunction::<SkewnessStateV2, NUM_TYPE>::try_create(
                        display_name,
                        return_type,
                        params,
                        arguments,
                        state,
                    )
                }

                _ => Err(ErrorCode::BadDataValueType(format!(
                    "{} does not support type '{:?}'",
                    display_name, arguments[0]
                ))),
            })
        }
        _ => Err(ErrorCode::UnknownAggregateFunction(format!(
            "{} aggregate function not exists",
            display_name
        ))),
    }
}

pub fn aggregate_unary_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_unary_function))
}
