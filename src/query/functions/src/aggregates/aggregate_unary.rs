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
use common_expression::types::ValueType;
use common_expression::AggregateFunction;
use common_expression::AggregateFunctionRef;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_expression::StateAddr;

use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::aggregate_skewness_v2::create_skewness;
use crate::aggregates::assert_unary_arguments;

pub trait UnaryState<T>: Send + Sync + Default
where T: ValueType
{
    fn add(&mut self, other: T::ScalarRef<'_>);

    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_result(&mut self) -> Result<Option<Scalar>>;

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()>;

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where Self: Sized;

    fn need_manual_drop_state() -> bool {
        false
    }
}

pub struct AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T>,
    T: ValueType,
    R: ValueType,
{
    display_name: String,
    _params: Vec<Scalar>,
    _argument: DataType,
    return_type: DataType,
    _phantom: PhantomData<(S, T, R)>,
}

impl<S, T, R> Display for AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T>,
    T: ValueType,
    R: ValueType,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<S, T, R> AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T> + 'static,
    T: Send + Sync + ValueType + 'static,
    R: Send + Sync + ValueType + 'static,
{
    pub(crate) fn try_create(
        display_name: &str,
        return_type: DataType,
        _params: Vec<Scalar>,
        _argument: DataType,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let func: AggregateUnaryFunction<S, T, R> = AggregateUnaryFunction {
            display_name: display_name.to_string(),
            return_type,
            _params,
            _argument,
            _phantom: Default::default(),
        };

        Ok(Arc::new(func))
    }
}

impl<S, T, R> AggregateFunction for AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T> + 'static,
    T: Send + Sync + ValueType + 'static,
    R: Send + Sync + ValueType + 'static,
{
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write_state(S::default())
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<S>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let column_iter = T::iter_column(&column);
        let state: &mut S = place.get::<S>();
        match validity {
            Some(bitmap) => {
                for (value, is_valid) in column_iter.zip(bitmap.iter()) {
                    if is_valid {
                        state.add(value);
                    }
                }
            }
            None => {
                for value in column_iter {
                    state.add(value);
                }
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let value = T::index_column(&column, row);

        let state: &mut S = place.get::<S>();
        state.add(value.unwrap());
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
            let state: &mut S = addr.get::<S>();
            state.add(v);
        });
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state: &mut S = place.get::<S>();
        state.serialize(writer)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state: &mut S = place.get::<S>();
        let rhs = S::deserialize(reader)?;
        state.merge(&rhs)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state: &mut S = place.get::<S>();
        let other: &mut S = rhs.get::<S>();
        state.merge(other)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state: &mut S = place.get::<S>();
        let value = state.merge_result()?;

        if value.is_some() {
            if let Some(inner) = R::try_downcast_builder(builder) {
                R::push_item(
                    inner,
                    R::try_downcast_scalar(&value.unwrap().as_ref()).unwrap(),
                );
            } else {
                builder.push(value.unwrap().as_ref());
            }
        } else if let Some(inner) = R::try_downcast_builder(builder) {
            R::push_default(inner);
        } else {
            builder.push_default();
        }

        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        S::need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<S>();
        std::ptr::drop_in_place(state);
    }
}

pub fn try_create_aggregate_unary_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<AggregateFunctionRef> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].clone();

    match display_name {
        "skewness_v2" => create_skewness(display_name, params, &arguments, &data_type),
        _ => Err(ErrorCode::UnknownAggregateFunction(format!(
            "{} aggregate function not exists",
            display_name
        ))),
    }
}

pub fn aggregate_unary_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_unary_function))
}
