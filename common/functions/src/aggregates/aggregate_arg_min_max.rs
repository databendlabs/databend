// Copyright 2021 Datafuse Labs.
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
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_datavalues::with_match_scalar_type;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use serde::de::DeserializeOwned;
use serde::Deserialize;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::ChangeIf;
use super::aggregate_scalar_state::CmpMax;
use super::aggregate_scalar_state::CmpMin;
use super::AggregateFunctionRef;
use super::StateAddr;
use crate::aggregates::assert_binary_arguments;
use crate::aggregates::AggregateFunction;

pub trait AggregateArgMinMaxState<S: Scalar>: Send + Sync + 'static {
    fn new() -> Self;
    fn add(&mut self, value: S::RefType<'_>, data: DataValue) -> Result<()>;
    fn add_batch(
        &mut self,
        data_column: &ColumnRef,
        column: &ColumnRef,
        validity: Option<&Bitmap>,
    ) -> Result<()>;

    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn serialize(&self, writer: &mut BytesMut) -> Result<()>;
    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()>;
    fn merge_result(&mut self, column: &mut dyn MutableColumn) -> Result<()>;
}

#[derive(serde::Serialize, Deserialize)]
struct ArgMinMaxState<S: Scalar, C> {
    #[serde(bound(deserialize = "S: DeserializeOwned"))]
    pub value: Option<S>,
    pub data: DataValue,
    #[serde(skip)]
    _c: PhantomData<C>,
}

impl<S, C> AggregateArgMinMaxState<S> for ArgMinMaxState<S, C>
where
    S: Scalar + Send + Sync + serde::Serialize + DeserializeOwned,
    C: ChangeIf<S> + Default,
{
    fn new() -> Self {
        Self {
            value: None,
            data: DataValue::Null,
            _c: PhantomData,
        }
    }

    fn add(&mut self, other: S::RefType<'_>, data: DataValue) -> Result<()> {
        match &self.value {
            Some(a) => {
                if C::change_if(a.as_scalar_ref(), other) {
                    self.value = Some(other.to_owned_scalar());
                    self.data = data;
                }
            }
            _ => {
                self.value = Some(other.to_owned_scalar());
                self.data = data;
            }
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        data_column: &ColumnRef,
        column: &ColumnRef,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        let col: &<S as Scalar>::ColumnType = unsafe { Series::static_cast(column) };

        if let Some(bit) = validity {
            if bit.null_count() == column.len() {
                return Ok(());
            }
            let mut v = S::default();
            let mut data_value = DataValue::Null;
            let mut has_v = false;
            let viewer = S::try_create_viewer(column)?;

            for (row, data) in viewer.iter().enumerate() {
                if viewer.null_at(row) {
                    continue;
                }
                if !has_v {
                    has_v = true;
                    v = data.to_owned_scalar();
                    data_value = data_column.get(row);
                } else if C::change_if(v.as_scalar_ref(), data) {
                    v = data.to_owned_scalar();
                    data_value = data_column.get(row);
                }
            }

            if has_v {
                self.add(v.as_scalar_ref(), data_value)?;
            }
        } else {
            let v = col.scalar_iter().enumerate().reduce(|acc, (idx, val)| {
                if C::change_if(acc.1, val) {
                    (idx, val)
                } else {
                    acc
                }
            });

            if let Some((idx, val)) = v {
                self.add(val, data_column.get(idx))?;
            }
        };
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(value) = &rhs.value {
            self.add(value.as_scalar_ref(), rhs.data.clone())?;
        }
        Ok(())
    }

    fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        serialize_into_buf(writer, self)
    }

    fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        *self = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn merge_result(&mut self, column: &mut dyn MutableColumn) -> Result<()> {
        // TODO:
        // Currently,this is all virtual call.
        // data can be dispatched into scalars to improve the performance.
        match self.value {
            Some(_) => column.append_data_value(self.data.clone())?,
            None => column.append_default(),
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateArgMinMaxFunction<S, C, State> {
    display_name: String,
    arguments: Vec<DataField>,
    _s: PhantomData<S>,
    _c: PhantomData<C>,
    _state: PhantomData<State>,
}

impl<S, C, State> AggregateFunction for AggregateArgMinMaxFunction<S, C, State>
where
    S: Scalar + Send + Sync + serde::Serialize + DeserializeOwned,
    C: ChangeIf<S> + Default,
    State: AggregateArgMinMaxState<S>,
{
    fn name(&self) -> &str {
        "AggregateArgMinMaxFunction"
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        Ok(self.arguments[0].data_type().clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| State::new());
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<State>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state: &mut State = place.get();
        state.add_batch(&columns[0], &columns[1], validity)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let col: &<S as Scalar>::ColumnType = unsafe { Series::static_cast(&columns[1]) };

        col.scalar_iter()
            .enumerate()
            .zip(places.iter())
            .try_for_each(|((row, item), place)| {
                let addr = place.next(offset);
                let state = addr.get::<State>();
                state.add(item, columns[0].get(row))
            })
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        let col: &<S as Scalar>::ColumnType = unsafe { Series::static_cast(&columns[1]) };
        let state = place.get::<State>();
        state.add(col.get_data(row), columns[0].get(row))
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
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

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(array)
    }
}

impl<S, C, State> fmt::Display for AggregateArgMinMaxFunction<S, C, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<S, C, State> AggregateArgMinMaxFunction<S, C, State>
where
    S: Scalar + Send + Sync + serde::Serialize + DeserializeOwned,
    C: ChangeIf<S> + Default,
    State: AggregateArgMinMaxState<S>,
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(AggregateArgMinMaxFunction::<S, C, State> {
            display_name: display_name.to_owned(),
            arguments,
            _s: PhantomData,
            _c: PhantomData,
            _state: PhantomData,
        }))
    }
}

pub fn try_create_aggregate_arg_minmax_function<const IS_MIN: bool>(
    display_name: &str,
    _params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_binary_arguments(display_name, arguments.len())?;
    let data_type = arguments[1].data_type();

    with_match_scalar_type!(data_type.data_type_id().to_physical_type(), |$T| {
        if IS_MIN {
            type State = ArgMinMaxState<$T, CmpMin>;
             AggregateArgMinMaxFunction::<$T, CmpMin, State>::try_create(
                display_name,
                arguments,
            )
        } else {
            type State = ArgMinMaxState<$T, CmpMax>;
             AggregateArgMinMaxFunction::<$T, CmpMax, State>::try_create(
                display_name,
                arguments,
            )
        }

    },

    {
        Err(ErrorCode::BadDataValueType(format!(
            "AggregateArgMinMaxFunction does not support type '{:?}'",
            data_type
        )))
    })
}

pub fn aggregate_arg_min_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_arg_minmax_function::<true>,
    ))
}

pub fn aggregate_arg_max_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_arg_minmax_function::<false>,
    ))
}
