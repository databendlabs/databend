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

use bytes::BytesMut;
use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_datavalues::with_match_scalar_types_error;
use common_datavalues::MutableColumn;
use common_datavalues::Scalar;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::de::DeserializeOwned;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::ChangeIf;
use super::aggregate_scalar_state::CmpMax;
use super::aggregate_scalar_state::CmpMin;
use super::aggregate_scalar_state::ScalarState;
use super::aggregate_scalar_state::ScalarStateFunc;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;

/// S: ScalarType
/// A: Aggregate State
#[derive(Clone)]
pub struct AggregateMinMaxFunction<S, C, State> {
    display_name: String,
    arguments: Vec<DataField>,
    _s: PhantomData<S>,
    _c: PhantomData<C>,
    _state: PhantomData<State>,
}

impl<S, C, State> AggregateFunction for AggregateMinMaxFunction<S, C, State>
where
    S: Scalar + Send + Sync + serde::Serialize + DeserializeOwned,
    C: ChangeIf<S> + Default,
    State: ScalarStateFunc<S>,
{
    fn name(&self) -> &str {
        "AggregateMinMaxFunction"
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
        columns: &[common_datavalues::ColumnRef],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<State>();
        state.add_batch(&columns[0], validity)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[common_datavalues::ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let col: &<S as Scalar>::ColumnType = unsafe { Series::static_cast(&columns[0]) };

        col.scalar_iter()
            .zip(places.iter())
            .for_each(|(item, place)| {
                let addr = place.next(offset);
                let state = addr.get::<State>();
                state.add(item)
            });
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        let col: &<S as Scalar>::ColumnType = unsafe { Series::static_cast(&columns[0]) };

        let state = place.get::<State>();
        state.add(col.get_data(row));
        Ok(())
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
        state.merge_result(array)?;
        Ok(())
    }
}

impl<S, C, State> fmt::Display for AggregateMinMaxFunction<S, C, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<S, C, State> AggregateMinMaxFunction<S, C, State>
where
    S: Scalar + Send + Sync + serde::Serialize + DeserializeOwned,
    C: ChangeIf<S> + Default,
    State: ScalarStateFunc<S>,
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateMinMaxFunction::<S, C, State> {
            display_name: display_name.to_string(),
            arguments,
            _s: PhantomData,
            _c: PhantomData,
            _state: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_minmax_function<const IS_MIN: bool>(
    display_name: &str,
    _params: Vec<DataValue>,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;
    let data_type = arguments[0].data_type().clone();
    let phid = data_type.data_type_id().to_physical_type();
    let result = with_match_scalar_types_error!(phid, |$T| {
        if IS_MIN {
            type State = ScalarState<$T, CmpMin>;
            AggregateMinMaxFunction::<$T, CmpMin, State>::try_create(display_name, arguments)
        } else {
            type State = ScalarState<$T, CmpMax>;
            AggregateMinMaxFunction::<$T, CmpMax, State>::try_create(display_name, arguments)
        }
    });

    result.map_err(|_|  // no matching branch
       ErrorCode::BadDataValueType(format!(
            "AggregateMinMaxFunction does not support type '{:?}'",
            data_type
        )))
}

pub fn aggregate_min_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_minmax_function::<true>))
}

pub fn aggregate_max_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_minmax_function::<false>))
}
