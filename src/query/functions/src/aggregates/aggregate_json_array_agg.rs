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
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_common_expression::types::*;
use jiff::tz::TimeZone;
use jsonb::OwnedJsonb;
use jsonb::RawJsonb;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionFeatures;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use super::StateSerde;
use super::aggregate_scalar_state::ScalarStateFunc;
use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge1;

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    values: Vec<T::Scalar>,
}

impl<T> Default for JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> ScalarStateFunc<T> for JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        if let Some(other) = other {
            self.values.push(T::to_owned_scalar(other));
        }
    }

    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()> {
        let column_len = column.len();
        if column_len == 0 {
            return Ok(());
        }
        if let Some(validity) = validity {
            for (val, valid) in column.iter().zip(validity.iter()) {
                if valid {
                    self.values.push(T::to_owned_scalar(val));
                }
            }
        } else {
            for val in column.iter() {
                self.values.push(T::to_owned_scalar(val));
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.extend_from_slice(&rhs.values);
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let tz = TimeZone::UTC;
        let mut items = Vec::with_capacity(self.values.len());
        let values = mem::take(&mut self.values);
        let data_type = builder.data_type();
        for value in values.into_iter() {
            let v = T::upcast_scalar_with_type(value, &data_type);
            // NULL values are omitted from the output.
            if v == Scalar::Null {
                continue;
            }
            let mut val = vec![];
            cast_scalar_to_variant(v.as_ref(), &tz, &mut val, None);
            items.push(val);
        }
        let owned_jsonb = OwnedJsonb::build_array(items.iter().map(|v| RawJsonb::new(v)))
            .map_err(|e| ErrorCode::Internal(format!("failed to build array error: {:?}", e)))?;
        let array_value = ScalarRef::Variant(owned_jsonb.as_ref());
        builder.push(array_value);
        Ok(())
    }
}

impl<T> StateSerde for JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn serialize_type(_: Option<&dyn super::SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state = AggrState::new(*place, loc).get::<Self>();
            state.serialize(&mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<BinaryType, Self, _>(places, loc, state, filter, |state, mut data| {
            let rhs = Self::deserialize_reader(&mut data)?;
            state.values.extend_from_slice(&rhs.values);
            Ok(())
        })
    }
}

#[derive(Clone)]
struct AggregateJsonArrayAggFunction<T> {
    display_name: String,
    return_type: DataType,
    _t: PhantomData<fn(T)>,
}

impl<T> AggregateFunction for AggregateJsonArrayAggFunction<T>
where
    T: ValueType,
    T::Scalar: borsh::BorshSerialize + borsh::BorshDeserialize,
{
    fn name(&self) -> &str {
        "AggregateJsonArrayAggFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: AggrState) {
        place.write(JsonArrayAggState::<T>::new);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<JsonArrayAggState<T>>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<JsonArrayAggState<T>>();
        match &columns[0] {
            BlockEntry::Const(Scalar::Null, _, _) => Ok(()),
            entry @ BlockEntry::Const(_, _, _) => {
                let column = entry.clone().remove_nullable().downcast::<T>().unwrap();
                state.add_batch(column, None)
            }
            BlockEntry::Column(Column::Nullable(box nullable_column)) => {
                let column = T::try_downcast_column(&nullable_column.column).unwrap();
                state.add_batch(ColumnView::Column(column), Some(&nullable_column.validity))
            }
            entry => {
                let c = entry.downcast::<T>().unwrap();
                state.add_batch(c, None)
            }
        }
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        block: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let entry = &block[0];
        if entry.data_type().is_nullable() {
            entry
                .downcast::<NullableType<T>>()
                .unwrap()
                .iter()
                .zip(places.iter())
                .for_each(|(v, place)| {
                    let state = AggrState::new(*place, loc).get::<JsonArrayAggState<T>>();
                    state.add(v)
                });
        } else {
            entry
                .downcast::<T>()
                .unwrap()
                .iter()
                .zip(places.iter())
                .for_each(|(v, place)| {
                    let state = AggrState::new(*place, loc).get::<JsonArrayAggState<T>>();
                    state.add(Some(v))
                });
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, block: ProjectedBlock, row: usize) -> Result<()> {
        let state = place.get::<JsonArrayAggState<T>>();
        let entry = &block[0];
        if entry.data_type().is_nullable() {
            let view = entry.downcast::<NullableType<T>>().unwrap();
            let v = view.index(row).unwrap();
            state.add(v);
        } else {
            let view = entry.downcast::<T>().unwrap();
            let v = view.index(row).unwrap();
            state.add(Some(v));
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        JsonArrayAggState::<T>::serialize_type(None)
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        JsonArrayAggState::<T>::batch_serialize(places, loc, builders)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        JsonArrayAggState::<T>::batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<JsonArrayAggState<T>>();
        let other = rhs.get::<JsonArrayAggState<T>>();
        state.merge(other)
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = place.get::<JsonArrayAggState<T>>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<JsonArrayAggState<T>>();
        std::ptr::drop_in_place(state);
    }
}

impl<T> fmt::Display for AggregateJsonArrayAggFunction<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateJsonArrayAggFunction<T>
where
    T: ValueType,
    T::Scalar: borsh::BorshSerialize + borsh::BorshDeserialize,
{
    fn try_create(display_name: &str, return_type: DataType) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateJsonArrayAggFunction::<T> {
            display_name: display_name.to_string(),
            return_type,
            _t: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

fn try_create_aggregate_json_array_agg_function(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, argument_types.len())?;
    let return_type = DataType::Variant;

    AggregateJsonArrayAggFunction::<AnyType>::try_create(display_name, return_type)
}

pub fn aggregate_json_array_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_json_array_agg_function),
        AggregateFunctionFeatures {
            keep_nullable: true,
            ..Default::default()
        },
    )
}
