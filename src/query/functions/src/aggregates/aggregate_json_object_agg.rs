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
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::string::StringColumn;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::*;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use jiff::tz::TimeZone;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_function_factory::AggregateFunctionSortDesc;
use super::borsh_deserialize_state;
use super::borsh_serialize_state;
use super::StateAddr;
use crate::aggregates::assert_binary_arguments;
use crate::aggregates::AggrState;
use crate::aggregates::AggrStateLoc;
use crate::aggregates::AggregateFunction;

pub trait BinaryScalarStateFunc<V: ValueType>:
    BorshSerialize + BorshDeserialize + Send + Sync + 'static
{
    fn new() -> Self;
    fn mem_size() -> Option<usize> {
        None
    }
    fn add(&mut self, other: Option<(&str, V::ScalarRef<'_>)>) -> Result<()>;
    fn add_batch(
        &mut self,
        key_column: &StringColumn,
        val_column: &V::Column,
        validity: Option<&Bitmap>,
    ) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()>;
}

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct JsonObjectAggState<V>
where
    V: ValueType,
    V::Scalar: BorshSerialize + BorshDeserialize,
{
    kvs: BTreeMap<String, V::Scalar>,
}

impl<V> Default for JsonObjectAggState<V>
where
    V: ValueType,
    V::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self {
            kvs: BTreeMap::new(),
        }
    }
}

impl<V> BinaryScalarStateFunc<V> for JsonObjectAggState<V>
where
    V: ValueType,
    V::Scalar: BorshSerialize + BorshDeserialize + Send + Sync,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<(&str, V::ScalarRef<'_>)>) -> Result<()> {
        if let Some(other) = other {
            if self.kvs.contains_key(other.0) {
                return Err(ErrorCode::BadArguments(format!(
                    "Json object have duplicate key '{}'",
                    other.0
                )));
            }
            let k = other.0.to_string();
            let v = V::to_owned_scalar(other.1);
            self.kvs.insert(k, v);
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        key_column: &StringColumn,
        val_column: &V::Column,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        let key_column_len = StringType::column_len(key_column);
        let val_column_len = V::column_len(val_column);
        if key_column_len != val_column_len {
            return Err(ErrorCode::Internal("Invalid column".to_string()));
        }
        if key_column_len == 0 {
            return Ok(());
        }
        let key_column_iter = StringType::iter_column(key_column);
        let val_column_iter = V::iter_column(val_column);
        if let Some(validity) = validity {
            for (key, (val, valid)) in key_column_iter.zip(val_column_iter.zip(validity.iter())) {
                if valid {
                    if self.kvs.contains_key(key) {
                        return Err(ErrorCode::BadArguments(format!(
                            "Json object have duplicate key '{}'",
                            key
                        )));
                    }
                    let k = StringType::to_owned_scalar(key);
                    let v = V::to_owned_scalar(val);
                    self.kvs.insert(k, v);
                }
            }
        } else {
            for (key, val) in key_column_iter.zip(val_column_iter) {
                if self.kvs.contains_key(key) {
                    return Err(ErrorCode::BadArguments(format!(
                        "Json object have duplicate key '{}'",
                        key
                    )));
                }
                let k = StringType::to_owned_scalar(key);
                let v = V::to_owned_scalar(val);
                self.kvs.insert(k, v);
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        for (k, v) in rhs.kvs.iter() {
            if self.kvs.contains_key(k) {
                return Err(ErrorCode::BadArguments(format!(
                    "Json object have duplicate key '{}'",
                    k
                )));
            }
            self.kvs.insert(k.clone(), v.clone());
        }
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let tz = TimeZone::UTC;
        let mut values = Vec::with_capacity(self.kvs.len());
        let kvs = mem::take(&mut self.kvs);
        for (key, value) in kvs.into_iter() {
            let v = V::upcast_scalar(value);
            // NULL values are omitted from the output.
            if v == Scalar::Null {
                continue;
            }
            let mut val = vec![];
            cast_scalar_to_variant(v.as_ref(), &tz, &mut val);
            values.push((key, val));
        }
        let mut data = vec![];
        jsonb::build_object(values.iter().map(|(k, v)| (k, &v[..])), &mut data).unwrap();

        let object_value = Scalar::Variant(data);
        builder.push(object_value.as_ref());
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateJsonObjectAggFunction<V, State> {
    display_name: String,
    return_type: DataType,
    _v: PhantomData<V>,
    _state: PhantomData<State>,
}

impl<V, State> AggregateFunction for AggregateJsonObjectAggFunction<V, State>
where
    V: ValueType + Send + Sync,
    State: BinaryScalarStateFunc<V>,
{
    fn name(&self) -> &str {
        "AggregateJsonObjectAggFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: AggrState) {
        place.write(State::new);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<State>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: InputColumns,
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<State>();
        let (key_column, val_column, validity) = self.downcast_columns(columns)?;
        state.add_batch(&key_column, &val_column, validity.as_ref())?;

        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        let (key_column, val_column, validity) = self.downcast_columns(columns)?;
        let key_column_iter = StringType::iter_column(&key_column);
        let val_column_iter = V::iter_column(&val_column);

        if let Some(validity) = validity {
            for (k, (v, (valid, place))) in
                key_column_iter.zip(val_column_iter.zip(validity.iter().zip(places.iter())))
            {
                let state = AggrState::new(*place, loc).get::<State>();
                if valid {
                    state.add(Some((k, v.clone())))?;
                } else {
                    state.add(None)?;
                }
            }
        } else {
            for (k, (v, place)) in key_column_iter.zip(val_column_iter.zip(places.iter())) {
                let state = AggrState::new(*place, loc).get::<State>();
                state.add(Some((k, v.clone())))?;
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        let state = place.get::<State>();
        let (key_column, val_column, validity) = self.downcast_columns(columns)?;

        let valid = if let Some(validity) = validity {
            validity.get_bit(row)
        } else {
            true
        };
        if valid {
            let key = StringType::index_column(&key_column, row).unwrap();
            let val = V::index_column(&val_column, row).unwrap();
            state.add(Some((key, val)))?;
        } else {
            state.add(None)?;
        }

        Ok(())
    }

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<State>();
        borsh_serialize_state(writer, state)
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        let rhs: State = borsh_deserialize_state(reader)?;

        state.merge(&rhs)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<State>();
        let other = rhs.get::<State>();
        state.merge(other)
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}

impl<V, State> fmt::Display for AggregateJsonObjectAggFunction<V, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<V, State> AggregateJsonObjectAggFunction<V, State>
where
    V: ValueType + Send + Sync,
    State: BinaryScalarStateFunc<V>,
{
    fn try_create(display_name: &str, return_type: DataType) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateJsonObjectAggFunction::<V, State> {
            display_name: display_name.to_string(),
            return_type,
            _v: PhantomData,
            _state: PhantomData,
        };
        Ok(Arc::new(func))
    }

    fn downcast_columns(
        &self,
        columns: InputColumns,
    ) -> Result<(StringColumn, V::Column, Option<Bitmap>)> {
        let (key_column, key_validity) = match &columns[0] {
            Column::Nullable(box nullable_column) => {
                let column = StringType::try_downcast_column(&nullable_column.column).unwrap();
                (column, Some(nullable_column.validity.clone()))
            }
            _ => {
                let column = StringType::try_downcast_column(&columns[0]).unwrap();
                (column, None)
            }
        };
        let (val_column, val_validity) = match &columns[1] {
            Column::Nullable(box nullable_column) => {
                let column = V::try_downcast_column(&nullable_column.column).unwrap();
                (column, Some(nullable_column.validity.clone()))
            }
            _ => {
                let column = V::try_downcast_column(&columns[1]).unwrap();
                (column, None)
            }
        };
        let validity = match (key_validity, val_validity) {
            (Some(key_validity), Some(val_validity)) => {
                let and_validity = boolean::and(&key_validity, &val_validity);
                Some(and_validity)
            }
            (Some(key_validity), None) => Some(key_validity.clone()),
            (None, Some(val_validity)) => Some(val_validity.clone()),
            (None, None) => None,
        };

        Ok((key_column, val_column, validity))
    }
}

pub fn try_create_aggregate_json_object_agg_function(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_binary_arguments(display_name, argument_types.len())?;

    let key_type = argument_types[0].remove_nullable();
    if key_type != DataType::String {
        return Err(ErrorCode::BadDataValueType(format!(
            "{} does not support key type '{:?}'",
            display_name, argument_types[0]
        )));
    }
    let return_type = DataType::Variant;

    type State = JsonObjectAggState<AnyType>;
    AggregateJsonObjectAggFunction::<AnyType, State>::try_create(display_name, return_type)
}

pub fn aggregate_json_object_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_json_object_agg_function))
}
