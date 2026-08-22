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
use std::marker::PhantomData;
use std::mem;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use databend_common_expression::utils::column_merge_validity;
use jiff::tz::TimeZone;
use jsonb::OwnedJsonb;
use jsonb::RawJsonb;

use super::FunctionFactory;
use super::adaptors::*;

struct JsonObjectAggBuilder;

impl JsonObjectAggBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        Self::route().register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: JsonObjectAggBuilder::register,
    }
}

impl JsonObjectAggBuilder {
    fn json_object_agg_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![
            AggregateArgumentPattern::exact(DataType::String),
            AggregateArgumentPattern::any(),
        ])
    }

    const JSON_OBJECT_AGG_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "aggregates key-value pairs into a JSON object",
        definition: "json_object_agg(key, value)",
        example: "select json_object_agg(name, value) from t",
    };
}

trait BinaryScalarStateFunc<V: ValueType>:
    Clone + BorshSerialize + BorshDeserialize + Send + 'static
where V::Scalar: BorshSerialize + BorshDeserialize
{
    fn new() -> Self;
    fn add(&mut self, other: Option<(&str, V::ScalarRef<'_>)>) -> Result<()>;
    fn add_batch(
        &mut self,
        key_column: ColumnView<StringType>,
        val_column: ColumnView<V>,
        validity: Option<&Bitmap>,
    ) -> Result<()>;
    fn merge(&mut self, rhs: &Self) -> Result<()>;
    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()>;
    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()>;
}

#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
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
    V::Scalar: BorshSerialize + BorshDeserialize,
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
        key_column: ColumnView<StringType>,
        val_column: ColumnView<V>,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        let key_column_len = key_column.len();
        let val_column_len = val_column.len();
        if key_column_len != val_column_len {
            return Err(ErrorCode::Internal("Invalid column".to_string()));
        }
        if key_column_len == 0 {
            return Ok(());
        }

        let key_column_iter = key_column.iter();
        let val_column_iter = val_column.iter();
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

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        for k in rhs.kvs.keys() {
            if self.kvs.contains_key(k) {
                return Err(ErrorCode::BadArguments(format!(
                    "Json object have duplicate key '{}'",
                    k
                )));
            }
        }
        self.kvs.append(&mut rhs.kvs);
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let tz = TimeZone::UTC;
        let mut values = Vec::with_capacity(self.kvs.len());
        let kvs = mem::take(&mut self.kvs);
        let data_type = builder.data_type();
        for (key, value) in kvs.into_iter() {
            let v = V::upcast_scalar_with_type(value, &data_type);
            // NULL values are omitted from the output.
            if v == Scalar::Null {
                continue;
            }
            let mut val = vec![];
            cast_scalar_to_variant(v.as_ref(), &tz, &mut val, None);
            values.push((key, val));
        }
        let owned_jsonb =
            OwnedJsonb::build_object(values.iter().map(|(k, v)| (k, RawJsonb::new(&v[..]))))
                .map_err(|e| {
                    ErrorCode::Internal(format!("failed to build object error: {:?}", e))
                })?;
        let object_value = ScalarRef::Variant(owned_jsonb.as_ref());
        builder.push(object_value);
        Ok(())
    }
}

struct AggregateJsonObjectAggImplementation<V, State> {
    _p: PhantomData<fn(V, State)>,
}

impl<V, State> Default for AggregateJsonObjectAggImplementation<V, State> {
    fn default() -> Self {
        Self { _p: PhantomData }
    }
}

impl<V, State> AggregateJsonObjectAggImplementation<V, State>
where
    V: ValueType,
    V::Scalar: BorshSerialize + BorshDeserialize,
    State: BinaryScalarStateFunc<V>,
{
    fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<State>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
    }

    fn downcast_columns(
        columns: databend_common_expression::ProjectedBlock<'_>,
        validity: Option<&Bitmap>,
    ) -> Result<(ColumnView<StringType>, ColumnView<V>, Option<Bitmap>)> {
        let key_entry = &columns[0];
        let val_entry = &columns[1];
        let validity = column_merge_validity(key_entry, validity.cloned());
        let validity = column_merge_validity(val_entry, validity);
        let key_column = key_entry
            .clone()
            .remove_nullable()
            .downcast::<StringType>()
            .unwrap();
        let val_column = val_entry.clone().remove_nullable().downcast::<V>().unwrap();
        Ok((key_column, val_column, validity))
    }
}

impl<V, State> AggrImpl for AggregateJsonObjectAggImplementation<V, State>
where
    V: ValueType,
    V::Scalar: BorshSerialize + BorshDeserialize + Clone + Send + Sync,
    State: BinaryScalarStateFunc<V> + Sync,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(State::new);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let state = input.state.get::<State>();
        let (key_column, val_column, validity) =
            Self::downcast_columns(input.columns, input.validity)?;
        state.add_batch(key_column, val_column, validity.as_ref())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let (key_column, val_column, validity) = Self::downcast_columns(input.columns, None)?;
        let key_column_iter = key_column.iter();
        let val_column_iter = val_column.iter();

        if let Some(validity) = validity {
            for (k, (v, (valid, state))) in
                key_column_iter.zip(val_column_iter.zip(validity.iter().zip(input.states.iter())))
            {
                let state = state.get::<State>();
                if valid {
                    state.add(Some((k, v.clone())))?;
                } else {
                    state.add(None)?;
                }
            }
        } else {
            for (k, (v, state)) in key_column_iter.zip(val_column_iter.zip(input.states.iter())) {
                let state = state.get::<State>();
                state.add(Some((k, v.clone())))?;
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let state = input.state.get::<State>();
        let (key_column, val_column, validity) = Self::downcast_columns(input.columns, None)?;

        let valid = if let Some(validity) = validity {
            validity.get(input.row).unwrap()
        } else {
            true
        };
        if valid {
            let key = key_column.index(input.row).unwrap();
            let val = val_column.index(input.row).unwrap();
            state.add(Some((key, val)))?;
        } else {
            state.add(None)?;
        }

        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        let binary_builder = input.builders[0].as_binary_mut().unwrap();
        for state in input.states.iter() {
            let state = state.get::<State>();
            state.serialize(&mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let ScalarRef::Binary(mut data) = super::serialized_scalar_at(input.state, row, 0)
            else {
                unreachable!()
            };
            let rhs = State::deserialize_reader(&mut data)?;
            state.get::<State>().merge(&rhs)?;
        }
        Ok(())
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input
            .state
            .get::<State>()
            .merge_owned(input.rhs.get::<State>())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        let state = input.state.get::<State>();
        state.merge_result(input.builder)
    }

    fn merge_result_read_only(&self, input: MergeResultInput<'_>) -> Result<()> {
        let mut state = input.state.get::<State>().clone();
        state.merge_result(input.builder)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<State>()) };
    }
}

impl JsonObjectAggBuilder {
    fn route() -> DirectNameRoute {
        let arguments = Self::json_object_agg_arguments();
        let features = Self::JSON_OBJECT_AGG_FEATURES;
        DirectNameRoute::new(
            &["json_object_agg"],
            arguments.clone(),
            features.clone(),
            NullPolicy::Keep,
        )
        .with_validator(Self::validate_request)
        .then(MergeRoute::new(false, JsonObjectAggBuilder::create))
        .then(MergeRoute::new(true, JsonObjectAggBuilder::create))
        .then(PlainRoute::new(JsonObjectAggBuilder::create))
        .then(IfRoute::new(JsonObjectAggBuilder::create))
        .then(StateRoute::new(JsonObjectAggBuilder::create))
    }

    fn validate_request(request: &AggregateFunctionRequest<'_>) -> Result<()> {
        if request.params.is_empty() {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                request.name
            )))
        }
    }

    fn create(build: DirectBuildContext<'_, impl CombinatorImpl>) -> Result<AggregateFunctionRef> {
        let key_type = build.args_type()[0].remove_nullable();
        if key_type != DataType::String {
            return Err(ErrorCode::BadDataValueType(format!(
                "{} does not support key type '{:?}'",
                build.name(),
                build.args_type()[0]
            )));
        }

        type State = JsonObjectAggState<AnyType>;
        build.create(
            DataType::Variant,
            AggregateJsonObjectAggImplementation::<AnyType, State>::state_description(),
            AggregateJsonObjectAggImplementation::<AnyType, State>::default(),
        )
    }
}
