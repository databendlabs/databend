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
use std::any::Any;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UnaryType;
use databend_common_expression::types::ValueType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;

use crate::aggregates::AggrState;
use crate::aggregates::AggrStateLoc;

pub trait UnaryState<T, R>:
    Send + Sync + Default + borsh::BorshSerialize + borsh::BorshDeserialize
where
    T: AccessType,
    R: ValueType,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()>;

    fn add_batch(
        &mut self,
        other: T::Column,
        validity: Option<&Bitmap>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        match validity {
            Some(validity) => {
                for (data, valid) in T::iter_column(&other).zip(validity.iter()) {
                    if valid {
                        self.add(data, function_data)?;
                    }
                }
            }
            None => {
                for value in T::iter_column(&other) {
                    self.add(value, function_data)?;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()>;

    fn merge_result(
        &mut self,
        builder: R::ColumnBuilderMut<'_>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()>;

    fn serialize_type(_function_data: Option<&dyn FunctionData>) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state: &mut Self = AggrState::new(*place, loc).get();
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
        let view = state.downcast::<UnaryType<BinaryType>>().unwrap();
        let iter = places.iter().zip(view.iter());
        if let Some(filter) = filter {
            for (place, mut data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
                let rhs = Self::deserialize_reader(&mut data)?;
                let state: &mut Self = AggrState::new(*place, loc).get();
                state.merge(&rhs)?;
            }
        } else {
            for (place, mut data) in iter {
                let rhs = Self::deserialize_reader(&mut data)?;
                let state: &mut Self = AggrState::new(*place, loc).get();
                state.merge(&rhs)?;
            }
        }
        Ok(())
    }
}

pub trait FunctionData: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R>,
    T: AccessType,
    R: ValueType,
{
    display_name: String,
    return_type: DataType,
    function_data: Option<Box<dyn FunctionData>>,
    need_drop: bool,
    _phantom: PhantomData<(S, T, R)>,
}

impl<S, T, R> Display for AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R>,
    T: AccessType,
    R: ValueType,
{
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<S, T, R> AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R> + 'static,
    T: Send + Sync + AccessType,
    R: Send + Sync + ValueType,
{
    pub(crate) fn try_create_unary(
        display_name: &str,
        return_type: DataType,
        _params: Vec<Scalar>,
        _argument: DataType,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self::try_create(
            display_name,
            return_type,
            _params,
            _argument,
        )))
    }

    pub(crate) fn try_create(
        display_name: &str,
        return_type: DataType,
        _params: Vec<Scalar>,
        _argument: DataType,
    ) -> AggregateUnaryFunction<S, T, R> {
        AggregateUnaryFunction {
            display_name: display_name.to_string(),
            return_type,
            function_data: None,
            need_drop: false,
            _phantom: Default::default(),
        }
    }

    pub(crate) fn with_function_data(
        mut self,
        function_data: Box<dyn FunctionData>,
    ) -> AggregateUnaryFunction<S, T, R> {
        self.function_data = Some(function_data);
        self
    }

    pub(crate) fn with_need_drop(mut self, need_drop: bool) -> AggregateUnaryFunction<S, T, R> {
        self.need_drop = need_drop;
        self
    }

    fn do_merge_result(&self, state: &mut S, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = R::downcast_builder(builder);
        state.merge_result(builder, self.function_data.as_deref())
    }
}

impl<S, T, R> AggregateFunction for AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R> + 'static,
    T: Send + Sync + AccessType,
    R: Send + Sync + ValueType,
{
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: AggrState) {
        place.write(S::default);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<S>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = T::try_downcast_column(&columns[0].to_column()).unwrap();
        let state: &mut S = place.get::<S>();

        state.add_batch(column, validity, self.function_data.as_deref())
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let view = columns[0].downcast::<T>().unwrap();
        let value = view.index(row).unwrap();

        let state: &mut S = place.get::<S>();
        state.add(value, self.function_data.as_deref())?;
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let view = columns[0].downcast::<T>().unwrap();

        for (v, place) in view.iter().zip(places.iter()) {
            let state: &mut S = AggrState::new(*place, loc).get::<S>();
            state.add(v, self.function_data.as_deref())?;
        }

        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        S::serialize_type(self.function_data.as_deref())
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        S::batch_serialize(places, loc, builders)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        S::batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state: &mut S = place.get::<S>();
        let other: &mut S = rhs.get::<S>();
        state.merge(other)
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let state: &mut S = place.get::<S>();
        self.do_merge_result(state, builder)
    }

    fn batch_merge_result(
        &self,
        places: &[StateAddr],
        loc: Box<[AggrStateLoc]>,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        for place in places {
            let state: &mut S = AggrState::new(*place, &loc).get::<S>();
            self.do_merge_result(state, builder)?;
        }
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        self.need_drop
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<S>();
        std::ptr::drop_in_place(state);
    }
}
