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

use databend_common_exception::Result;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::StateAddr;
use databend_common_expression::StateSerdeItem;

use super::AggrState;
use super::AggrStateLoc;
use super::SerializeInfo;
use super::StateSerde;

pub(super) trait UnaryState<T, R>: StateSerde + Default + Send + 'static
where
    T: AccessType,
    R: ValueType,
{
    type FunctionInfo: Send + Sync = ();

    fn add(&mut self, other: T::ScalarRef<'_>, function_data: &Self::FunctionInfo) -> Result<()>;

    fn add_batch(
        &mut self,
        other: ColumnView<T>,
        validity: Option<&Bitmap>,
        function_data: &Self::FunctionInfo,
    ) -> Result<()> {
        match validity {
            Some(validity) => {
                for (data, valid) in other.iter().zip(validity.iter()) {
                    if valid {
                        self.add(data, function_data)?;
                    }
                }
            }
            None => {
                for value in other.iter() {
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
        function_info: &Self::FunctionInfo,
    ) -> Result<()>;
}

pub(super) struct AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R>,
    T: AccessType,
    R: ValueType,
{
    display_name: String,
    return_type: DataType,
    function_info: S::FunctionInfo,
    serialize_info: Option<Box<dyn SerializeInfo>>,
    need_drop: bool,
    _p: PhantomData<fn(S, T, R)>,
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
    S: UnaryState<T, R, FunctionInfo = ()> + 'static,
    T: AccessType,
    R: ValueType,
{
    pub fn new(display_name: &str, return_type: DataType) -> Self {
        Self::with_function_info(display_name, return_type, ())
    }

    pub fn create(display_name: &str, return_type: DataType) -> Result<AggregateFunctionRef> {
        Self::with_function_info(display_name, return_type, ()).finish()
    }
}

impl<S, T, R> AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R> + 'static,
    T: AccessType,
    R: ValueType,
{
    pub fn with_function_info(
        display_name: &str,
        return_type: DataType,
        function_info: S::FunctionInfo,
    ) -> Self {
        AggregateUnaryFunction {
            display_name: display_name.to_string(),
            return_type,
            function_info,
            serialize_info: None,
            need_drop: false,
            _p: PhantomData,
        }
    }

    pub fn with_serialize_info(mut self, serialize_info: Box<dyn SerializeInfo>) -> Self {
        self.serialize_info = Some(serialize_info);
        self
    }

    pub fn with_need_drop(mut self, need_drop: bool) -> Self {
        self.need_drop = need_drop;
        self
    }

    pub fn finish(self) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(self))
    }

    fn do_merge_result(&self, state: &mut S, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = R::downcast_builder(builder);
        state.merge_result(builder, &self.function_info)
    }
}

impl<S, T, R> AggregateFunction for AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R>,
    T: AccessType,
    R: ValueType,
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
        let column = columns[0].downcast().unwrap();
        let state: &mut S = place.get::<S>();

        state.add_batch(column, validity, &self.function_info)
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let view = columns[0].downcast::<T>().unwrap();
        let value = view.index(row).unwrap();

        let state: &mut S = place.get::<S>();
        state.add(value, &self.function_info)?;
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
            state.add(v, &self.function_info)?;
        }

        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        S::serialize_type(self.serialize_info.as_deref())
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

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
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
