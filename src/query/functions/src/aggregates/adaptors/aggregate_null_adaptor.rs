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

use std::fmt;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::utils::column_merge_validity;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;

use super::AggrState;
use super::AggrStateLoc;
use super::AggrStateRegistry;
use super::AggrStateType;
use super::AggregateFunction;
use super::AggregateFunctionFeatures;
use super::AggregateFunctionRef;
use super::AggregateNullResultFunction;
use super::StateAddr;

#[derive(Clone)]
pub struct AggregateFunctionCombinatorNull {}

impl AggregateFunctionCombinatorNull {
    pub fn transform_arguments(arguments: &[DataType]) -> Result<Vec<DataType>> {
        let mut results = Vec::with_capacity(arguments.len());

        for arg in arguments.iter() {
            match arg {
                DataType::Nullable(box ty) => {
                    results.push(ty.clone());
                }
                _ => {
                    results.push(arg.clone());
                }
            }
        }
        Ok(results)
    }

    pub fn transform_params(params: &[Scalar]) -> Result<Vec<Scalar>> {
        Ok(params.to_owned())
    }

    pub fn try_create(
        _name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        nested: AggregateFunctionRef,
        properties: AggregateFunctionFeatures,
    ) -> Result<AggregateFunctionRef> {
        // has_null_types
        if arguments.iter().any(|f| f == &DataType::Null) {
            if properties.returns_default_when_only_null {
                return AggregateNullResultFunction::try_create(DataType::Number(
                    NumberDataType::UInt64,
                ));
            } else {
                return AggregateNullResultFunction::try_create(DataType::Null);
            }
        }
        let params = Self::transform_params(&params)?;
        let arguments = Self::transform_arguments(&arguments)?;
        let size = arguments.len();

        // Some functions may have their own null adaptor
        if let Some(null_adaptor) =
            nested.get_own_null_adaptor(nested.clone(), params, arguments)?
        {
            return Ok(null_adaptor);
        }

        let return_type = nested.return_type()?;
        let result_is_null =
            !properties.returns_default_when_only_null && return_type.can_inside_nullable();

        match size {
            1 => match result_is_null {
                true => Ok(AggregateNullUnaryAdaptor::<true>::create(nested)),
                false => Ok(AggregateNullUnaryAdaptor::<false>::create(nested)),
            },

            _ => match result_is_null {
                true => Ok(AggregateNullVariadicAdaptor::<true>::create(nested)),
                false => Ok(AggregateNullVariadicAdaptor::<false>::create(nested)),
            },
        }
    }
}

#[derive(Clone)]
pub struct AggregateNullUnaryAdaptor<const NULLABLE_RESULT: bool>(
    CommonNullAdaptor<NULLABLE_RESULT>,
);

impl<const NULLABLE_RESULT: bool> AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    pub fn create(nested: AggregateFunctionRef) -> AggregateFunctionRef {
        Arc::new(Self(CommonNullAdaptor::<NULLABLE_RESULT> { nested }))
    }
}

impl<const NULLABLE_RESULT: bool> AggregateFunction for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn name(&self) -> &str {
        "AggregateNullUnaryAdaptor"
    }

    fn return_type(&self) -> Result<DataType> {
        self.0.return_type()
    }

    #[inline]
    fn init_state(&self, place: AggrState) {
        self.0.init_state(place);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.0.register_state(registry);
    }

    #[inline]
    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let entry = &columns[0];
        let validity = column_merge_validity(entry, validity.cloned());
        let not_null_column = &[entry.clone().remove_nullable()];
        let not_null_column = not_null_column.into();
        let validity = Bitmap::map_all_sets_to_none(validity);

        self.0
            .accumulate(place, not_null_column, validity, input_rows)
    }

    #[inline]
    fn accumulate_keys(
        &self,
        addrs: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        input_rows: usize,
    ) -> Result<()> {
        let entry = &columns[0];
        let validity = column_merge_validity(entry, None);
        let not_null_columns = &[entry.clone().remove_nullable()];
        let not_null_columns = not_null_columns.into();

        self.0
            .accumulate_keys(addrs, loc, not_null_columns, validity, input_rows)
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let entry = &columns[0];
        let validity = column_merge_validity(entry, None);
        let not_null_columns = &[entry.clone().remove_nullable()];
        let not_null_columns = not_null_columns.into();

        self.0
            .accumulate_row(place, not_null_columns, validity, row)
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        self.0.serialize_type()
    }

    fn serialize(&self, place: AggrState, builders: &mut [ColumnBuilder]) -> Result<()> {
        self.0.serialize(place, builders)
    }

    fn merge(&self, place: AggrState, data: &[ScalarRef]) -> Result<()> {
        self.0.merge(place, data)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        self.0.batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        self.0.merge_states(place, rhs)
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        self.0.merge_result(place, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        self.0.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: AggrState) {
        self.0.drop_state(place);
    }

    fn get_if_condition(&self, columns: ProjectedBlock) -> Option<Bitmap> {
        self.0.nested.get_if_condition(columns)
    }
}

impl<const NULLABLE_RESULT: bool> fmt::Display for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggregateNullUnaryAdaptor")
    }
}

#[derive(Clone)]
pub struct AggregateNullVariadicAdaptor<const NULLABLE_RESULT: bool>(
    CommonNullAdaptor<NULLABLE_RESULT>,
);

impl<const NULLABLE_RESULT: bool> AggregateNullVariadicAdaptor<NULLABLE_RESULT> {
    pub fn create(nested: AggregateFunctionRef) -> AggregateFunctionRef {
        Arc::new(Self(CommonNullAdaptor::<NULLABLE_RESULT> { nested }))
    }
}

impl<const NULLABLE_RESULT: bool> AggregateNullVariadicAdaptor<NULLABLE_RESULT> {
    fn merge_validity(
        columns: ProjectedBlock,
        mut validity: Option<Bitmap>,
    ) -> (Vec<BlockEntry>, Option<Bitmap>) {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        for entry in columns.iter() {
            validity = column_merge_validity(&entry.clone(), validity);
            not_null_columns.push(entry.clone().remove_nullable());
        }
        (not_null_columns, validity)
    }
}

impl<const NULLABLE_RESULT: bool> AggregateFunction
    for AggregateNullVariadicAdaptor<NULLABLE_RESULT>
{
    fn name(&self) -> &str {
        "AggregateNullVariadicAdaptor"
    }

    fn return_type(&self) -> Result<DataType> {
        self.0.return_type()
    }

    fn init_state(&self, place: AggrState) {
        self.0.init_state(place);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.0.register_state(registry);
    }

    #[inline]
    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let (not_null_columns, validity) = Self::merge_validity(columns, validity.cloned());
        let not_null_columns = (&not_null_columns).into();
        self.0
            .accumulate(place, not_null_columns, validity, input_rows)
    }

    fn accumulate_keys(
        &self,
        addrs: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        input_rows: usize,
    ) -> Result<()> {
        let (not_null_columns, validity) = Self::merge_validity(columns, None);
        let not_null_columns = (&not_null_columns).into();
        self.0
            .accumulate_keys(addrs, loc, not_null_columns, validity, input_rows)
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let (not_null_columns, validity) = Self::merge_validity(columns, None);
        let not_null_columns = (&not_null_columns).into();
        self.0
            .accumulate_row(place, not_null_columns, validity, row)
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        self.0.serialize_type()
    }

    fn serialize(&self, place: AggrState, builders: &mut [ColumnBuilder]) -> Result<()> {
        self.0.serialize(place, builders)
    }

    fn merge(&self, place: AggrState, data: &[ScalarRef]) -> Result<()> {
        self.0.merge(place, data)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        self.0.batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        self.0.merge_states(place, rhs)
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        self.0.merge_result(place, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        self.0.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: AggrState) {
        self.0.drop_state(place);
    }

    fn get_if_condition(&self, columns: ProjectedBlock) -> Option<Bitmap> {
        self.0.nested.get_if_condition(columns)
    }
}

impl<const NULLABLE_RESULT: bool> fmt::Display for AggregateNullVariadicAdaptor<NULLABLE_RESULT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggregateNullVariadicAdaptor")
    }
}

#[derive(Clone)]
struct CommonNullAdaptor<const NULLABLE_RESULT: bool> {
    nested: AggregateFunctionRef,
}

impl<const NULLABLE_RESULT: bool> CommonNullAdaptor<NULLABLE_RESULT> {
    fn return_type(&self) -> Result<DataType> {
        if !NULLABLE_RESULT {
            return self.nested.return_type();
        }

        let nested = self.nested.return_type()?;
        Ok(nested.wrap_nullable())
    }

    fn init_state(&self, place: AggrState) {
        if !NULLABLE_RESULT {
            return self.nested.init_state(place);
        }

        set_flag(place, false);
        self.nested.init_state(place.remove_last_loc());
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.nested.register_state(registry);
        if NULLABLE_RESULT {
            registry.register(AggrStateType::Bool);
        }
    }

    #[inline]
    fn accumulate(
        &self,
        place: AggrState,
        not_null_column: ProjectedBlock,
        validity: Option<Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        if !NULLABLE_RESULT {
            return self
                .nested
                .accumulate(place, not_null_column, validity.as_ref(), input_rows);
        }

        if validity
            .as_ref()
            .map(|c| c.null_count() != input_rows)
            .unwrap_or(true)
        {
            set_flag(place, true);
        }
        self.nested.accumulate(
            place.remove_last_loc(),
            not_null_column,
            validity.as_ref(),
            input_rows,
        )
    }

    fn accumulate_keys(
        &self,
        addrs: &[StateAddr],
        loc: &[AggrStateLoc],
        not_null_columns: ProjectedBlock,
        validity: Option<Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        match validity {
            Some(v) if v.null_count() > 0 => {
                // all nulls
                if v.null_count() == v.len() {
                    return Ok(());
                }

                for (valid, (row, place)) in v.iter().zip(addrs.iter().enumerate()) {
                    if !valid {
                        continue;
                    }
                    let place = AggrState::new(*place, loc);
                    if NULLABLE_RESULT {
                        set_flag(place, true);
                        self.nested.accumulate_row(
                            place.remove_last_loc(),
                            not_null_columns,
                            row,
                        )?;
                    } else {
                        self.nested.accumulate_row(place, not_null_columns, row)?;
                    };
                }
                Ok(())
            }
            _ => {
                if !NULLABLE_RESULT {
                    self.nested
                        .accumulate_keys(addrs, loc, not_null_columns, input_rows)
                } else {
                    addrs
                        .iter()
                        .for_each(|addr| set_flag(AggrState::new(*addr, loc), true));
                    self.nested.accumulate_keys(
                        addrs,
                        &loc[..loc.len() - 1],
                        not_null_columns,
                        input_rows,
                    )
                }
            }
        }
    }

    fn accumulate_row(
        &self,
        place: AggrState,
        not_null_columns: ProjectedBlock,
        validity: Option<Bitmap>,
        row: usize,
    ) -> Result<()> {
        let v = if let Some(v) = validity {
            if v.null_count() == 0 {
                true
            } else if v.null_count() == v.len() {
                false
            } else {
                unsafe { v.get_bit_unchecked(row) }
            }
        } else {
            true
        };
        if !v {
            return Ok(());
        }

        if !NULLABLE_RESULT {
            return self.nested.accumulate_row(place, not_null_columns, row);
        }

        set_flag(place, true);
        self.nested
            .accumulate_row(place.remove_last_loc(), not_null_columns, row)
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        if !NULLABLE_RESULT {
            return self.nested.serialize_type();
        }
        self.nested
            .serialize_type()
            .into_iter()
            .chain(Some(StateSerdeItem::DataType(DataType::Boolean)))
            .collect()
    }

    fn serialize(&self, place: AggrState, builders: &mut [ColumnBuilder]) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.serialize(place, builders);
        }
        let n = builders.len();
        debug_assert_eq!(self.nested.serialize_type().len() + 1, n);

        let flag = get_flag(place);
        builders
            .last_mut()
            .and_then(ColumnBuilder::as_boolean_mut)
            .unwrap()
            .push(flag);
        self.nested
            .serialize(place.remove_last_loc(), &mut builders[..(n - 1)])
    }

    fn merge(&self, place: AggrState, data: &[ScalarRef]) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.merge(place, data);
        }

        let flag = *data.last().and_then(ScalarRef::as_boolean).unwrap();
        if !flag {
            return Ok(());
        }

        self.update_flag(place);
        self.nested
            .merge(place.remove_last_loc(), &data[..data.len() - 1])
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.batch_merge(places, loc, state, filter);
        }

        match state {
            BlockEntry::Column(Column::Tuple(tuple)) => {
                let nested_state = Column::Tuple(tuple[0..tuple.len() - 1].to_vec());
                let flag = tuple.last().unwrap().as_boolean().unwrap();
                let flag = match filter {
                    Some(filter) => filter & flag,
                    None => flag.clone(),
                };
                let filter = if flag.null_count() == 0 {
                    for place in places.iter() {
                        self.update_flag(AggrState::new(*place, loc));
                    }
                    None
                } else {
                    for place in places
                        .iter()
                        .zip(flag.iter())
                        .filter_map(|(place, flag)| flag.then_some(place))
                    {
                        self.update_flag(AggrState::new(*place, loc));
                    }
                    Some(&flag)
                };
                self.nested
                    .batch_merge(places, &loc[..loc.len() - 1], &nested_state.into(), filter)
            }
            _ => {
                let state = state.downcast::<AnyType>().unwrap();
                for (place, data) in places.iter().zip(state.iter()) {
                    self.merge(
                        AggrState::new(*place, loc),
                        data.as_tuple().unwrap().as_slice(),
                    )?;
                }
                Ok(())
            }
        }
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.merge_states(place, rhs);
        }

        if !get_flag(rhs) {
            return Ok(());
        }

        if !get_flag(place) {
            // initial the state to remove the dirty stats
            self.init_state(place);
        }
        set_flag(place, true);
        self.nested
            .merge_states(place.remove_last_loc(), rhs.remove_last_loc())
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.merge_result(place, builder);
        }

        let ColumnBuilder::Nullable(ref mut inner) = builder else {
            unreachable!()
        };

        if get_flag(place) {
            inner.validity.push(true);
            self.nested
                .merge_result(place.remove_last_loc(), &mut inner.builder)
        } else {
            inner.push_null();
            Ok(())
        }
    }

    unsafe fn drop_state(&self, place: AggrState) {
        if !NULLABLE_RESULT {
            self.nested.drop_state(place)
        } else {
            self.nested.drop_state(place.remove_last_loc())
        }
    }

    fn update_flag(&self, place: AggrState) {
        if !get_flag(place) {
            // initial the state to remove the dirty stats
            self.init_state(place);
        }
        set_flag(place, true);
    }
}

fn set_flag(place: AggrState, flag: bool) {
    let c = place.addr.next(flag_offset(place)).get::<u8>();
    *c = flag as u8;
}

fn get_flag(place: AggrState) -> bool {
    let c = place.addr.next(flag_offset(place)).get::<u8>();
    *c != 0
}

fn flag_offset(place: AggrState) -> usize {
    *place.loc.last().unwrap().as_bool().unwrap().1
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use databend_common_expression::types::*;
    use databend_common_expression::*;

    use super::*;

    struct TestStateBuffer {
        buffer: Vec<u8>,
    }

    impl TestStateBuffer {
        fn new(layout: &std::alloc::Layout) -> Self {
            let buffer = vec![1u8; layout.size()];
            Self { buffer }
        }

        fn addr(&self) -> StateAddr {
            StateAddr::new(self.buffer.as_ptr() as usize)
        }
    }

    #[derive(Clone)]
    struct MockAggregateFunction {
        return_type: DataType,
        serialize_items: Vec<StateSerdeItem>,
    }

    impl fmt::Display for MockAggregateFunction {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "MockAggregateFunction")
        }
    }

    impl AggregateFunction for MockAggregateFunction {
        fn name(&self) -> &str {
            "mock"
        }

        fn return_type(&self) -> Result<DataType> {
            Ok(self.return_type.clone())
        }

        fn init_state(&self, place: AggrState) {
            place.write(|| 3_u64);
        }

        fn register_state(&self, registry: &mut AggrStateRegistry) {
            registry.register(AggrStateType::Custom(std::alloc::Layout::new::<u64>()));
        }

        fn accumulate(
            &self,
            _place: AggrState,
            _columns: ProjectedBlock,
            _validity: Option<&Bitmap>,
            _input_rows: usize,
        ) -> Result<()> {
            Ok(())
        }

        fn accumulate_row(
            &self,
            _place: AggrState,
            _columns: ProjectedBlock,
            _row: usize,
        ) -> Result<()> {
            Ok(())
        }

        fn serialize_type(&self) -> Vec<StateSerdeItem> {
            self.serialize_items.clone()
        }

        fn serialize(&self, place: AggrState, builders: &mut [ColumnBuilder]) -> Result<()> {
            let state = place.get::<u64>();
            if let ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) = &mut builders[0] {
                builder.push(*state);
            }
            Ok(())
        }

        fn merge(&self, place: AggrState, data: &[ScalarRef]) -> Result<()> {
            let state = place.get::<u64>();
            if let Some(ScalarRef::Number(NumberScalar::UInt64(value))) = data.first() {
                *state += value;
            }
            Ok(())
        }

        fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
            let lhs_state = place.get::<u64>();
            let rhs_state = rhs.get::<u64>();
            *lhs_state += *rhs_state;
            Ok(())
        }

        fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
            let state = place.get::<u64>();
            if let ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) = builder {
                builder.push(*state);
            }
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BatchMergeStrategy {
        Reference,
        Optimized,
    }

    pub struct TestNullFunction<const NULLABLE_RESULT: bool> {
        adaptor: CommonNullAdaptor<NULLABLE_RESULT>,
        strategy: BatchMergeStrategy,
    }

    impl<const NULLABLE_RESULT: bool> TestNullFunction<NULLABLE_RESULT> {
        pub fn new(nested: AggregateFunctionRef, strategy: BatchMergeStrategy) -> Self {
            Self {
                adaptor: CommonNullAdaptor { nested },
                strategy,
            }
        }

        fn reference_batch_merge_impl(
            &self,
            places: &[StateAddr],
            loc: &[AggrStateLoc],
            state: &BlockEntry,
            filter: Option<&Bitmap>,
        ) -> Result<()> {
            let view = state.downcast::<AnyType>().unwrap();
            let iter = places.iter().zip(view.iter());
            if let Some(filter) = filter {
                for (place, data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
                    self.merge(
                        AggrState::new(*place, loc),
                        data.as_tuple().unwrap().as_slice(),
                    )?;
                }
            } else {
                for (place, data) in iter {
                    self.merge(
                        AggrState::new(*place, loc),
                        data.as_tuple().unwrap().as_slice(),
                    )?;
                }
            }

            Ok(())
        }
    }

    impl<const NULLABLE_RESULT: bool> fmt::Display for TestNullFunction<NULLABLE_RESULT> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "ConfigurableNull")
        }
    }

    impl<const NULLABLE_RESULT: bool> AggregateFunction for TestNullFunction<NULLABLE_RESULT> {
        fn name(&self) -> &str {
            "test_null"
        }

        fn return_type(&self) -> Result<DataType> {
            self.adaptor.return_type()
        }

        fn init_state(&self, place: AggrState) {
            self.adaptor.init_state(place)
        }

        fn register_state(&self, registry: &mut AggrStateRegistry) {
            self.adaptor.register_state(registry)
        }

        fn accumulate(
            &self,
            place: AggrState,
            columns: ProjectedBlock,
            validity: Option<&Bitmap>,
            input_rows: usize,
        ) -> Result<()> {
            self.adaptor
                .accumulate(place, columns, validity.cloned(), input_rows)
        }

        fn accumulate_row(
            &self,
            place: AggrState,
            columns: ProjectedBlock,
            row: usize,
        ) -> Result<()> {
            self.adaptor.accumulate_row(place, columns, None, row)
        }

        fn serialize_type(&self) -> Vec<StateSerdeItem> {
            self.adaptor.serialize_type()
        }

        fn serialize(&self, place: AggrState, builders: &mut [ColumnBuilder]) -> Result<()> {
            self.adaptor.serialize(place, builders)
        }

        fn merge(&self, place: AggrState, data: &[ScalarRef]) -> Result<()> {
            self.adaptor.merge(place, data)
        }

        fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
            self.adaptor.merge_states(place, rhs)
        }

        fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
            self.adaptor.merge_result(place, builder)
        }

        /// Custom batch_merge implementation that switches between reference and optimized strategies
        fn batch_merge(
            &self,
            places: &[StateAddr],
            loc: &[AggrStateLoc],
            state: &BlockEntry,
            filter: Option<&Bitmap>,
        ) -> Result<()> {
            match self.strategy {
                BatchMergeStrategy::Reference => {
                    self.reference_batch_merge_impl(places, loc, state, filter)
                }
                BatchMergeStrategy::Optimized => {
                    self.adaptor.batch_merge(places, loc, state, filter)
                }
            }
        }
    }

    fn create_configurable(strategy: BatchMergeStrategy) -> TestNullFunction<true> {
        let nested = Arc::new(MockAggregateFunction {
            return_type: DataType::Number(NumberDataType::UInt64),
            serialize_items: vec![StateSerdeItem::DataType(DataType::Number(
                NumberDataType::UInt64,
            ))],
        });
        TestNullFunction::new(nested, strategy)
    }

    fn create_test_state_entry(values: &[u64], flags: &[bool]) -> Result<BlockEntry> {
        // Build the state column as a tuple
        let mut value_builder =
            ColumnBuilder::with_capacity(&DataType::Number(NumberDataType::UInt64), values.len());
        let mut flag_builder = ColumnBuilder::with_capacity(&DataType::Boolean, flags.len());

        for &value in values {
            if let ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) = &mut value_builder
            {
                builder.push(value);
            }
        }

        for &flag in flags {
            if let ColumnBuilder::Boolean(builder) = &mut flag_builder {
                builder.push(flag);
            }
        }

        let value_column = value_builder.build();
        let flag_column = flag_builder.build();
        let tuple_column = Column::Tuple(vec![value_column, flag_column]);

        Ok(BlockEntry::new(Value::Column(tuple_column), || {
            (
                DataType::Tuple(vec![
                    DataType::Number(NumberDataType::UInt64),
                    DataType::Boolean,
                ]),
                values.len(),
            )
        }))
    }

    fn run_batch_merge_with_strategy(
        strategy: BatchMergeStrategy,
        buffers: &[TestStateBuffer],
        loc: &[AggrStateLoc],
        state_entry: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        let func = create_configurable(strategy);
        let places: Vec<StateAddr> = buffers.iter().map(|b| b.addr()).collect();

        // Initialize states
        for buffer in buffers {
            let place = AggrState::new(buffer.addr(), loc);
            func.init_state(place);
        }

        // Run batch_merge
        func.batch_merge(&places, loc, state_entry, filter)
    }

    fn test_both_strategies(values: &[u64], flags: &[bool], filter: Option<&Bitmap>) -> Result<()> {
        let state_entry = create_test_state_entry(values, flags)?;
        let (reference_buffers, optimized_buffers, loc) = {
            let buffer_count = values.len();
            let states_layout =
                get_states_layout(&[Arc::new(create_configurable(BatchMergeStrategy::Reference))])?;
            let layout = states_layout.layout;
            let loc = states_layout.states_loc[0].to_vec();

            let reference_buffers: Vec<TestStateBuffer> = (0..buffer_count)
                .map(|_| TestStateBuffer::new(&layout))
                .collect();
            let optimized_buffers: Vec<TestStateBuffer> = (0..buffer_count)
                .map(|_| TestStateBuffer::new(&layout))
                .collect();

            (reference_buffers, optimized_buffers, loc)
        };

        // Run both strategies
        run_batch_merge_with_strategy(
            BatchMergeStrategy::Reference,
            &reference_buffers,
            &loc,
            &state_entry,
            filter,
        )?;

        run_batch_merge_with_strategy(
            BatchMergeStrategy::Optimized,
            &optimized_buffers,
            &loc,
            &state_entry,
            filter,
        )?;

        for i in 0..reference_buffers.len() {
            assert_eq!(
                reference_buffers[i].buffer, optimized_buffers[i].buffer,
                "Buffer contents should match at index {}",
                i
            );
        }
        Ok(())
    }

    #[test]
    fn test_batch_merge_equivalence() -> Result<()> {
        {
            let values = vec![10u64, 20u64, 30u64];
            let flags = vec![true, false, true];

            test_both_strategies(&values, &flags, None)?;
        }

        {
            let values = vec![100u64, 200u64, 300u64, 400u64];
            let flags = vec![true, false, true, true]; // Skip index 1

            test_both_strategies(&values, &flags, None)?;
        }

        {
            let values = vec![10u64, 20u64, 30u64, 40u64];
            let flags = vec![true, false, true, true];
            let filter_bits = [true, false, true, false]; // Only process indices 0 and 2
            let filter = Bitmap::from_iter(filter_bits.iter().copied());

            test_both_strategies(&values, &flags, Some(&filter))?;
        }

        {
            let values = vec![100u64, 200u64, 300u64, 400u64];
            let flags = vec![true, true, false, true]; // Skip index 2
            let filter_bits = [true, false, true, true]; // Skip index 1
            let filter = Bitmap::from_iter(filter_bits.iter().copied());

            test_both_strategies(&values, &flags, Some(&filter))?;
        }

        Ok(())
    }
}
