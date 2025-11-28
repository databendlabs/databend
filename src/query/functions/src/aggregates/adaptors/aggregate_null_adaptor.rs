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
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::utils::column_merge_validity;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;

use super::AggrState;
use super::AggrStateLoc;
use super::AggrStateRegistry;
use super::AggrStateType;
use super::AggregateFunction;
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

    pub fn try_create(
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        nested: AggregateFunctionRef,
        returns_default_when_only_null: bool,
    ) -> Result<AggregateFunctionRef> {
        // has_null_types
        if arguments.iter().any(|f| f == &DataType::Null) {
            if returns_default_when_only_null {
                return AggregateNullResultFunction::try_create(DataType::Number(
                    NumberDataType::UInt64,
                ));
            } else {
                return AggregateNullResultFunction::try_create(DataType::Null);
            }
        }
        let arguments = Self::transform_arguments(&arguments)?;
        let size = arguments.len();

        // Some functions may have their own null adaptor
        if let Some(null_adaptor) =
            nested.get_own_null_adaptor(nested.clone(), params, arguments)?
        {
            return Ok(null_adaptor);
        }

        let return_type = nested.return_type()?;
        let result_is_null = !returns_default_when_only_null && return_type.can_inside_nullable();

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

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        self.0.batch_serialize(places, loc, builders)
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

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        self.0.merge_result(place, read_only, builder)
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

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        self.0.batch_serialize(places, loc, builders)
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

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        self.0.merge_result(place, read_only, builder)
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

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.batch_serialize(places, loc, builders);
        }
        let n = builders.len();
        debug_assert_eq!(self.nested.serialize_type().len() + 1, n);
        let flag_builder = builders
            .last_mut()
            .and_then(ColumnBuilder::as_boolean_mut)
            .unwrap();
        for place in places {
            let place = AggrState::new(*place, loc);
            flag_builder.push(get_flag(place));
        }
        self.nested
            .batch_serialize(places, &loc[..loc.len() - 1], &mut builders[..(n - 1)])
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
            BlockEntry::Const(Scalar::Tuple(tuple), DataType::Tuple(data_type), num_rows) => {
                let flag = *tuple.last().and_then(Scalar::as_boolean).unwrap();
                let flag = Bitmap::new_constant(flag, *num_rows);
                let flag = match filter {
                    Some(filter) => filter & &flag,
                    None => flag,
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
                let nested_state = BlockEntry::new_const_column(
                    DataType::Tuple(data_type[0..data_type.len() - 1].to_vec()),
                    Scalar::Tuple(tuple[0..tuple.len() - 1].to_vec()),
                    *num_rows,
                );
                self.nested
                    .batch_merge(places, &loc[..loc.len() - 1], &nested_state, filter)
            }
            _ => unreachable!(),
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

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.merge_result(place, read_only, builder);
        }

        let ColumnBuilder::Nullable(ref mut inner) = builder else {
            unreachable!()
        };

        if get_flag(place) {
            inner.validity.push(true);
            self.nested
                .merge_result(place.remove_last_loc(), read_only, &mut inner.builder)
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
    let n = place.loc.len();
    debug_assert!(n > 0);
    let loc = unsafe { place.loc.get_unchecked(n - 1) };
    debug_assert!(loc.is_bool());
    loc.offset()
}
