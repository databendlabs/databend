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
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_io::prelude::BinaryWrite;

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

    fn serialize_size_per_row(&self) -> Option<usize> {
        self.0.serialize_size_per_row()
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.0.register_state(registry);
    }

    #[inline]
    fn accumulate(
        &self,
        place: AggrState,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let col = &columns[0];
        let validity = column_merge_validity(col, validity.cloned());
        let not_null_column = &[col.remove_nullable()];
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
        columns: InputColumns,
        input_rows: usize,
    ) -> Result<()> {
        let col = &columns[0];
        let validity = column_merge_validity(col, None);
        let not_null_columns = &[col.remove_nullable()];
        let not_null_columns = not_null_columns.into();

        self.0
            .accumulate_keys(addrs, loc, not_null_columns, validity, input_rows)
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        let col = &columns[0];
        let validity = column_merge_validity(col, None);
        let not_null_columns = &[col.remove_nullable()];
        let not_null_columns = not_null_columns.into();

        self.0
            .accumulate_row(place, not_null_columns, validity, row)
    }

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        self.0.serialize(place, writer)
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        self.0.merge(place, reader)
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

    fn convert_const_to_full(&self) -> bool {
        self.0.nested.convert_const_to_full()
    }

    fn get_if_condition(&self, columns: InputColumns) -> Option<Bitmap> {
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

    fn serialize_size_per_row(&self) -> Option<usize> {
        self.0.serialize_size_per_row()
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.0.register_state(registry);
    }

    #[inline]
    fn accumulate(
        &self,
        place: AggrState,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        let mut validity = validity.cloned();
        for col in columns.iter() {
            validity = column_merge_validity(col, validity);
            not_null_columns.push(col.remove_nullable());
        }
        let not_null_columns = (&not_null_columns).into();

        self.0
            .accumulate(place, not_null_columns, validity, input_rows)
    }

    fn accumulate_keys(
        &self,
        addrs: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: InputColumns,
        input_rows: usize,
    ) -> Result<()> {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        let mut validity = None;
        for col in columns.iter() {
            validity = column_merge_validity(col, validity);
            not_null_columns.push(col.remove_nullable());
        }
        let not_null_columns = (&not_null_columns).into();

        self.0
            .accumulate_keys(addrs, loc, not_null_columns, validity, input_rows)
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        let mut validity = None;
        for col in columns.iter() {
            validity = column_merge_validity(col, validity);
            not_null_columns.push(col.remove_nullable());
        }
        let not_null_columns = (&not_null_columns).into();

        self.0
            .accumulate_row(place, not_null_columns, validity, row)
    }

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        self.0.serialize(place, writer)
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        self.0.merge(place, reader)
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

    fn convert_const_to_full(&self) -> bool {
        self.0.nested.convert_const_to_full()
    }

    fn get_if_condition(&self, columns: InputColumns) -> Option<Bitmap> {
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

    fn serialize_size_per_row(&self) -> Option<usize> {
        self.nested
            .serialize_size_per_row()
            .map(|row| if NULLABLE_RESULT { row + 1 } else { row })
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
        not_null_column: InputColumns,
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
        not_null_columns: InputColumns,
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
        not_null_columns: InputColumns,
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

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.serialize(place, writer);
        }

        self.nested.serialize(place.remove_last_loc(), writer)?;
        let flag = get_flag(place);
        writer.write_scalar(&flag)
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.merge(place, reader);
        }

        let flag = reader[reader.len() - 1];
        if flag == 0 {
            return Ok(());
        }

        if !get_flag(place) {
            // initial the state to remove the dirty stats
            self.init_state(place);
        }
        set_flag(place, true);
        self.nested
            .merge(place.remove_last_loc(), &mut &reader[..reader.len() - 1])
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
