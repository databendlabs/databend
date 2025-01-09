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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::utils::column_merge_validity;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_io::prelude::BinaryWrite;

use crate::aggregates::AggrState;
use crate::aggregates::AggrStateLoc;
use crate::aggregates::AggrStateRegistry;
use crate::aggregates::AggrStateType;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;

#[derive(Clone)]
pub struct AggregateNullUnaryAdaptor<const NULLABLE_RESULT: bool> {
    nested: AggregateFunctionRef,
}

impl<const NULLABLE_RESULT: bool> AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    pub fn create(nested: AggregateFunctionRef) -> AggregateFunctionRef {
        Arc::new(Self { nested })
    }
}

fn set_flag(place: &AggrState, flag: bool) {
    let c = place.addr.next(flag_offset(place)).get::<u8>();
    *c = flag as u8;
}

fn get_flag(place: &AggrState) -> bool {
    let c = place.addr.next(flag_offset(place)).get::<u8>();
    *c != 0
}

fn flag_offset(place: &AggrState) -> usize {
    *place.loc().last().unwrap().as_bool().unwrap().1
}

impl<const NULLABLE_RESULT: bool> AggregateFunction for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn name(&self) -> &str {
        "AggregateNullUnaryAdaptor"
    }

    fn return_type(&self) -> Result<DataType> {
        let nested = self.nested.return_type()?;
        match NULLABLE_RESULT {
            true => Ok(nested.wrap_nullable()),
            false => Ok(nested),
        }
    }

    #[inline]
    fn init_state(&self, place: &AggrState) {
        if NULLABLE_RESULT {
            set_flag(place, false);
            self.nested.init_state(&place.remove_last_loc());
        } else {
            self.nested.init_state(place);
        }
    }

    fn serialize_size_per_row(&self) -> Option<usize> {
        self.nested
            .serialize_size_per_row()
            .map(|row| if NULLABLE_RESULT { row + 1 } else { row })
    }

    #[inline]
    fn state_layout(&self) -> Layout {
        unreachable!()
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
        place: &AggrState,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let col = &columns[0];
        let validity = column_merge_validity(col, validity.cloned());
        let not_null_column = &[col.remove_nullable()];
        let not_null_column = not_null_column.into();
        let validity = Bitmap::map_all_sets_to_none(validity);

        if !NULLABLE_RESULT {
            self.nested
                .accumulate(place, not_null_column, validity.as_ref(), input_rows)
        } else {
            if validity
                .as_ref()
                .map(|c| c.null_count() != input_rows)
                .unwrap_or(true)
            {
                set_flag(place, true);
            }
            self.nested.accumulate(
                &place.remove_last_loc(),
                not_null_column,
                validity.as_ref(),
                input_rows,
            )
        }
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
                    let place = if NULLABLE_RESULT {
                        let place = AggrState::new(*place, loc);
                        set_flag(&place, true);
                        place.remove_last_loc()
                    } else {
                        AggrState::new(*place, loc)
                    };
                    self.nested.accumulate_row(&place, not_null_columns, row)?;
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
                        .for_each(|addr| set_flag(&AggrState::new(*addr, loc), true));
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

    fn accumulate_row(&self, place: &AggrState, columns: InputColumns, row: usize) -> Result<()> {
        let col = &columns[0];
        let validity = column_merge_validity(col, None);
        let not_null_columns = &[col.remove_nullable()];
        let not_null_columns = not_null_columns.into();

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

        if NULLABLE_RESULT {
            set_flag(place, true);
            self.nested
                .accumulate_row(&place.remove_last_loc(), not_null_columns, row)
        } else {
            self.nested.accumulate_row(place, not_null_columns, row)
        }
    }

    fn serialize(&self, place: &AggrState, writer: &mut Vec<u8>) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.serialize(place, writer);
        }

        self.nested.serialize(&place.remove_last_loc(), writer)?;
        let flag = get_flag(place);
        writer.write_scalar(&flag)
    }

    fn merge(&self, place: &AggrState, reader: &mut &[u8]) -> Result<()> {
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
            .merge(&place.remove_last_loc(), &mut &reader[..reader.len() - 1])
    }

    fn merge_states(&self, place: &AggrState, rhs: &AggrState) -> Result<()> {
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
            .merge_states(&place.remove_last_loc(), &rhs.remove_last_loc())
    }

    fn merge_result(&self, place: &AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        if !NULLABLE_RESULT {
            return self.nested.merge_result(place, builder);
        }

        let ColumnBuilder::Nullable(ref mut inner) = builder else {
            unreachable!()
        };

        if get_flag(place) {
            inner.validity.push(true);
            self.nested
                .merge_result(&place.remove_last_loc(), &mut inner.builder)
        } else {
            inner.push_null();
            Ok(())
        }
    }

    fn need_manual_drop_state(&self) -> bool {
        self.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: &AggrState) {
        self.nested.drop_state(place)
    }

    fn convert_const_to_full(&self) -> bool {
        self.nested.convert_const_to_full()
    }

    fn get_if_condition(&self, columns: InputColumns) -> Option<Bitmap> {
        self.nested.get_if_condition(columns)
    }
}

impl<const NULLABLE_RESULT: bool> fmt::Display for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggregateNullUnaryAdaptor")
    }
}
