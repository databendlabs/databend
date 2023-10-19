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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::utils::column_merge_validity;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_io::prelude::BinaryWrite;

use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;

#[derive(Clone)]
pub struct AggregateNullVariadicAdaptor<const NULLABLE_RESULT: bool> {
    nested: AggregateFunctionRef,
    size_of_data: usize,
}

impl<const NULLABLE_RESULT: bool> AggregateNullVariadicAdaptor<NULLABLE_RESULT> {
    pub fn create(nested: AggregateFunctionRef) -> AggregateFunctionRef {
        let size_of_data = if NULLABLE_RESULT {
            let layout = nested.state_layout();
            layout.size()
        } else {
            0
        };
        Arc::new(Self {
            nested,
            size_of_data,
        })
    }

    #[inline]
    pub fn set_flag(&self, place: StateAddr, flag: u8) {
        if NULLABLE_RESULT {
            let c = place.next(self.size_of_data).get::<u8>();
            *c = flag;
        }
    }

    #[inline]
    pub fn init_flag(&self, place: StateAddr) {
        if NULLABLE_RESULT {
            let c = place.next(self.size_of_data).get::<u8>();
            *c = 0;
        }
    }

    #[inline]
    pub fn get_flag(&self, place: StateAddr) -> u8 {
        if NULLABLE_RESULT {
            let c = place.next(self.size_of_data).get::<u8>();
            *c
        } else {
            1
        }
    }
}

impl<const NULLABLE_RESULT: bool> AggregateFunction
    for AggregateNullVariadicAdaptor<NULLABLE_RESULT>
{
    fn name(&self) -> &str {
        "AggregateNullVariadicAdaptor"
    }

    fn return_type(&self) -> Result<DataType> {
        let nested = self.nested.return_type()?;
        match NULLABLE_RESULT {
            true => Ok(nested.wrap_nullable()),
            false => Ok(nested),
        }
    }

    fn init_state(&self, place: StateAddr) {
        self.init_flag(place);
        self.nested.init_state(place);
    }

    fn serialize_size_per_row(&self) -> Option<usize> {
        self.nested.serialize_size_per_row().map(|row| row + 1)
    }

    #[inline]
    fn state_layout(&self) -> Layout {
        let layout = self.nested.state_layout();
        let add = if NULLABLE_RESULT { layout.align() } else { 0 };
        Layout::from_size_align(layout.size() + add, layout.align()).unwrap()
    }

    #[inline]
    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        let mut validity = validity.cloned();
        for col in columns.iter() {
            validity = column_merge_validity(col, validity);
            not_null_columns.push(col.remove_nullable());
        }

        self.nested
            .accumulate(place, &not_null_columns, validity.as_ref(), input_rows)?;

        if validity
            .as_ref()
            .map(|c| c.unset_bits() != input_rows)
            .unwrap_or(true)
        {
            self.set_flag(place, 1);
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        input_rows: usize,
    ) -> Result<()> {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        let mut validity = None;
        for col in columns.iter() {
            validity = column_merge_validity(col, validity);
            not_null_columns.push(col.remove_nullable());
        }

        match validity {
            Some(v) if v.unset_bits() > 0 => {
                // all nulls
                if v.unset_bits() == v.len() {
                    return Ok(());
                }
                for (valid, (row, place)) in v.iter().zip(places.iter().enumerate()) {
                    if valid {
                        self.set_flag(place.next(offset), 1);
                        self.nested
                            .accumulate_row(place.next(offset), &not_null_columns, row)?;
                    }
                }
            }
            _ => {
                self.nested
                    .accumulate_keys(places, offset, &not_null_columns, input_rows)?;
                places
                    .iter()
                    .for_each(|place| self.set_flag(place.next(offset), 1));
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        let mut validity = None;
        for col in columns.iter() {
            validity = column_merge_validity(col, validity);
            not_null_columns.push(col.remove_nullable());
        }

        match validity {
            Some(v) if v.unset_bits() > 0 => {
                // all nulls
                if v.unset_bits() == v.len() {
                    return Ok(());
                }

                if unsafe { v.get_bit_unchecked(row) } {
                    self.set_flag(place, 1);
                    self.nested.accumulate_row(place, &not_null_columns, row)?;
                }
            }
            _ => {
                self.nested.accumulate_row(place, &not_null_columns, row)?;
                self.set_flag(place, 1);
            }
        }

        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        self.nested.serialize(place, writer)?;
        if NULLABLE_RESULT {
            let flag: u8 = self.get_flag(place);
            writer.write_scalar(&flag)?;
        }
        Ok(())
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        if self.get_flag(place) == 0 {
            // initial the state to remove the dirty stats
            self.init_state(place);
        }

        if NULLABLE_RESULT {
            let flag = reader[reader.len() - 1];
            if flag == 1 {
                self.set_flag(place, flag);
                self.nested.merge(place, &mut &reader[..reader.len() - 1])?;
            }
        } else {
            self.nested.merge(place, reader)?;
        }
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        if self.get_flag(place) == 0 {
            // initial the state to remove the dirty stats
            self.init_state(place);
        }

        if self.get_flag(rhs) == 1 {
            self.set_flag(place, 1);
            self.nested.merge_states(place, rhs)?;
        }
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        if NULLABLE_RESULT {
            if self.get_flag(place) == 1 {
                match builder {
                    ColumnBuilder::Nullable(ref mut inner) => {
                        self.nested.merge_result(place, &mut inner.builder)?;
                        inner.validity.push(true);
                    }
                    _ => unreachable!(),
                }
            } else {
                builder.push_default();
            }
            Ok(())
        } else {
            self.nested.merge_result(place, builder)
        }
    }

    fn need_manual_drop_state(&self) -> bool {
        self.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        self.nested.drop_state(place)
    }

    fn convert_const_to_full(&self) -> bool {
        self.nested.convert_const_to_full()
    }

    fn get_if_condition(&self, columns: &[Column]) -> Option<Bitmap> {
        self.nested.get_if_condition(columns)
    }
}

impl<const NULLABLE_RESULT: bool> fmt::Display for AggregateNullVariadicAdaptor<NULLABLE_RESULT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggregateNullVariadicAdaptor")
    }
}
