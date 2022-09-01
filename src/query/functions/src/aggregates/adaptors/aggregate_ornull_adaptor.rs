// Copyright 2021 Datafuse Labs.
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
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::BinaryRead;
use common_io::prelude::BinaryWriteBuf;

use super::AggregateFunctionBasicAdaptor;
use crate::aggregates::aggregate_function_factory::AggregateFunctionFeatures;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;

/// OrNullAdaptor will use OrNull for aggregate functions.
/// If there are no input values, return NULL or a default value, accordingly.
/// Use a single additional byte of data after the nested function data:
/// 0 means there was no input, 1 means there was some.
pub struct AggregateFunctionOrNullAdaptor {
    inner: AggregateFunctionRef,
    size_of_data: usize,
    inner_nullable: bool,
}

impl AggregateFunctionOrNullAdaptor {
    pub fn create(
        inner: AggregateFunctionRef,
        features: AggregateFunctionFeatures,
    ) -> Result<AggregateFunctionRef> {
        // count/count distinct should not be nullable for empty set, just return zero
        let inner_return_type = inner.return_type()?;
        if features.returns_default_when_only_null
            || inner_return_type.data_type_id() == TypeID::Null
        {
            return AggregateFunctionBasicAdaptor::create(inner);
        }

        let inner_layout = inner.state_layout();
        let inner_nullable = inner_return_type.is_nullable();
        Ok(Arc::new(AggregateFunctionOrNullAdaptor {
            inner,
            size_of_data: inner_layout.size(),
            inner_nullable,
        }))
    }

    #[inline]
    pub fn set_flag(&self, place: StateAddr, flag: u8) {
        let c = place.next(self.size_of_data).get::<u8>();
        *c = flag;
    }

    #[inline]
    pub fn get_flag(&self, place: StateAddr) -> u8 {
        let c = place.next(self.size_of_data).get::<u8>();
        *c
    }
}

impl AggregateFunction for AggregateFunctionOrNullAdaptor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn return_type(&self) -> Result<DataTypeImpl> {
        Ok(wrap_nullable(&self.inner.return_type()?))
    }

    #[inline]
    fn init_state(&self, place: StateAddr) {
        let c = place.next(self.size_of_data).get::<u8>();
        *c = 0;
        self.inner.init_state(place)
    }

    #[inline]
    fn state_layout(&self) -> std::alloc::Layout {
        let layout = self.inner.state_layout();
        Layout::from_size_align(layout.size() + 1, layout.align()).unwrap()
    }

    #[inline]
    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        if input_rows == 0 {
            return Ok(());
        }

        let if_cond = self.inner.get_if_condition(columns);
        if let Some(bm) = if_cond {
            if bm.unset_bits() == input_rows {
                return Ok(());
            }
        }

        if let Some(bm) = validity {
            if bm.unset_bits() == input_rows {
                return Ok(());
            }
        }
        self.set_flag(place, 1);

        if self.inner.convert_const_to_full() && columns.iter().any(|c| c.is_const()) {
            let columns: Vec<ColumnRef> = columns.iter().map(|c| c.convert_full_column()).collect();
            self.inner.accumulate(place, &columns, validity, input_rows)
        } else {
            self.inner.accumulate(place, columns, validity, input_rows)
        }
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        input_rows: usize,
    ) -> Result<()> {
        if self.inner.convert_const_to_full() && columns.iter().any(|c| c.is_const()) {
            let columns: Vec<ColumnRef> = columns.iter().map(|c| c.convert_full_column()).collect();
            self.inner
                .accumulate_keys(places, offset, &columns, input_rows)?;
        } else {
            self.inner
                .accumulate_keys(places, offset, columns, input_rows)?;
        }
        let if_cond = self.inner.get_if_condition(columns);
        match if_cond {
            Some(bm) if bm.unset_bits() != 0 => {
                for (place, valid) in places.iter().zip(bm.iter()) {
                    if valid {
                        self.set_flag(*place, 1);
                    }
                }
            }
            _ => {
                for place in places {
                    self.set_flag(place.next(offset), 1);
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        self.inner.accumulate_row(place, columns, row)?;
        self.set_flag(place, 1);
        Ok(())
    }

    #[inline]
    fn serialize(&self, place: StateAddr, writer: &mut bytes::BytesMut) -> Result<()> {
        self.inner.serialize(place, writer)?;
        writer.write_scalar(&self.get_flag(place))
    }

    #[inline]
    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        self.inner
            .deserialize(place, &mut &reader[..reader.len() - 1])?;
        let flag = reader[reader.len() - 1];
        self.set_flag(place, flag);
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        self.inner.merge(place, rhs)?;
        let flag = self.get_flag(place) + self.get_flag(rhs);
        self.set_flag(place, flag);
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        let inner_mut: &mut MutableNullableColumn = Series::check_get_mutable_column(array)?;
        if self.get_flag(place) == 0 {
            inner_mut.append_default();
        } else if self.inner_nullable {
            self.inner.merge_result(place, array)?;
        } else {
            self.inner
                .merge_result(place, inner_mut.inner_mut().as_mut())?;
            inner_mut.append_value(true);
        }
        Ok(())
    }

    fn get_own_null_adaptor(
        &self,
        nested_function: AggregateFunctionRef,
        params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<Option<AggregateFunctionRef>> {
        self.inner
            .get_own_null_adaptor(nested_function, params, arguments)
    }

    fn need_manual_drop_state(&self) -> bool {
        self.inner.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        self.inner.drop_state(place)
    }

    fn convert_const_to_full(&self) -> bool {
        self.inner.convert_const_to_full()
    }
}
impl fmt::Display for AggregateFunctionOrNullAdaptor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}
