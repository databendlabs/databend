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

use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;

pub struct AggregateFunctionBasicAdaptor {
    inner: AggregateFunctionRef,
}

impl AggregateFunctionBasicAdaptor {
    pub fn create(inner: AggregateFunctionRef) -> AggregateFunctionRef {
        Arc::new(AggregateFunctionBasicAdaptor { inner })
    }
}

impl AggregateFunction for AggregateFunctionBasicAdaptor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        self.inner.return_type()
    }

    #[inline]
    fn init_state(&self, place: StateAddr) {
        self.inner.init_state(place)
    }

    #[inline]
    fn state_layout(&self) -> std::alloc::Layout {
        self.inner.state_layout()
    }

    #[inline]
    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
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
                .accumulate_keys(places, offset, &columns, input_rows)
        } else {
            self.inner
                .accumulate_keys(places, offset, columns, input_rows)
        }
    }

    #[inline]
    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        self.inner.accumulate_row(place, columns, row)
    }

    #[inline]
    fn serialize(&self, place: StateAddr, writer: &mut bytes::BytesMut) -> Result<()> {
        self.inner.serialize(place, writer)
    }

    #[inline]
    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        self.inner.deserialize(place, reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        self.inner.merge(place, rhs)
    }

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        self.inner.merge_result(place, array)
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
}
impl fmt::Display for AggregateFunctionBasicAdaptor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}
