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

use bytes::BytesMut;
use common_datavalues::prelude::Series;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues2::combine_validities;
use common_datavalues2::DataField as NewDataField;
use common_datavalues2::DataValue as NewDataValue;
use common_datavalues2::IntoColumn;
use common_datavalues2::MutableColumn;
use common_exception::Result;

use super::AggregateFunctionRef;
use super::StateAddr;

pub type AggregateFunctionV1Ref = Arc<dyn AggregateFunctionV1>;

/// AggregateFunction
/// In AggregateFunction, all datablock columns are not ConstantColumn, we take the column as Series
pub trait AggregateFunctionV1: fmt::Display + Sync + Send {
    fn name(&self) -> &str;
    fn return_type(&self) -> Result<DataType>;
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool>;

    fn init_state(&self, place: StateAddr);
    fn state_layout(&self) -> Layout;

    // accumulate is to accumulate the arrays in batch mode
    // common used when there is no group by for aggregate function
    fn accumulate(&self, _place: StateAddr, _arrays: &[Series], _input_rows: usize) -> Result<()>;

    // used when we need to calculate with group keys
    fn accumulate_keys(
        &self,
        _places: &[StateAddr],
        _offset: usize,
        _arrays: &[Series],
        _input_rows: usize,
    ) -> Result<()>;

    // serialize  the state into binary array
    fn serialize(&self, _place: StateAddr, _writer: &mut BytesMut) -> Result<()>;

    fn deserialize(&self, _place: StateAddr, _reader: &mut &[u8]) -> Result<()>;

    fn merge(&self, _place: StateAddr, _rhs: StateAddr) -> Result<()>;

    // TODO append the value into the column builder
    fn merge_result(&self, _place: StateAddr, array: &mut dyn MutableColumn) -> Result<()>;
}

#[derive(Clone)]
pub struct AggConvertor {
    inner: AggregateFunctionRef,
    params: Vec<NewDataValue>,
    arguments: Vec<NewDataField>,
}

impl AggConvertor {
    pub fn create(
        inner: AggregateFunctionRef,
        params: Vec<NewDataValue>,
        arguments: Vec<NewDataField>,
    ) -> AggregateFunctionV1Ref {
        Arc::new(Self {
            inner,
            params,
            arguments,
        })
    }
}

impl AggregateFunctionV1 for AggConvertor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn return_type(&self) -> Result<DataType> {
        let typ = self.inner.return_type()?;
        let new_f = NewDataField::new("x", typ);
        let old_f = DataField::from(new_f);

        Ok(old_f.data_type().clone())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        let typ = self.inner.return_type()?;
        let new_f = NewDataField::new("x", typ);
        Ok(new_f.is_nullable())
    }

    fn init_state(&self, place: StateAddr) {
        self.inner.init_state(place);
    }

    fn state_layout(&self) -> Layout {
        self.inner.state_layout()
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], input_rows: usize) -> Result<()> {
        let columns = arrays
            .iter()
            .zip(self.arguments.iter())
            .map(|(s, f)| {
                let arrow_column = s.get_array_ref();
                match f.is_nullable_or_null() {
                    true => arrow_column.into_nullable_column(),
                    false => arrow_column.into_column(),
                }
            })
            .collect::<Vec<_>>();

        let mut bitmap = None;
        arrays.iter().for_each(|s| {
            let b = s.validity();
            bitmap = combine_validities(bitmap.as_ref(), b);
        });
        let bitmap = bitmap.as_ref();
        self.inner.accumulate(place, &columns, bitmap, input_rows)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        arrays: &[Series],
        input_rows: usize,
    ) -> Result<()> {
        let columns = arrays
            .iter()
            .zip(self.arguments.iter())
            .map(|(s, f)| {
                let arrow_column = s.get_array_ref();
                match f.is_nullable_or_null() {
                    true => arrow_column.into_nullable_column(),
                    false => arrow_column.into_column(),
                }
            })
            .collect::<Vec<_>>();

        self.inner
            .accumulate_keys(places, offset, &columns, input_rows)
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        self.inner.serialize(place, writer)
    }

    fn deserialize(&self, _place: StateAddr, _reader: &mut &[u8]) -> Result<()> {
        self.inner.deserialize(_place, _reader)
    }

    fn merge(&self, _place: StateAddr, _rhs: StateAddr) -> Result<()> {
        self.inner.merge(_place, _rhs)
    }

    fn merge_result(&self, place: StateAddr, array: &mut dyn MutableColumn) -> Result<()> {
        self.inner.merge_result(place, array)
    }
}

impl std::fmt::Display for AggConvertor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner.name())
    }
}
