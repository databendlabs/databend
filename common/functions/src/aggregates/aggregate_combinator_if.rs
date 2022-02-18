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
use common_arrow::arrow::bitmap::Bitmap;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionCreator;
use crate::aggregates::aggregate_function_factory::CombinatorDescription;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddrs;

#[derive(Clone)]
pub struct AggregateIfCombinator {
    name: String,
    argument_len: usize,
    nested_name: String,
    nested: AggregateFunctionRef,
}

impl AggregateIfCombinator {
    pub fn try_create(
        nested_name: &str,
        params: Vec<DataValue>,
        arguments: Vec<DataField>,
        nested_creator: &AggregateFunctionCreator,
    ) -> Result<AggregateFunctionRef> {
        let name = format!("IfCombinator({})", nested_name);
        let argument_len = arguments.len();

        if argument_len == 0 {
            return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                "{} expect to have more than one argument",
                name
            )));
        }

        match arguments[argument_len - 1].data_type().data_type_id() {
            TypeID::Boolean => {}
            other => {
                return Err(ErrorCode::BadArguments(format!(
                    "The type of the last argument for {} must be boolean type, but got {:?}",
                    name, other
                )));
            }
        }

        let nested_arguments = &arguments[0..argument_len - 1];
        let nested = nested_creator(nested_name, params, nested_arguments.to_vec())?;

        Ok(Arc::new(AggregateIfCombinator {
            name,
            argument_len,
            nested_name: nested_name.to_owned(),
            nested,
        }))
    }

    pub fn combinator_desc() -> CombinatorDescription {
        CombinatorDescription::creator(Box::new(Self::try_create))
    }
}

impl AggregateFunction for AggregateIfCombinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        self.nested.return_type()
    }

    fn init_state(&self, place: StateAddr) {
        self.nested.init_state(place);
    }

    fn state_layout(&self) -> Layout {
        self.nested.state_layout()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        if columns.is_empty() {
            return Ok(());
        };

        let predicate: &BooleanColumn = Series::check_get(&columns[self.argument_len - 1])?;
        let bitmap = combine_validities(validity, Some(predicate.values()));
        self.nested.accumulate(
            place,
            &columns[0..self.argument_len - 1],
            bitmap.as_ref(),
            input_rows,
        )
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        _input_rows: usize,
    ) -> Result<()> {
        let predicate: &BooleanColumn = Series::check_get(&columns[self.argument_len - 1])?;

        let (columns, row_size) = self.filter_column(&columns[0..self.argument_len - 1], predicate);
        let new_places = Self::filter_place(places, predicate);

        let new_places_slice = new_places.as_slice();
        self.nested
            .accumulate_keys(new_places_slice, offset, &columns, row_size)
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[ColumnRef], row: usize) -> Result<()> {
        let predicate: &BooleanColumn = Series::check_get(&columns[self.argument_len - 1])?;
        if predicate.values().get_bit(row) {
            self.nested
                .accumulate_row(place, &columns[0..self.argument_len - 1], row)?;
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        self.nested.serialize(place, writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        self.nested.deserialize(place, reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        self.nested.merge(place, rhs)
    }

    fn merge_result(&self, place: StateAddr, column: &mut dyn MutableColumn) -> Result<()> {
        self.nested.merge_result(place, column)
    }
}

impl fmt::Display for AggregateIfCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}if", self.nested_name)
    }
}

impl AggregateIfCombinator {
    #[inline]
    fn filter_column(
        &self,
        columns: &[ColumnRef],
        predicate: &BooleanColumn,
    ) -> (Vec<ColumnRef>, usize) {
        let columns = columns
            .iter()
            .map(|c| c.filter(predicate))
            .collect::<Vec<_>>();

        let rows = predicate.len() - predicate.values().null_count();

        (columns, rows)
    }

    fn filter_place(places: &[StateAddr], predicate: &BooleanColumn) -> StateAddrs {
        if predicate.values().null_count() == 0 {
            return places.to_vec();
        }
        let it = predicate
            .values()
            .iter()
            .zip(places.iter())
            .filter(|(v, _)| *v)
            .map(|(_, c)| *c);

        Vec::from_iter(it)
    }
}
