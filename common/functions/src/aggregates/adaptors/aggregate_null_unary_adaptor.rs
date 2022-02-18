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
use common_exception::Result;
use common_io::prelude::BinaryRead;
use common_io::prelude::BinaryWriteBuf;

use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;

#[derive(Clone)]
pub struct AggregateNullUnaryAdaptor<const NULLABLE_RESULT: bool> {
    nested: AggregateFunctionRef,
    prefix_size: usize,
}

impl<const NULLABLE_RESULT: bool> AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    pub fn create(nested: AggregateFunctionRef) -> AggregateFunctionRef {
        let prefix_size = if NULLABLE_RESULT {
            let layout = nested.state_layout();
            layout.align()
        } else {
            0
        };
        Arc::new(Self {
            nested,
            prefix_size,
        })
    }

    #[inline]
    pub fn set_flag(place: StateAddr) {
        if NULLABLE_RESULT {
            let c = place.get::<u8>();
            *c = 1;
        }
    }

    #[inline]
    pub fn init_flag(place: StateAddr) {
        if NULLABLE_RESULT {
            let c = place.get::<u8>();
            *c = 0;
        }
    }

    #[inline]
    pub fn get_flag(place: StateAddr) -> u8 {
        if NULLABLE_RESULT {
            let c = place.get::<u8>();
            *c
        } else {
            1
        }
    }

    #[inline]
    pub fn nested_place(&self, place: StateAddr) -> StateAddr {
        place.next(self.prefix_size)
    }
}

impl<const NULLABLE_RESULT: bool> AggregateFunction for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn name(&self) -> &str {
        "AggregateNullUnaryAdaptor"
    }

    fn return_type(&self) -> Result<DataTypePtr> {
        match NULLABLE_RESULT {
            true => Ok(wrap_nullable(&self.nested.return_type()?)),
            false => Ok(self.nested.return_type()?),
        }
    }

    #[inline]
    fn init_state(&self, place: StateAddr) {
        Self::init_flag(place);
        self.nested.init_state(self.nested_place(place));
    }

    #[inline]
    fn state_layout(&self) -> Layout {
        let layout = self.nested.state_layout();
        Layout::from_size_align(layout.size() + self.prefix_size, layout.align()).unwrap()
    }

    #[inline]
    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[ColumnRef],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let mut validity = validity.cloned();
        let col = &columns[0];
        let (all_null, v) = col.validity();
        validity = combine_validities(validity.as_ref(), v);
        let not_null_columns = Series::remove_nullable(col);

        self.nested.accumulate(
            self.nested_place(place),
            &[not_null_columns],
            validity.as_ref(),
            input_rows,
        )?;

        if !all_null {
            match validity {
                Some(v) => {
                    if v.null_count() != input_rows {
                        Self::set_flag(place);
                    }
                }
                None => Self::set_flag(place),
            }
        }
        Ok(())
    }

    #[inline]
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[ColumnRef],
        input_rows: usize,
    ) -> Result<()> {
        let col = &columns[0];

        let (all_null, validity) = col.validity();
        let not_null_column = Series::remove_nullable(col);

        self.nested.accumulate_keys(
            places,
            offset + self.prefix_size,
            &[not_null_column],
            input_rows,
        )?;

        if !all_null {
            match validity {
                Some(v) => v.iter().zip(places.iter()).for_each(|(valid, place)| {
                    if valid {
                        Self::set_flag(place.next(offset));
                    }
                }),
                None => places
                    .iter()
                    .for_each(|place| Self::set_flag(place.next(offset))),
            }
        }
        Ok(())
    }

    /// we already have accumulate_keys, so we don't need to implement this
    fn accumulate_row(&self, _place: StateAddr, _columns: &[ColumnRef], _row: usize) -> Result<()> {
        unreachable!()
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        if NULLABLE_RESULT {
            let flag = Self::get_flag(place) == 1;
            writer.write_scalar(&flag)?;
        }
        self.nested.serialize(self.nested_place(place), writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        if NULLABLE_RESULT {
            let flag: bool = reader.read_scalar()?;
            if flag {
                Self::set_flag(place);
            }
        }

        self.nested.deserialize(self.nested_place(place), reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        if Self::get_flag(place) == 0 {
            // initial the state to remove the dirty stats
            self.init_state(place);
        }

        if Self::get_flag(rhs) == 1 {
            Self::set_flag(place);
        }
        self.nested
            .merge(self.nested_place(place), self.nested_place(rhs))
    }

    fn merge_result(&self, place: StateAddr, column: &mut dyn MutableColumn) -> Result<()> {
        if NULLABLE_RESULT {
            let builder: &mut MutableNullableColumn = Series::check_get_mutable_column(column)?;
            if Self::get_flag(place) == 1 {
                let inner = builder.inner_mut();
                self.nested
                    .merge_result(self.nested_place(place), inner.as_mut())?;

                let validity = builder.validity_mut();

                validity.push(true);
            } else {
                builder.append_default();
            }
            Ok(())
        } else {
            self.nested.merge_result(self.nested_place(place), column)
        }
    }
}

impl<const NULLABLE_RESULT: bool> fmt::Display for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggregateNullUnaryAdaptor")
    }
}
