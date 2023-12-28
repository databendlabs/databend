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

use ethnum::i256;

use super::payload::Payload;
use super::probe_state::ProbeState;
use crate::load;
use crate::types::decimal::DecimalType;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::TimestampType;
use crate::with_number_mapped_type;
use crate::Column;
use crate::StateAddr;

const FLUSH_BATCH_SIZE: usize = 8192;

#[derive(Default)]
pub struct PayloadFlushState {
    pub probe_state: ProbeState,
    pub group_hashes: Vec<u64>,
    pub group_columns: Vec<Column>,
    pub aggregate_results: Vec<Column>,
    pub row_count: usize,
    pub flush_offset: usize,

    pub addresses: Vec<*const u8>,
    pub state_places: Vec<StateAddr>,
}

impl Payload {
    pub fn flush(&self, state: &mut PayloadFlushState) -> bool {
        let flush_end = (state.flush_offset + FLUSH_BATCH_SIZE).min(self.len());

        let rows = flush_end - state.flush_offset;
        if rows == 0 {
            return false;
        }

        if state.row_count < rows {
            state.group_hashes.resize(rows, 0);
            state.addresses.resize(rows, std::ptr::null::<u8>());
            state.state_places.resize(rows, StateAddr::new(0));
        }

        state.group_columns.clear();
        state.row_count = rows;
        state.probe_state.adjust_row_count(rows);

        for row in state.flush_offset..flush_end {
            state.addresses[row - state.flush_offset] = self.get_row_ptr(row);
        }

        self.flush_hashes(state);
        for col_index in 0..self.group_types.len() {
            let col = self.flush_column(col_index, state);
            state.group_columns.push(col);
        }

        for i in 0..rows {
            state.state_places[i] = unsafe {
                StateAddr::new(load::<u64>(state.addresses[i].add(self.state_offset)) as usize)
            };
        }

        state.flush_offset = flush_end;
        true
    }

    fn flush_hashes(&self, state: &mut PayloadFlushState) {
        let len = state.probe_state.row_count;

        for i in 0..len {
            state.group_hashes[i] =
                unsafe { load::<u64>(state.addresses[i].add(self.hash_offset)) };
        }
    }

    fn flush_column(&self, col_index: usize, state: &mut PayloadFlushState) -> Column {
        let len = state.probe_state.row_count;

        let col_offset = self.group_offsets[col_index];
        let col = match self.group_types[col_index].remove_nullable() {
            DataType::Null => Column::Null { len },
            DataType::EmptyArray => Column::EmptyArray { len },
            DataType::EmptyMap => Column::EmptyMap { len },
            DataType::Boolean => self.flush_type_column::<BooleanType>(col_offset, state),
            DataType::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
                NumberDataType::NUM_TYPE =>
                    self.flush_type_column::<NumberType<NUM_TYPE>>(col_offset, state),
            }),
            DataType::Decimal(v) => match v {
                crate::types::DecimalDataType::Decimal128(_) => {
                    self.flush_type_column::<DecimalType<i128>>(col_offset, state)
                }
                crate::types::DecimalDataType::Decimal256(_) => {
                    self.flush_type_column::<DecimalType<i256>>(col_offset, state)
                }
            },
            DataType::Timestamp => self.flush_type_column::<TimestampType>(col_offset, state),
            DataType::Date => self.flush_type_column::<DateType>(col_offset, state),
            DataType::Binary => Column::Binary(self.flush_string_column(col_offset, state)),
            DataType::String => Column::String(self.flush_string_column(col_offset, state)),
            DataType::Bitmap => Column::Bitmap(self.flush_string_column(col_offset, state)),
            DataType::Variant => Column::Variant(self.flush_string_column(col_offset, state)),
            DataType::Nullable(_) => unreachable!(),
            DataType::Array(_) => todo!(),
            DataType::Map(_) => todo!(),
            DataType::Tuple(_) => todo!(),
            DataType::Generic(_) => unreachable!(),
        };

        let validity_offset = self.validity_offsets[col_index];
        if self.group_types[col_index].is_nullable() {
            let b = self.flush_type_column::<BooleanType>(validity_offset, state);
            let validity = b.into_boolean().unwrap();

            Column::Nullable(Box::new(NullableColumn {
                column: col,
                validity,
            }))
        } else {
            col
        }
    }

    fn flush_type_column<T: ArgType>(
        &self,
        col_offset: usize,
        state: &mut PayloadFlushState,
    ) -> Column {
        let len = state.probe_state.row_count;
        let iter =
            (0..len).map(|idx| unsafe { load::<T::Scalar>(state.addresses[idx].add(col_offset)) });
        let col = T::column_from_iter(iter, &[]);
        T::upcast_column(col)
    }

    fn flush_string_column(
        &self,
        col_offset: usize,
        state: &mut PayloadFlushState,
    ) -> StringColumn {
        let len = state.probe_state.row_count;
        let mut string_builder = StringColumnBuilder::with_capacity(len, len * 4);

        unsafe {
            for idx in 0..len {
                let str_len = load::<u32>(state.addresses[idx].add(col_offset)) as usize;
                let data_address =
                    load::<u64>(state.addresses[idx].add(col_offset + 4)) as usize as *const u8;

                let scalar = std::slice::from_raw_parts(data_address, str_len);

                string_builder.put_slice(scalar);
                string_builder.commit_row();
            }
        }
        string_builder.build()
    }
}
