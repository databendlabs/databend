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

use databend_common_exception::Result;
use databend_common_io::prelude::bincode_deserialize_from_slice;

use super::partitioned_payload::PartitionedPayload;
use super::payload::Payload;
use super::probe_state::ProbeState;
use super::row_ptr::RowPtr;
use crate::types::binary::BinaryColumn;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::bitmap::BitmapColumn;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalType;
use crate::types::i256;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::AccessType;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalDataKind;
use crate::types::DecimalSize;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::ReturnType;
use crate::types::TimestampType;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Scalar;
use crate::StateAddr;
use crate::BATCH_SIZE;

pub struct PayloadFlushState {
    pub probe_state: Box<ProbeState>,
    pub group_columns: Vec<BlockEntry>,
    pub(super) aggregate_results: Vec<BlockEntry>,
    pub row_count: usize,

    pub flush_partition: usize,
    pub flush_page: usize,
    pub flush_page_row: usize,

    pub addresses: [RowPtr; BATCH_SIZE],
    pub state_places: [StateAddr; BATCH_SIZE],
}

impl Default for PayloadFlushState {
    fn default() -> Self {
        PayloadFlushState {
            probe_state: ProbeState::new_boxed(),
            group_columns: Vec::new(),
            aggregate_results: Vec::new(),
            row_count: 0,
            flush_partition: 0,
            flush_page: 0,
            flush_page_row: 0,
            addresses: [RowPtr::null(); BATCH_SIZE],
            state_places: [StateAddr::null(); BATCH_SIZE],
        }
    }
}

unsafe impl Send for PayloadFlushState {}
unsafe impl Sync for PayloadFlushState {}

impl PayloadFlushState {
    pub fn clear(&mut self) {
        self.row_count = 0;
        self.flush_partition = 0;
        self.flush_page = 0;
        self.flush_page_row = 0;
    }

    pub fn take_group_columns(&mut self) -> Vec<BlockEntry> {
        std::mem::take(&mut self.group_columns)
    }
    pub fn take_aggregate_results(&mut self) -> Vec<BlockEntry> {
        std::mem::take(&mut self.aggregate_results)
    }
}

impl PartitionedPayload {
    pub fn flush(&mut self, state: &mut PayloadFlushState) -> bool {
        if state.flush_partition >= self.payloads.len() {
            return false;
        }

        let p = &self.payloads[state.flush_partition];
        if p.flush(state) {
            true
        } else {
            let partition_idx = state.flush_partition + 1;
            state.clear();
            state.flush_partition = partition_idx;
            self.flush(state)
        }
    }
}

impl Payload {
    pub fn aggregate_flush_all(&self) -> Result<DataBlock> {
        let mut state = PayloadFlushState::default();
        let mut blocks = vec![];

        while let Some(block) = self.aggregate_flush(&mut state)? {
            blocks.push(block);
        }

        if blocks.is_empty() {
            return Ok(self.empty_block(0));
        }
        DataBlock::concat(&blocks)
    }

    pub fn aggregate_flush(&self, state: &mut PayloadFlushState) -> Result<Option<DataBlock>> {
        if !self.flush(state) {
            return Ok(None);
        }

        let row_count = state.row_count;

        let mut entries = Vec::with_capacity(self.aggrs.len() + self.group_types.len());
        if let Some(state_layout) = self.row_layout.states_layout.as_ref() {
            let mut builders = state_layout.serialize_builders(row_count);

            for ((loc, func), builder) in state_layout
                .states_loc
                .iter()
                .zip(self.aggrs.iter())
                .zip(builders.iter_mut())
            {
                let builders = builder.as_tuple_mut().unwrap().as_mut_slice();
                func.batch_serialize(&state.state_places.as_slice()[0..row_count], loc, builders)?;
            }

            entries.extend(builders.into_iter().map(|builder| builder.build().into()));
        }

        entries.extend_from_slice(&state.take_group_columns());
        Ok(Some(DataBlock::new(entries, row_count)))
    }

    pub fn group_by_flush_all(&self) -> Result<DataBlock> {
        let mut state = PayloadFlushState::default();
        let mut blocks = vec![];

        while self.flush(&mut state) {
            let entries = state.take_group_columns();
            blocks.push(DataBlock::new(entries, state.row_count));
        }

        if blocks.is_empty() {
            return Ok(self.empty_block(0));
        }

        DataBlock::concat(&blocks)
    }

    pub fn flush(&self, flush_state: &mut PayloadFlushState) -> bool {
        let Some(page) = self.pages.get(flush_state.flush_page) else {
            return false;
        };

        if flush_state.flush_page_row >= page.rows {
            flush_state.flush_page += 1;
            flush_state.flush_page_row = 0;
            flush_state.row_count = 0;

            return self.flush(flush_state);
        }

        let end = (flush_state.flush_page_row + BATCH_SIZE).min(page.rows);
        let rows = end - flush_state.flush_page_row;
        flush_state.group_columns.clear();
        flush_state.row_count = rows;
        let state = &mut *flush_state.probe_state;
        state.row_count = rows;

        for (idx, row_ptr) in flush_state.addresses[..rows].iter_mut().enumerate() {
            *row_ptr = self.data_ptr(page, idx + flush_state.flush_page_row);
            state.group_hashes[idx] = row_ptr.hash(&self.row_layout);

            if !self.aggrs.is_empty() {
                flush_state.state_places[idx] = row_ptr.state_addr(&self.row_layout);
            }
        }

        for col_index in 0..self.group_types.len() {
            let col = self.flush_column(col_index, flush_state);
            flush_state.group_columns.push(col.into());
        }

        flush_state.flush_page_row = end;
        true
    }

    fn flush_column(&self, col_index: usize, state: &mut PayloadFlushState) -> Column {
        let len = state.probe_state.row_count;

        let col_offset = self.row_layout.group_offsets[col_index];
        let col = match self.group_types[col_index].remove_nullable() {
            DataType::Null => Column::Null { len },
            DataType::EmptyArray => Column::EmptyArray { len },
            DataType::EmptyMap => Column::EmptyMap { len },
            DataType::Boolean => self.flush_type_column::<BooleanType>(col_offset, state),
            DataType::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
                NumberDataType::NUM_TYPE =>
                    self.flush_type_column::<NumberType<NUM_TYPE>>(col_offset, state),
            }),
            DataType::Decimal(size) => match size.data_kind() {
                DecimalDataKind::Decimal64 => {
                    self.flush_decimal_column::<i64>(col_offset, state, size)
                }
                DecimalDataKind::Decimal128 => {
                    self.flush_decimal_column::<i128>(col_offset, state, size)
                }
                DecimalDataKind::Decimal256 => {
                    self.flush_decimal_column::<i256>(col_offset, state, size)
                }
            },
            DataType::Timestamp => self.flush_type_column::<TimestampType>(col_offset, state),
            DataType::Date => self.flush_type_column::<DateType>(col_offset, state),
            DataType::Binary => Column::Binary(self.flush_binary_column(col_offset, state)),
            DataType::String => Column::String(self.flush_string_column(col_offset, state)),
            DataType::Bitmap => Column::Bitmap(Box::new(
                BitmapColumn::from_binary(self.flush_binary_column(col_offset, state))
                    .expect("valid bitmap column"),
            )),
            DataType::Variant => Column::Variant(self.flush_binary_column(col_offset, state)),
            DataType::Geometry => Column::Geometry(self.flush_binary_column(col_offset, state)),
            DataType::Nullable(_) => unreachable!(),
            other => self.flush_generic_column(&other, col_offset, state),
        };

        let validity_offset = self.row_layout.validity_offsets[col_index];
        if self.group_types[col_index].is_nullable() {
            let b = self.flush_type_column::<BooleanType>(validity_offset, state);
            let validity = b.into_boolean().unwrap();

            NullableColumn::new_column(col, validity)
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
            (0..len).map(|idx| unsafe { state.addresses[idx].read::<T::Scalar>(col_offset) });
        let col = T::column_from_iter(iter, &[]);
        T::upcast_column(col)
    }

    fn flush_decimal_column<Num: Decimal>(
        &self,
        col_offset: usize,
        state: &mut PayloadFlushState,
        decimal_size: DecimalSize,
    ) -> Column {
        let len = state.probe_state.row_count;
        let iter = (0..len).map(|idx| unsafe {
            state.addresses[idx].read::<<DecimalType<Num> as AccessType>::Scalar>(col_offset)
        });
        let col = DecimalType::<Num>::column_from_iter(iter, &[]);
        Num::upcast_column(col, decimal_size)
    }

    fn flush_binary_column(
        &self,
        col_offset: usize,
        state: &mut PayloadFlushState,
    ) -> BinaryColumn {
        let len = state.probe_state.row_count;
        let mut binary_builder = BinaryColumnBuilder::with_capacity(len, len * 4);

        unsafe {
            for idx in 0..len {
                let str_len = state.addresses[idx].read::<u32>(col_offset) as usize;
                let data_address =
                    state.addresses[idx].read::<u64>(col_offset + 4) as usize as *const u8;

                let scalar = std::slice::from_raw_parts(data_address, str_len);

                binary_builder.put_slice(scalar);
                binary_builder.commit_row();
            }
        }
        binary_builder.build()
    }

    fn flush_string_column(
        &self,
        col_offset: usize,
        state: &mut PayloadFlushState,
    ) -> StringColumn {
        let len = state.probe_state.row_count;
        let mut binary_builder = StringColumnBuilder::with_capacity(len);

        unsafe {
            for idx in 0..len {
                let str_len = state.addresses[idx].read::<u32>(col_offset) as usize;
                let data_address =
                    state.addresses[idx].read::<u64>(col_offset + 4) as usize as *const u8;

                let scalar = std::slice::from_raw_parts(data_address, str_len);

                binary_builder.put_and_commit(std::str::from_utf8(scalar).unwrap());
            }
        }
        binary_builder.build()
    }

    fn flush_generic_column(
        &self,
        data_type: &DataType,
        col_offset: usize,
        state: &mut PayloadFlushState,
    ) -> Column {
        let len = state.probe_state.row_count;
        let mut builder = ColumnBuilder::with_capacity(data_type, len);

        unsafe {
            for idx in 0..len {
                let str_len = state.addresses[idx].read::<u32>(col_offset) as usize;
                let data_address =
                    state.addresses[idx].read::<u64>(col_offset + 4) as usize as *const u8;

                let scalar = std::slice::from_raw_parts(data_address, str_len);
                let scalar: Scalar = bincode_deserialize_from_slice(scalar).unwrap();

                builder.push(scalar.as_ref());
            }
        }
        builder.build()
    }
}
