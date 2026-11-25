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

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_io::prelude::bincode_deserialize_from_slice;

use super::AggrState;
use super::BATCH_SIZE;
use super::StateAddr;
use super::aggregate_function::AggregateStateSet;
use super::partitioned_payload::PartitionedPayload;
use super::payload::Payload;
use super::payload::merge_flushed_states;
use super::payload_row::RowTypeVisitor;
use super::payload_row::visit_row_type;
use super::probe_state::ProbeState;
use super::row_ptr::RowPtr;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Scalar;
use crate::types::AccessType;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::ReturnType;
use crate::types::binary::BinaryColumn;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalType;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;

pub struct PayloadFlushState {
    pub(super) probe_state: Box<ProbeState>,
    pub(super) group_columns: Vec<BlockEntry>,
    pub(super) aggregate_results: Vec<BlockEntry>,
    pub(super) row_count: usize,

    pub(super) flush_page: usize,
    pub(super) flush_page_row: usize,

    pub(super) addresses: [RowPtr; BATCH_SIZE],
    pub(super) state_places: [StateAddr; BATCH_SIZE],
}

impl Default for PayloadFlushState {
    fn default() -> Self {
        PayloadFlushState {
            probe_state: ProbeState::new_boxed(),
            group_columns: Vec::new(),
            aggregate_results: Vec::new(),
            row_count: 0,
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

/// Builds a group column out of `rows`, from the values at `offset` in each row.
#[derive(Clone, Copy)]
struct FlushColumn<'a> {
    rows: &'a [RowPtr],
    offset: usize,
}

impl FlushColumn<'_> {
    fn fixed<T: ArgType>(self) -> Column {
        let iter = self
            .rows
            .iter()
            .map(|row| unsafe { row.read::<T::Scalar>(self.offset) });
        T::upcast_column(T::column_from_iter(iter, &[]))
    }

    fn decimal<Num: Decimal>(self, decimal_size: DecimalSize) -> Column {
        let iter = self.rows.iter().map(|row| unsafe {
            row.read::<<DecimalType<Num> as AccessType>::Scalar>(self.offset)
        });
        let col = DecimalType::<Num>::column_from_iter(iter, &[]);
        Num::upcast_column(col, decimal_size)
    }

    fn binary(self) -> BinaryColumn {
        let len = self.rows.len();
        let mut builder = BinaryColumnBuilder::with_capacity(len, len * 4);
        for row in self.rows {
            builder.put_slice(unsafe { row.read_bytes(self.offset) });
            builder.commit_row();
        }
        builder.build()
    }

    fn string(self) -> StringColumn {
        let mut builder = StringColumnBuilder::with_capacity(self.rows.len());
        for row in self.rows {
            builder.put_and_commit(unsafe {
                std::str::from_utf8_unchecked(row.read_bytes(self.offset))
            });
        }
        builder.build()
    }

    fn generic(self, data_type: &DataType) -> Column {
        let mut builder = ColumnBuilder::with_capacity(data_type, self.rows.len());
        for row in self.rows {
            let scalar: Scalar =
                bincode_deserialize_from_slice(unsafe { row.read_bytes(self.offset) }).unwrap();
            builder.push(scalar.as_ref());
        }
        builder.build()
    }
}

impl RowTypeVisitor for FlushColumn<'_> {
    type Output = Column;

    fn visit_empty(self, data_type: &DataType) -> Column {
        let len = self.rows.len();
        match data_type {
            DataType::Null => Column::Null { len },
            DataType::EmptyArray => Column::EmptyArray { len },
            DataType::EmptyMap => Column::EmptyMap { len },
            _ => unreachable!(),
        }
    }

    fn visit_fixed<T: ArgType>(self) -> Column {
        self.fixed::<T>()
    }

    fn visit_decimal<Num: Decimal>(self, size: DecimalSize) -> Column {
        self.decimal::<Num>(size)
    }

    fn visit_bytes(self, data_type: &DataType) -> Column {
        match data_type {
            DataType::String => Column::String(self.string()),
            DataType::Binary => Column::Binary(self.binary()),
            DataType::Bitmap => Column::Bitmap(self.binary()),
            DataType::Variant => Column::Variant(self.binary()),
            DataType::Geometry => Column::Geometry(self.binary()),
            _ => unreachable!(),
        }
    }

    fn visit_serialized(self, data_type: &DataType) -> Column {
        self.generic(data_type)
    }
}

impl PartitionedPayload {
    pub fn aggregate_flush_all(&self) -> Result<DataBlock> {
        let mut blocks = Vec::with_capacity(self.partition_count());

        for payload in self.payloads.iter() {
            if payload.len() != 0 {
                blocks.push(payload.aggregate_flush_all()?);
            }
        }

        if blocks.is_empty() {
            return Ok(self.payloads[0].empty_block());
        }
        DataBlock::concat(&blocks)
    }
}

impl Payload {
    pub fn aggregate_flush_all(&self) -> Result<DataBlock> {
        // A PayloadFlushState is over 100KB, do not allocate one for an empty payload.
        if self.len() == 0 {
            return Ok(self.empty_block());
        }

        let mut state = PayloadFlushState::default();
        self.aggregate_flush_all_with_state(&mut state)
    }

    /// Same as `aggregate_flush_all`, but reuses the caller's flush state, so that
    /// flushing many payloads does not allocate a new state for each of them.
    pub fn aggregate_flush_all_with_state(
        &self,
        state: &mut PayloadFlushState,
    ) -> Result<DataBlock> {
        state.clear();
        let mut blocks = vec![];

        while let Some(block) = self.aggregate_flush(state)? {
            blocks.push(block);
        }

        if blocks.is_empty() {
            return Ok(self.empty_block());
        }
        DataBlock::concat(&blocks)
    }

    fn aggregate_flush(&self, state: &mut PayloadFlushState) -> Result<Option<DataBlock>> {
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
                func.serialize(
                    AggregateStateSet::new(&state.state_places.as_slice()[0..row_count], loc),
                    builders,
                )?;
            }

            entries.extend(builders.into_iter().map(|builder| builder.build().into()));
        }

        entries.extend_from_slice(&state.take_group_columns());
        Ok(Some(DataBlock::new(entries, row_count)))
    }

    pub(super) fn flush(&self, flush_state: &mut PayloadFlushState) -> bool {
        if !self.scan_rows(flush_state) {
            return false;
        }

        flush_state.group_columns.clear();
        for col_index in 0..self.group_types.len() {
            let rows = &flush_state.addresses[..flush_state.row_count];
            let col = self.flush_column(col_index, rows);
            flush_state.group_columns.push(col.into());
        }
        true
    }

    /// Finalizes the next rows like `merge_result`, but only the rows that `select` keeps.
    ///
    /// `select` gets the columns of the result for all the rows of a batch on demand, see
    /// [`BatchResultColumns`], and returns the rows to keep, or `None` to keep them all. Only
    /// the kept rows have their full result built, so a top N builds it for the rows that can
    /// still rank within it. A batch that keeps no row is skipped. The states of the dropped
    /// rows stay in the payload, which releases them as usual.
    pub fn merge_selected_result(
        &self,
        flush_state: &mut PayloadFlushState,
        mut select: impl FnMut(&BatchResultColumns) -> Result<Option<Bitmap>>,
    ) -> Result<Option<DataBlock>> {
        loop {
            if !self.scan_rows(flush_state) {
                return Ok(None);
            }
            let keep = select(&BatchResultColumns {
                payload: self,
                flush_state,
            })?;
            if let Some(keep) = keep {
                let mut kept = 0;
                for (row, _) in keep.iter().enumerate().filter(|(_, keep)| *keep) {
                    flush_state.addresses[kept] = flush_state.addresses[row];
                    flush_state.state_places[kept] = flush_state.state_places[row];
                    kept += 1;
                }
                if kept == 0 {
                    continue;
                }
                flush_state.row_count = kept;
            }

            flush_state.group_columns.clear();
            for col_index in 0..self.group_types.len() {
                let rows = &flush_state.addresses[..flush_state.row_count];
                let col = self.flush_column(col_index, rows);
                flush_state.group_columns.push(col.into());
            }
            merge_flushed_states(&self.row_layout, &self.aggrs, flush_state)?;
            let mut entries = flush_state.take_aggregate_results();
            entries.extend(flush_state.take_group_columns());
            return Ok(Some(DataBlock::new(entries, flush_state.row_count)));
        }
    }

    /// Scans the next rows like `flush`, without building their group columns.
    pub(super) fn scan_rows(&self, flush_state: &mut PayloadFlushState) -> bool {
        let Some(page) = self.pages.get(flush_state.flush_page) else {
            return false;
        };

        if flush_state.flush_page_row >= page.rows {
            flush_state.flush_page += 1;
            flush_state.flush_page_row = 0;
            flush_state.row_count = 0;

            return self.scan_rows(flush_state);
        }

        let end = (flush_state.flush_page_row + BATCH_SIZE).min(page.rows);
        let rows = end - flush_state.flush_page_row;
        flush_state.row_count = rows;
        let state = &mut *flush_state.probe_state;

        for (idx, row_ptr) in flush_state.addresses[..rows].iter_mut().enumerate() {
            *row_ptr = self.data_ptr(page, idx + flush_state.flush_page_row);
            state.group_hashes[idx] = row_ptr.hash(&self.row_layout);

            if !self.aggrs.is_empty() {
                flush_state.state_places[idx] = row_ptr.state_addr(&self.row_layout);
            }
        }

        flush_state.flush_page_row = end;
        true
    }

    fn flush_column(&self, col_index: usize, rows: &[RowPtr]) -> Column {
        let col = visit_row_type(
            &self.group_types[col_index].remove_nullable(),
            FlushColumn {
                rows,
                offset: self.row_layout.group_offsets[col_index],
            },
        );

        if self.group_types[col_index].is_nullable() {
            let validity = FlushColumn {
                rows,
                offset: self.row_layout.validity_offsets[col_index],
            }
            .fixed::<BooleanType>()
            .into_boolean()
            .unwrap();
            col.wrap_nullable(Some(validity))
        } else {
            col
        }
    }
}

/// The columns of the result of a batch of rows that `Payload::merge_selected_result` scanned,
/// built on demand, for all the rows of the batch.
pub struct BatchResultColumns<'a> {
    payload: &'a Payload,
    flush_state: &'a PayloadFlushState,
}

impl BatchResultColumns<'_> {
    pub fn num_rows(&self) -> usize {
        self.flush_state.row_count
    }

    /// The type of `column(index)`, without building it.
    pub fn data_type(&self, index: usize) -> DataType {
        let payload = self.payload;
        match index.checked_sub(payload.aggrs.len()) {
            Some(group_index) => payload.group_types[group_index].clone(),
            None => payload.aggrs[index].signature().return_type.clone(),
        }
    }

    /// Column `index` of the result in the layout of `Payload::merge_result`: the results of
    /// the aggregates, then the group columns. The results are computed without consuming the
    /// states, which the result of the kept rows consumes later.
    pub fn column(&self, index: usize) -> Result<Column> {
        let payload = self.payload;
        let rows = self.flush_state.row_count;
        let Some(group_index) = index.checked_sub(payload.aggrs.len()) else {
            let states_layout = payload.row_layout.states_layout.as_ref().unwrap();
            let (aggr, loc) = (&payload.aggrs[index], &states_layout.states_loc[index]);
            let mut builder = ColumnBuilder::with_capacity(&aggr.signature().return_type, rows);
            for place in &self.flush_state.state_places[..rows] {
                aggr.merge_result_read_only(AggrState::new(*place, loc), &mut builder)?;
            }
            return Ok(builder.build());
        };
        Ok(payload.flush_column(group_index, &self.flush_state.addresses[..rows]))
    }
}
