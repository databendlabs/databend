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

use std::fmt;
use std::mem::MaybeUninit;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_base::runtime::drop_guard;
use databend_common_column::types::months_days_micros;
use databend_common_io::prelude::bincode_deserialize_from_slice;
use log::info;
use strength_reduce::StrengthReducedU64;

use super::payload_row::rowformat_size;
use super::payload_row::serialize_column_to_rowformat;
use super::row_ptr::RowLayout;
use super::row_ptr::RowPtr;
use crate::types::decimal::i256;
use crate::types::decimal::DecimalDataKind;
use crate::types::decimal::DecimalScalar;
use crate::types::geography::Geography;
use crate::types::number::NumberDataType;
use crate::types::number::NumberScalar;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::DataType;
use crate::AggrState;
use crate::AggregateFunctionRef;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::PayloadFlushState;
use crate::ProjectedBlock;
use crate::Scalar;
use crate::SelectVector;
use crate::StateAddr;
use crate::StatesLayout;
use crate::BATCH_SIZE;
use crate::MAX_PAGE_SIZE;

// payload layout
// [VALIDITY][GROUPS][HASH][STATE_ADDRS]
// [VALIDITY] is the validity bits of the data columns (including the HASH)
// [GROUPS] is the group data, could be multiple values, fixed size, strings are elsewhere
// [HASH] is the hash data of the groups
// [STATE_ADDRS] is the state_addrs of the aggregate functions, 8 bytes each
pub struct Payload {
    pub arena: Arc<Bump>,
    // if true, the states are moved out of the payload into other payload, and will not be dropped
    pub state_move_out: bool,
    pub group_types: Vec<DataType>,
    pub aggrs: Vec<AggregateFunctionRef>,

    pub pages: Pages,
    pub tuple_size: usize,
    pub row_per_page: usize,

    pub total_rows: usize,

    // Starts from 1, zero means no page allocated
    pub current_write_page: usize,

    pub(super) row_layout: RowLayout,

    // if set, the payload contains at least duplicate rows
    pub min_cardinality: Option<usize>,
}

unsafe impl Send for Payload {}
unsafe impl Sync for Payload {}

pub struct Page {
    pub(crate) data: Vec<MaybeUninit<u8>>,
    pub(crate) rows: usize,
    // state_offset = state_rows * agg_len
    // which mark that the offset to clean the agg states
    pub(crate) state_offsets: usize,
    pub(crate) capacity: usize,
}

impl Page {
    pub fn is_partial_state(&self, agg_len: usize) -> bool {
        self.rows * agg_len != self.state_offsets
    }

    pub(super) fn data_ptr(&self, row: usize, row_size: usize) -> RowPtr {
        RowPtr::new(unsafe { self.data.as_ptr().add(row * row_size) as _ })
    }
}

pub type Pages = Vec<Page>;

struct PageDebugSnapshot {
    index: usize,
    row_count: usize,
    capacity_rows: usize,
    used_bytes: usize,
    capacity_bytes: usize,
    state_offsets: usize,
    state_rows: Option<usize>,
    partial_state: bool,
    rows: Vec<PageRowDebug>,
}

struct StatesLayoutDebug {
    aggr_funcs: usize,
    layout_size: usize,
    layout_align: usize,
}

struct PageRowDebug {
    index: usize,
    hash: u64,
    values: Vec<ValueDebug>,
    state_addr: Option<usize>,
}

enum ValueDebug {
    Null,
    Value(String),
}

impl fmt::Debug for PageDebugSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Page")
            .field("index", &self.index)
            .field("row_count", &self.row_count)
            .field("capacity_rows", &self.capacity_rows)
            .field("used_bytes", &self.used_bytes)
            .field("capacity_bytes", &self.capacity_bytes)
            .field("state_offsets", &self.state_offsets)
            .field("state_rows", &self.state_rows)
            .field("partial_state", &self.partial_state)
            .field_with("rows", |f| {
                if self.rows.is_empty() {
                    return f.write_str("[]");
                }
                f.write_str("[\n")?;
                for row in &self.rows {
                    writeln!(f, "    {row:?},")?;
                }
                f.write_str("]")
            })
            .finish()
    }
}

impl fmt::Debug for PageRowDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Row");
        debug
            .field("index", &self.index)
            .field("hash", &format_args!("0x{hash:016x}", hash = self.hash))
            .field("values", &self.values);

        if let Some(addr) = self.state_addr {
            debug.field("state_addr", &format_args!("0x{addr:016x}"));
        }

        debug.finish()
    }
}

impl fmt::Debug for StatesLayoutDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StatesLayout")
            .field("aggr_funcs", &self.aggr_funcs)
            .field("layout_size", &self.layout_size)
            .field("layout_align", &self.layout_align)
            .finish()
    }
}

impl fmt::Debug for ValueDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueDebug::Null => f.write_str("NULL"),
            ValueDebug::Value(val) => f.write_str(val),
        }
    }
}

impl ValueDebug {
    fn null() -> Self {
        ValueDebug::Null
    }

    fn from_scalar(scalar: Scalar) -> Self {
        ValueDebug::Value(scalar.to_string())
    }
}

impl Payload {
    pub fn new(
        arena: Arc<Bump>,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateFunctionRef>,
        states_layout: Option<StatesLayout>,
    ) -> Self {
        let mut tuple_size = 0;
        let mut validity_offsets = Vec::with_capacity(group_types.len());
        for x in group_types.iter() {
            if x.is_nullable() {
                validity_offsets.push(tuple_size);
                tuple_size += 1;
            } else {
                validity_offsets.push(0);
            }
        }

        let mut group_offsets = Vec::with_capacity(group_types.len());
        let mut group_sizes = Vec::with_capacity(group_types.len());

        for x in group_types.iter() {
            group_offsets.push(tuple_size);
            let size = rowformat_size(x);
            group_sizes.push(size);
            tuple_size += size;
        }

        let hash_offset = tuple_size;

        let hash_size = 8;
        tuple_size += hash_size;

        let state_offset = tuple_size;
        if !aggrs.is_empty() {
            tuple_size += 8;
        }

        let row_per_page = (u16::MAX as usize).min(MAX_PAGE_SIZE / tuple_size).max(1);

        Self {
            arena,
            state_move_out: false,
            pages: vec![],
            current_write_page: 0,
            group_types,
            aggrs,
            tuple_size,
            row_per_page,
            min_cardinality: None,
            total_rows: 0,
            row_layout: RowLayout {
                hash_offset,
                state_offset,
                validity_offsets,
                group_offsets,
                group_sizes,
                states_layout,
            },
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.total_rows
    }

    pub fn clear(&mut self) {
        self.total_rows = 0;
        self.pages.clear();
    }

    #[inline]
    pub fn memory_size(&self) -> usize {
        self.total_rows * self.tuple_size
    }

    pub fn states_layout(&self) -> Option<&StatesLayout> {
        self.row_layout.states_layout.as_ref()
    }

    #[inline]
    pub fn writable_page(&mut self) -> (&mut Page, usize) {
        if self.current_write_page == 0
            || self.pages[self.current_write_page - 1].rows
                == self.pages[self.current_write_page - 1].capacity
        {
            self.current_write_page += 1;
            if self.current_write_page > self.pages.len() {
                let data = Vec::with_capacity(self.row_per_page * self.tuple_size);
                self.pages.push(Page {
                    data,
                    rows: 0,
                    state_offsets: 0,
                    capacity: self.row_per_page,
                });
            }
        }
        (
            &mut self.pages[self.current_write_page - 1],
            self.current_write_page - 1,
        )
    }

    pub(super) fn data_ptr(&self, page: &Page, row: usize) -> RowPtr {
        RowPtr::new(unsafe { page.data.as_ptr().add(row * self.tuple_size) as _ })
    }

    pub(super) fn reserve_append_rows(
        &mut self,
        select_vector: &SelectVector,
        group_hashes: &[u64; BATCH_SIZE],
        address: &mut [RowPtr; BATCH_SIZE],
        page_index: &mut [usize],
        new_group_rows: usize,
        group_columns: ProjectedBlock,
    ) {
        let tuple_size = self.tuple_size;
        let (mut page, mut page_index_value) = self.writable_page();
        for idx in select_vector[..new_group_rows].iter().copied() {
            address[idx] = page.data_ptr(page.rows, tuple_size);
            page_index[idx] = page_index_value;
            page.rows += 1;

            if page.rows == page.capacity {
                (page, page_index_value) = self.writable_page();
            }
        }

        self.append_rows(
            select_vector,
            group_hashes,
            address,
            page_index,
            new_group_rows,
            group_columns,
        )
    }

    fn append_rows(
        &mut self,
        select_vector: &SelectVector,
        group_hashes: &[u64; BATCH_SIZE],
        address: &mut [RowPtr; BATCH_SIZE],
        page_index: &mut [usize],
        new_group_rows: usize,
        group_columns: ProjectedBlock,
    ) {
        let mut write_offset = 0;
        // write validity
        for entry in group_columns.iter() {
            if let Column::Nullable(c) = entry.to_column() {
                let bitmap = c.validity();
                if bitmap.null_count() == 0 || bitmap.null_count() == bitmap.len() {
                    let val: u8 = if bitmap.null_count() == 0 { 1 } else { 0 };
                    // faster path
                    for idx in select_vector.iter().take(new_group_rows).copied() {
                        unsafe {
                            address[idx].write_u8(write_offset, val);
                        }
                    }
                } else {
                    for idx in select_vector.iter().take(new_group_rows).copied() {
                        unsafe {
                            address[idx].write_u8(write_offset, bitmap.get_bit(idx) as u8);
                        }
                    }
                }
                write_offset += 1;
            }
        }

        let mut scratch = vec![];
        for (idx, entry) in group_columns.iter().enumerate() {
            let offset = self.row_layout.group_offsets[idx];
            assert!(write_offset == offset);

            unsafe {
                serialize_column_to_rowformat(
                    &self.arena,
                    &entry.to_column(),
                    select_vector,
                    new_group_rows,
                    address,
                    offset,
                    &mut scratch,
                );
            }
            write_offset += self.row_layout.group_sizes[idx];
        }

        // write group hashes
        debug_assert!(write_offset == self.row_layout.hash_offset);
        for idx in select_vector.iter().take(new_group_rows).copied() {
            address[idx].set_hash(&self.row_layout, group_hashes[idx]);
        }

        debug_assert!(write_offset + 8 == self.row_layout.state_offset);
        if let Some(StatesLayout {
            layout, states_loc, ..
        }) = &self.row_layout.states_layout
        {
            // write states
            let (array_layout, padded_size) = layout.repeat(new_group_rows).unwrap();
            // Bump only allocates but does not drop, so there is no use after free for any item.
            let place = self.arena.alloc_layout(array_layout);
            for (idx, place) in select_vector
                .iter()
                .take(new_group_rows)
                .copied()
                .enumerate()
                .map(|(i, idx)| (idx, unsafe { place.add(padded_size * i) }))
            {
                let place = StateAddr::from(place);
                address[idx].set_state_addr(&self.row_layout, &place);
                let page = &mut self.pages[page_index[idx]];
                for (aggr, loc) in self.aggrs.iter().zip(states_loc.iter()) {
                    aggr.init_state(AggrState::new(place, loc));
                    page.state_offsets += 1;
                }
            }

            #[cfg(debug_assertions)]
            {
                for page in self.pages.iter() {
                    assert_eq!(page.rows * self.aggrs.len(), page.state_offsets);
                }
            }
        }

        self.total_rows += new_group_rows;

        debug_assert_eq!(
            self.total_rows,
            self.pages.iter().map(|x| x.rows).sum::<usize>()
        );
    }

    pub fn combine(&mut self, mut other: Payload) {
        debug_assert_eq!(
            other.total_rows,
            other.pages.iter().map(|x| x.rows).sum::<usize>()
        );

        self.total_rows += other.total_rows;
        self.pages.append(other.pages.as_mut());
    }

    pub fn mark_min_cardinality(&mut self) {
        if self.min_cardinality.is_none() {
            self.min_cardinality = Some(self.total_rows);
        }
    }

    pub fn copy_rows(
        &mut self,
        select_vector: &SelectVector,
        row_count: usize,
        address: &[RowPtr; BATCH_SIZE],
    ) {
        let tuple_size = self.tuple_size;
        let agg_len = self.aggrs.len();
        let (mut page, _) = self.writable_page();
        for i in 0..row_count {
            let index = select_vector[i];

            unsafe {
                std::ptr::copy_nonoverlapping(
                    address[index].as_ptr(),
                    page.data.as_mut_ptr().add(page.rows * tuple_size) as _,
                    tuple_size,
                )
            }
            page.rows += 1;
            page.state_offsets += agg_len;

            if page.rows == page.capacity {
                (page, _) = self.writable_page();
            }
        }

        self.total_rows += row_count;

        debug_assert_eq!(
            self.total_rows,
            self.pages.iter().map(|x| x.rows).sum::<usize>()
        );
    }

    pub fn scatter(&self, state: &mut PayloadFlushState, partition_count: usize) -> bool {
        if state.flush_page >= self.pages.len() {
            return false;
        }

        let page = &self.pages[state.flush_page];

        // ToNext
        if state.flush_page_row >= page.rows {
            state.flush_page += 1;
            state.flush_page_row = 0;
            state.row_count = 0;
            return self.scatter(state, partition_count);
        }

        let end = (state.flush_page_row + BATCH_SIZE).min(page.rows);
        let rows = end - state.flush_page_row;
        state.row_count = rows;

        state.probe_state.reset_partitions(partition_count);

        let mods: StrengthReducedU64 = StrengthReducedU64::new(partition_count as u64);
        for idx in 0..rows {
            state.addresses[idx] = self.data_ptr(page, idx + state.flush_page_row);

            let hash = state.addresses[idx].hash(&self.row_layout);

            let partition_idx = (hash % mods) as usize;

            let sel = &mut state.probe_state.partition_entries[partition_idx];
            sel[state.probe_state.partition_count[partition_idx]] = idx;
            state.probe_state.partition_count[partition_idx] += 1;
        }
        state.flush_page_row = end;
        true
    }

    pub fn empty_block(&self, fake_rows: usize) -> DataBlock {
        assert_eq!(
            self.aggrs.is_empty(),
            self.row_layout.states_layout.is_none()
        );
        let entries = self
            .row_layout
            .states_layout
            .as_ref()
            .iter()
            .flat_map(|layout| layout.serialize_type.iter())
            .map(|serde_type| {
                ColumnBuilder::repeat_default(&serde_type.data_type(), fake_rows)
                    .build()
                    .into()
            })
            .chain(
                self.group_types
                    .iter()
                    .map(|t| ColumnBuilder::repeat_default(t, fake_rows).build().into()),
            )
            .collect();
        DataBlock::new(entries, fake_rows)
    }

    fn debug_page_rows(&self, page: &Page) -> Vec<PageRowDebug> {
        (0..page.rows)
            .map(|row_idx| {
                let row_ptr = self.data_ptr(page, row_idx);
                let values = (0..self.group_types.len())
                    .map(|column_idx| self.debug_group_value(row_ptr, column_idx))
                    .collect();
                let state_addr = self
                    .row_layout
                    .states_layout
                    .as_ref()
                    .map(|_| row_ptr.state_addr(&self.row_layout).addr())
                    .filter(|addr| *addr != 0);

                PageRowDebug {
                    index: row_idx,
                    hash: row_ptr.hash(&self.row_layout),
                    values,
                    state_addr,
                }
            })
            .collect()
    }

    fn debug_group_value(&self, row: RowPtr, column_idx: usize) -> ValueDebug {
        let offset = self.row_layout.group_offsets[column_idx];
        match &self.group_types[column_idx] {
            DataType::Nullable(inner) => {
                let validity_offset = self.row_layout.validity_offsets[column_idx];
                let is_valid = unsafe { row.read::<u8>(validity_offset) != 0 };
                if is_valid {
                    ValueDebug::from_scalar(decode_scalar(row, inner, offset))
                } else {
                    ValueDebug::null()
                }
            }
            data_type => ValueDebug::from_scalar(decode_scalar(row, data_type, offset)),
        }
    }
}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let agg_len = self.aggrs.len();
        let tuple_size = self.tuple_size;

        let pages: Vec<PageDebugSnapshot> = self
            .pages
            .iter()
            .enumerate()
            .map(|(index, page)| {
                let state_rows = if agg_len == 0 {
                    None
                } else {
                    Some(page.state_offsets / agg_len)
                };
                let rows = self.debug_page_rows(page);
                PageDebugSnapshot {
                    index,
                    row_count: page.rows,
                    capacity_rows: page.capacity,
                    used_bytes: page.rows * tuple_size,
                    capacity_bytes: page.capacity * tuple_size,
                    state_offsets: page.state_offsets,
                    state_rows,
                    partial_state: page.is_partial_state(agg_len),
                    rows,
                }
            })
            .collect();

        let group_types: Vec<String> = self.group_types.iter().map(|ty| ty.to_string()).collect();
        let aggregate_functions: Vec<String> = self
            .aggrs
            .iter()
            .map(|func| func.name().to_string())
            .collect();
        let states_layout = self.states_layout().map(|layout| StatesLayoutDebug {
            aggr_funcs: layout.num_aggr_func(),
            layout_size: layout.layout.size(),
            layout_align: layout.layout.align(),
        });

        let mut debug = f.debug_struct("Payload");
        debug
            .field("rows", &self.total_rows)
            .field("tuple_size", &self.tuple_size)
            .field("row_per_page", &self.row_per_page)
            .field("current_write_page", &self.current_write_page)
            .field("state_move_out", &self.state_move_out)
            .field("min_cardinality", &self.min_cardinality)
            .field("group_types", &group_types)
            .field("aggregate_functions", &aggregate_functions)
            .field("pages", &pages)
            .field(
                "arena_addr",
                &format_args!("{:p}", Arc::as_ptr(&self.arena)),
            )
            .field("arena_strong", &Arc::strong_count(&self.arena));

        if let Some(layout) = states_layout {
            debug.field("states_layout", &layout);
        }

        debug.finish()
    }
}

fn decode_scalar(row: RowPtr, data_type: &DataType, offset: usize) -> Scalar {
    match data_type {
        DataType::Null => Scalar::Null,
        DataType::EmptyArray => Scalar::EmptyArray,
        DataType::EmptyMap => Scalar::EmptyMap,
        DataType::Boolean => Scalar::Boolean(unsafe { row.read::<u8>(offset) != 0 }),
        DataType::Number(number_type) => match number_type {
            NumberDataType::UInt8 => {
                Scalar::Number(NumberScalar::UInt8(unsafe { row.read::<u8>(offset) }))
            }
            NumberDataType::UInt16 => {
                Scalar::Number(NumberScalar::UInt16(unsafe { row.read::<u16>(offset) }))
            }
            NumberDataType::UInt32 => {
                Scalar::Number(NumberScalar::UInt32(unsafe { row.read::<u32>(offset) }))
            }
            NumberDataType::UInt64 => {
                Scalar::Number(NumberScalar::UInt64(unsafe { row.read::<u64>(offset) }))
            }
            NumberDataType::Int8 => {
                Scalar::Number(NumberScalar::Int8(unsafe { row.read::<i8>(offset) }))
            }
            NumberDataType::Int16 => {
                Scalar::Number(NumberScalar::Int16(unsafe { row.read::<i16>(offset) }))
            }
            NumberDataType::Int32 => {
                Scalar::Number(NumberScalar::Int32(unsafe { row.read::<i32>(offset) }))
            }
            NumberDataType::Int64 => {
                Scalar::Number(NumberScalar::Int64(unsafe { row.read::<i64>(offset) }))
            }
            NumberDataType::Float32 => {
                let value: f32 = unsafe { row.read::<f32>(offset) };
                Scalar::Number(NumberScalar::Float32(F32::from(value)))
            }
            NumberDataType::Float64 => {
                let value: f64 = unsafe { row.read::<f64>(offset) };
                Scalar::Number(NumberScalar::Float64(F64::from(value)))
            }
        },
        DataType::Decimal(size) => match DecimalDataKind::from(*size) {
            DecimalDataKind::Decimal64 => Scalar::Decimal(DecimalScalar::Decimal64(
                unsafe { row.read::<i64>(offset) },
                *size,
            )),
            DecimalDataKind::Decimal128 => Scalar::Decimal(DecimalScalar::Decimal128(
                unsafe { row.read::<i128>(offset) },
                *size,
            )),
            DecimalDataKind::Decimal256 => Scalar::Decimal(DecimalScalar::Decimal256(
                unsafe { row.read::<i256>(offset) },
                *size,
            )),
        },
        DataType::Timestamp => Scalar::Timestamp(unsafe { row.read::<i64>(offset) }),
        DataType::Date => Scalar::Date(unsafe { row.read::<i32>(offset) }),
        DataType::Interval => Scalar::Interval(unsafe { row.read::<months_days_micros>(offset) }),
        DataType::Binary => {
            let bytes = unsafe { row.read_bytes(offset) };
            Scalar::Binary(bytes.to_vec())
        }
        DataType::String => {
            let bytes = unsafe { row.read_bytes(offset) };
            let value = String::from_utf8(bytes.to_vec())
                .unwrap_or_else(|_| String::from_utf8_lossy(bytes).into_owned());
            Scalar::String(value)
        }
        DataType::Bitmap => {
            let bytes = unsafe { row.read_bytes(offset) };
            Scalar::Bitmap(bytes.to_vec())
        }
        DataType::Variant => {
            let bytes = unsafe { row.read_bytes(offset) };
            Scalar::Variant(bytes.to_vec())
        }
        DataType::Geometry => {
            let bytes = unsafe { row.read_bytes(offset) };
            Scalar::Geometry(bytes.to_vec())
        }
        DataType::Geography => {
            let bytes = unsafe { row.read_bytes(offset) };
            Scalar::Geography(Geography(bytes.to_vec()))
        }
        DataType::Array(_)
        | DataType::Map(_)
        | DataType::Tuple(_)
        | DataType::Vector(_)
        | DataType::Opaque(_) => {
            let bytes = unsafe { row.read_bytes(offset) };
            match bincode_deserialize_from_slice(bytes) {
                Ok(value) => value,
                Err(err) => Scalar::String(format!("<bincode error: {err}>")),
            }
        }
        DataType::Nullable(inner) => decode_scalar(row, inner, offset),
        DataType::Generic(_) | DataType::StageLocation => {
            Scalar::String(format!("<unsupported type: {data_type}>"))
        }
    }
}
impl Drop for Payload {
    fn drop(&mut self) {
        drop_guard(move || {
            // drop states
            if self.state_move_out {
                return;
            }

            let Some(states_layout) = self.row_layout.states_layout.as_ref() else {
                return;
            };

            'FOR: for (idx, (aggr, loc)) in self
                .aggrs
                .iter()
                .zip(states_layout.states_loc.iter())
                .enumerate()
            {
                if !aggr.need_manual_drop_state() {
                    continue;
                }

                for page in self.pages.iter() {
                    let is_partial_state = page.is_partial_state(self.aggrs.len());

                    if is_partial_state && idx == 0 {
                        info!(
                            "Cleaning partial page, state_offsets: {}, row: {}, agg length: {}",
                            page.state_offsets,
                            page.rows,
                            self.aggrs.len()
                        );
                    }
                    for row in 0..page.state_offsets.div_ceil(self.aggrs.len()) {
                        // When OOM, some states are not initialized, we don't need to destroy them
                        if is_partial_state && row * self.aggrs.len() + idx >= page.state_offsets {
                            continue 'FOR;
                        }
                        let addr = self.data_ptr(page, row).state_addr(&self.row_layout);
                        unsafe {
                            aggr.drop_state(AggrState::new(addr, loc));
                        }
                    }
                }
            }
        })
    }
}
