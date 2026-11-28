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

//! The groups of a final aggregate kept by bucket of their group hash, then aggregated a bucket
//! at a time.
//!
//! A final aggregate that inserts every row it receives into one hash table probes a table that
//! outgrows the caches as soon as it holds a few hundred thousand groups: each insert then waits
//! on memory, and the whole index is rebuilt every time it grows. Here the input is kept by
//! bucket instead, and each bucket is aggregated on its own, a fraction of all the groups:
//!
//! - The rows of partial aggregate payloads, already aggregated, are moved to the payload of their
//!   bucket. At the output each bucket is aggregated in place, in an index sized to the bucket:
//!   the first row of each group stays the group, the states of the others merge into it, and no
//!   row is copied again. A bucket too big for the caches is split by the next bits of the hash
//!   first.
//! - A partial aggregate may send the same groups many times, a payload per block. A bucket is
//!   aggregated in place as it grows too, keeping one row per group, so that it holds a bounded
//!   number of rows per group; see `StagedGroups::compact`.
//! - Raw rows and serialized states, which may repeat their groups any number of times, are
//!   appended to buffers of their bucket with their hashes, and once a bucket holds `FLUSH_ROWS`
//!   rows they go to a hash table of the bucket at once, while that table is in the caches.
//!
//! The buckets are the radix partitions of the group hash below bit 48 from `start_bit`, the bits
//! a final aggregate spills by: the first bits of a bucket are its spill bucket, so the groups
//! spill bucket by bucket without another pass.

use std::intrinsics::prefetch_read_data;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;

use super::BATCH_SIZE;
use super::MAX_PAGE_SIZE;
use super::Payload;
use super::PayloadFlushState;
use super::StateAddr;
use super::aggregate_function::AggregateCallRef;
use super::aggregate_function::AggregateStateSet;
use super::aggregate_function::get_states_layout;
use super::aggregate_hashtable::AggregateHashTable;
use super::group_hash_entries;
use super::payload_row::CompareState;
use super::payload_row::IsInArena;
use super::payload_row::visit_row_type;
use super::probe_state::RowID;
use super::row_ptr::RowPtr;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::HashTableConfig;
use crate::ProjectedBlock;
use crate::types::DataType;
use crate::types::DecimalColumn;
use crate::types::DecimalColumnBuilder;
use crate::types::NumberColumn;
use crate::types::NumberColumnBuilder;
use crate::with_number_type;

/// Pages of the rows kept by bucket: a final aggregate keeps a page open per bucket.
const STAGED_PAGE_SIZE: usize = MAX_PAGE_SIZE / 4;

/// The raw rows or serialized states buffered in a bucket before they go to its table.
const FLUSH_ROWS: usize = 32 * 1024;

/// The rows of partial aggregate states a bucket holds before it is first aggregated in place,
/// see `StagedGroups::compact`.
const COMPACT_ROWS: usize = 32 * 1024;

/// The bytes of rows and index a bucket is aggregated in at most: a bigger bucket splits by the
/// next bits of the hash first. Up to this size, fetching the slots of the rows ahead hides the
/// misses of an index out of the caches, which costs less than moving the rows again.
const BUILD_TARGET_BYTES: usize = 16 << 20;

/// An index of more bytes than this fetches the slots of the rows ahead.
const PREFETCH_SLOT_BYTES: usize = 256 << 10;

/// How many rows ahead the slots are fetched.
const PREFETCH_ROWS: usize = 16;

/// Index bytes per row of a bucket build, see `InPlaceBuilder`: 8-byte slots for 1.5 to 3 slots
/// a row, and the address of each group.
const BUILD_INDEX_BYTES_PER_ROW: usize = 24;

/// The rows of blocks buffered for their buckets, column by column: the group columns, then the
/// arguments of the aggregates or their serialized states.
struct ColumnBuffer {
    /// The builders of each column, one per bucket.
    columns: Vec<Vec<ColumnBuilder>>,
    types: Vec<DataType>,
    /// The group hashes of the rows of each bucket.
    hashes: Vec<Vec<u64>>,
}

impl ColumnBuffer {
    fn new(types: Vec<DataType>, buckets: usize) -> Self {
        let columns = types
            .iter()
            .map(|ty| {
                (0..buckets)
                    .map(|_| ColumnBuilder::with_capacity(ty, 0))
                    .collect()
            })
            .collect();
        ColumnBuffer {
            columns,
            types,
            hashes: vec![vec![]; buckets],
        }
    }

    /// Append the rows of `entries`, of group hashes `hashes`, row `i` to bucket `buckets[i]`.
    fn append(&mut self, entries: &[&BlockEntry], hashes: &[u64], buckets: &[u8]) {
        for (hash, bucket) in hashes.iter().zip(buckets) {
            self.hashes[*bucket as usize].push(*hash);
        }
        for (entry, builders) in entries.iter().zip(&mut self.columns) {
            let column = match entry.to_column() {
                Column::Encoded(encoded) => encoded.decode(),
                column => column,
            };
            scatter(
                &column,
                buckets,
                &mut builders.iter_mut().collect::<Vec<_>>(),
            );
        }
    }

    /// The columns buffered for `bucket` and their hashes, taken out.
    fn take(&mut self, bucket: usize) -> (Vec<BlockEntry>, Vec<u64>) {
        let hashes = std::mem::take(&mut self.hashes[bucket]);
        self.hashes[bucket].reserve(hashes.len());
        let columns = self
            .columns
            .iter_mut()
            .zip(&self.types)
            .map(|(builders, ty)| {
                let builder = std::mem::replace(
                    &mut builders[bucket],
                    ColumnBuilder::with_capacity(ty, hashes.len()),
                );
                BlockEntry::from(builder.build())
            })
            .collect();
        (columns, hashes)
    }
}

/// Append row `i` of `column` to `builders[buckets[i]]`, builders of the type of the column.
fn scatter(column: &Column, buckets: &[u8], builders: &mut [&mut ColumnBuilder]) {
    macro_rules! values {
        ($values:expr, $pattern:pat => $vec:expr) => {{
            let mut to = builders
                .iter_mut()
                .map(|builder| match &mut **builder {
                    $pattern => $vec,
                    _ => unreachable!("a builder of the type of the column"),
                })
                .collect::<Vec<_>>();
            for (value, bucket) in $values.iter().zip(buckets) {
                to[*bucket as usize].push(*value);
            }
        }};
    }
    match column {
        Column::Number(numbers) => with_number_type!(|NUM_TYPE| match numbers {
            NumberColumn::NUM_TYPE(values) => values!(
                values,
                ColumnBuilder::Number(NumberColumnBuilder::NUM_TYPE(v)) => v
            ),
        }),
        Column::Decimal(DecimalColumn::Decimal64(values, _)) => values!(
            values,
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal64(v, _)) => v
        ),
        Column::Decimal(DecimalColumn::Decimal128(values, _)) => values!(
            values,
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal128(v, _)) => v
        ),
        Column::Decimal(DecimalColumn::Decimal256(values, _)) => values!(
            values,
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal256(v, _)) => v
        ),
        Column::Date(values) => values!(values, ColumnBuilder::Date(v) => v),
        Column::Timestamp(values) => values!(values, ColumnBuilder::Timestamp(v) => v),
        Column::String(strings) => {
            let mut to = builders
                .iter_mut()
                .map(|builder| match &mut **builder {
                    ColumnBuilder::String(b) => b,
                    _ => unreachable!("a builder of the type of the column"),
                })
                .collect::<Vec<_>>();
            for (value, bucket) in strings.iter().zip(buckets) {
                to[*bucket as usize].put_and_commit(value);
            }
        }
        Column::Nullable(nullable) => {
            let mut inner = Vec::with_capacity(builders.len());
            for builder in builders.iter_mut() {
                let ColumnBuilder::Nullable(builder) = &mut **builder else {
                    unreachable!("a builder of the type of the column")
                };
                let builder = &mut **builder;
                inner.push((&mut builder.builder, &mut builder.validity));
            }
            for (valid, bucket) in nullable.validity().iter().zip(buckets) {
                inner[*bucket as usize].1.push(valid);
            }
            let mut inner = inner.into_iter().map(|(b, _)| b).collect::<Vec<_>>();
            scatter(nullable.column(), buckets, &mut inner);
        }
        Column::Tuple(fields) => {
            let mut tuples = builders
                .iter_mut()
                .map(|builder| match &mut **builder {
                    ColumnBuilder::Tuple(fields) => fields.iter_mut().collect::<Vec<_>>(),
                    _ => unreachable!("a builder of the type of the column"),
                })
                .collect::<Vec<_>>();
            for (index, field) in fields.iter().enumerate() {
                let mut to = tuples
                    .iter_mut()
                    .map(|fields| &mut *fields[index])
                    .collect::<Vec<_>>();
                scatter(field, buckets, &mut to);
            }
        }
        _ => {
            for (row, bucket) in buckets.iter().enumerate() {
                // SAFETY: `row` is below the length of the column.
                let value = unsafe { column.index_unchecked(row) };
                builders[*bucket as usize].push(value);
            }
        }
    }
}

/// The groups of a final aggregate, kept by bucket of their group hash, see the module.
pub struct StagedGroups {
    group_types: Vec<DataType>,
    aggrs: Vec<AggregateCallRef>,
    /// The buckets are `(hash & mask) >> shift`.
    mask: u64,
    shift: u64,
    bits: u64,
    /// The rows of partial aggregate states of each bucket.
    buckets: Vec<Payload>,
    /// The rows of each bucket at which it is aggregated in place next, see `compact`.
    compact_at: Vec<usize>,
    builder: InPlaceBuilder,
    /// The offsets in a row of the group values whose bytes are in an arena: they are copied into
    /// the arena of the bucket of their row.
    arena_offsets: Vec<usize>,
    /// The hash table of each bucket that was given raw rows or serialized states.
    tables: Vec<Option<AggregateHashTable>>,
    /// The raw rows buffered in each bucket, and the serialized states, by column.
    raw: Option<ColumnBuffer>,
    serialized: Option<ColumnBuffer>,
    /// The arguments of each aggregate in the raw rows.
    params_len: Vec<usize>,
    /// The raw rows and serialized states buffered in each bucket.
    buffered: Vec<usize>,
    /// The rows added.
    added: usize,
}

unsafe impl Send for StagedGroups {}

/// The radix partition of `bits` bits of the group hash below bit 48, `start_bit` bits down.
fn radix(start_bit: u64, bits: u64) -> (u64, u64) {
    debug_assert!(start_bit + bits <= 48);
    let shift = 48 - start_bit - bits;
    (((1u64 << bits) - 1) << shift, shift)
}

/// The offsets in the rows of `payload` of the group values whose bytes are in an arena.
fn arena_offsets(payload: &Payload) -> Vec<usize> {
    payload
        .group_types
        .iter()
        .zip(&payload.row_layout.group_offsets)
        .filter(|(data_type, _)| visit_row_type(&data_type.remove_nullable(), IsInArena))
        .map(|(_, offset)| *offset)
        .collect()
}

fn new_bucket(group_types: &[DataType], aggrs: &[AggregateCallRef]) -> Payload {
    let states_layout = (!aggrs.is_empty()).then(|| get_states_layout(aggrs).unwrap());
    Payload::new(
        Arc::new(Bump::new()),
        group_types.to_vec(),
        aggrs.to_vec(),
        states_layout,
        STAGED_PAGE_SIZE,
    )
}

fn new_table(group_types: &[DataType], aggrs: &[AggregateCallRef]) -> AggregateHashTable {
    AggregateHashTable::new(
        group_types.to_vec(),
        aggrs.to_vec(),
        HashTableConfig::default().with_initial_radix_bits(0),
        Arc::new(Bump::new()),
    )
}

impl StagedGroups {
    /// Buckets by `bits` bits of the group hash from `start_bit`, at most 8.
    pub fn new(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        start_bit: u64,
        bits: u64,
    ) -> Self {
        assert!(bits <= 8);
        let (mask, shift) = radix(start_bit, bits);
        let count = 1usize << bits;
        let buckets = (0..count)
            .map(|_| new_bucket(&group_types, &aggrs))
            .collect::<Vec<_>>();
        let arena_offsets = arena_offsets(&buckets[0]);
        StagedGroups {
            group_types,
            aggrs,
            mask,
            shift,
            bits,
            buckets,
            compact_at: vec![COMPACT_ROWS; count],
            builder: InPlaceBuilder::default(),
            arena_offsets,
            tables: (0..count).map(|_| None).collect(),
            raw: None,
            serialized: None,
            params_len: vec![],
            buffered: vec![0; count],
            added: 0,
        }
    }

    /// Whether bucketing by `bits` bits from `start_bit` fits the bits of the group hash below 48.
    pub fn fits(start_bit: u64, bits: u64) -> bool {
        start_bit + bits <= 48
    }

    /// The rows added.
    pub fn len(&self) -> usize {
        self.added
    }

    pub fn is_empty(&self) -> bool {
        self.added == 0
    }

    /// Move the rows of `payload`, partial aggregate states of the same aggregates, to the payloads
    /// of their buckets.
    pub fn add_payload(
        &mut self,
        mut payload: Payload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        if payload.len() == 0 {
            return Ok(());
        }
        self.added += payload.len();
        payload.move_rows_into(
            &mut self.buckets,
            self.mask,
            self.shift,
            &self.arena_offsets,
        );
        for bucket in 0..self.buckets.len() {
            if self.buckets[bucket].len() >= self.compact_at[bucket] {
                self.compact(bucket, flush_state)?;
            }
        }
        Ok(())
    }

    /// Aggregate the rows of `bucket` in place and keep the first row of each group, the group,
    /// in a new payload of the bucket: the other rows of the group were merged into it, and are
    /// dropped with the old payload.
    ///
    /// A bucket whose groups came again is aggregated again once it holds twice its groups, so
    /// that it never holds more than about two rows per group. A bucket of few repeated groups
    /// waits sixteen times longer each time, so that it is mostly aggregated once, at
    /// `COMPACT_ROWS`, before the output when its groups do not come again.
    fn compact(&mut self, bucket: usize, flush_state: &mut PayloadFlushState) -> Result<()> {
        let before = self.buckets[bucket].len();
        self.builder
            .build(&self.buckets[bucket], flush_state, true)?;
        let mut compacted = new_bucket(&self.group_types, &self.aggrs);
        self.buckets[bucket].move_rows_to(
            &self.builder.leaders,
            &self.builder.merged,
            &mut compacted,
            &self.arena_offsets,
        );
        // Dropping the old payload drops its pages and arena, not its states.
        self.buckets[bucket] = compacted;
        let after = self.builder.leaders.len();
        self.compact_at[bucket] = match after * 4 <= before * 3 {
            true => (after * 2).max(COMPACT_ROWS),
            false => before.saturating_mul(16),
        };
        self.builder.clear();
        Ok(())
    }

    /// The rows of partial aggregate states the buckets hold.
    pub fn staged_rows(&self) -> usize {
        self.buckets.iter().map(Payload::len).sum()
    }

    /// Move the groups of `table`, a table of the same aggregates, here.
    pub fn add_table(
        &mut self,
        table: AggregateHashTable,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        for payload in table.into_payloads() {
            self.add_payload(payload, flush_state)?;
        }
        Ok(())
    }

    /// Add the rows of a block to the buffers of their buckets: raw rows, aggregated with
    /// `params`, or else serialized states, `agg_states`, merged.
    pub fn add_rows(
        &mut self,
        flush_state: &mut PayloadFlushState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        agg_states: ProjectedBlock,
        row_count: usize,
    ) -> Result<()> {
        if row_count == 0 {
            return Ok(());
        }
        self.added += row_count;
        let mut hashes = vec![0u64; row_count];
        group_hash_entries(group_columns, &mut hashes);
        let buckets = hashes
            .iter()
            .map(|hash| ((hash & self.mask) >> self.shift) as u8)
            .collect::<Vec<_>>();
        for bucket in &buckets {
            self.buffered[*bucket as usize] += 1;
        }
        let mut entries = group_columns.iter().collect::<Vec<_>>();
        let buffer = match agg_states.is_empty() {
            true => {
                if self.raw.is_none() {
                    self.params_len = params.iter().map(|p| p.len()).collect();
                }
                entries.extend(params.iter().flat_map(|p| p.iter()));
                &mut self.raw
            }
            false => {
                entries.extend(agg_states.iter());
                &mut self.serialized
            }
        };
        let count = self.buckets.len();
        buffer
            .get_or_insert_with(|| {
                let types = entries.iter().map(|e| e.data_type()).collect();
                ColumnBuffer::new(types, count)
            })
            .append(&entries, &hashes, &buckets);
        for bucket in 0..count {
            if self.buffered[bucket] >= FLUSH_ROWS {
                self.flush(bucket, flush_state)?;
            }
        }
        Ok(())
    }

    /// Merge the raw rows and serialized states buffered in `bucket` into its table.
    fn flush(&mut self, bucket: usize, flush_state: &mut PayloadFlushState) -> Result<()> {
        if self.buffered[bucket] == 0 {
            return Ok(());
        }
        self.buffered[bucket] = 0;
        let table =
            self.tables[bucket].get_or_insert_with(|| new_table(&self.group_types, &self.aggrs));
        let groups = self.group_types.len();
        let state = &mut flush_state.probe_state;
        if let Some((columns, hashes)) = self.raw.as_mut().map(|raw| raw.take(bucket))
            && !hashes.is_empty()
        {
            let rows = hashes.len();
            let (group_columns, args) = columns.split_at(groups);
            let mut params = Vec::with_capacity(self.params_len.len());
            let mut start = 0;
            for len in &self.params_len {
                params.push(ProjectedBlock::from(&args[start..start + len]));
                start += len;
            }
            let empty: ProjectedBlock = (&[]).into();
            table.add_hashed_groups(state, group_columns.into(), &params, empty, &hashes, rows)?;
        }
        if let Some((columns, hashes)) = self.serialized.as_mut().map(|s| s.take(bucket))
            && !hashes.is_empty()
        {
            let rows = hashes.len();
            let (group_columns, states) = columns.split_at(groups);
            let group_columns = group_columns.into();
            table.add_hashed_groups(state, group_columns, &[], states.into(), &hashes, rows)?;
        }
        Ok(())
    }

    /// The groups, a bucket at a time; `serialize` gives the serialized states of the groups
    /// rather than their results, for a spill.
    pub fn into_output(self, serialize: bool) -> StagedOutput {
        StagedOutput {
            staged: self,
            next: 0,
            pending: vec![],
            current: None,
            serialize,
        }
    }
}

/// A part of a bucket of `StagedGroups` to aggregate in place.
struct StagedPart {
    /// The bucket of `StagedGroups` the part comes from.
    bucket: usize,
    /// The bit of the hash below 48 the part is split by next.
    split_bit: u64,
    payload: Payload,
    /// The bytes of group values in the arena, per row.
    arena_bytes: usize,
}

impl StagedPart {
    /// The bytes an aggregation of the part takes.
    fn build_bytes(&self) -> usize {
        let rows = self.payload.len();
        self.payload.memory_size() + rows * (BUILD_INDEX_BYTES_PER_ROW + self.arena_bytes)
    }
}

/// The groups being output: of a part aggregated in place, `leaders` of the builder, or of a
/// table.
enum Current {
    /// The part, and the groups output so far.
    Part(usize, Payload, usize),
    Table(usize, Payload),
}

/// The groups of `StagedGroups`, output a bucket at a time.
pub struct StagedOutput {
    staged: StagedGroups,
    /// The bucket to output next.
    next: usize,
    /// The parts of the bucket still to aggregate, the next last.
    pending: Vec<StagedPart>,
    current: Option<Current>,
    serialize: bool,
}

unsafe impl Send for StagedOutput {}

impl StagedOutput {
    /// The next block of groups, and the bucket of `StagedGroups` they come from; `None` once every
    /// group was output. The buckets are output in order.
    pub fn next_block(
        &mut self,
        flush_state: &mut PayloadFlushState,
    ) -> Result<Option<(usize, DataBlock)>> {
        loop {
            match self.current.as_mut() {
                Some(Current::Part(bucket, payload, done)) => {
                    let leaders = &self.staged.builder.leaders;
                    if *done < leaders.len() {
                        let end = (*done + BATCH_SIZE).min(leaders.len());
                        let block = payload.flush_rows(
                            &leaders[*done..end],
                            flush_state,
                            self.serialize,
                        )?;
                        *done = end;
                        return Ok(Some((*bucket, block)));
                    }
                }
                Some(Current::Table(bucket, payload)) => {
                    let block = match self.serialize {
                        true => payload.aggregate_flush(flush_state)?,
                        false => payload.merge_result(flush_state)?,
                    };
                    if let Some(block) = block {
                        return Ok(Some((*bucket, block)));
                    }
                }
                None => {}
            }
            // Dropping the rows drops their states.
            self.current = None;
            flush_state.clear();
            if let Some(part) = self.pending.pop() {
                if part.build_bytes() > BUILD_TARGET_BYTES && part.split_bit < 48 {
                    self.split(part);
                } else {
                    self.staged
                        .builder
                        .build(&part.payload, flush_state, false)?;
                    self.current = Some(Current::Part(part.bucket, part.payload, 0));
                }
                continue;
            }
            let bucket = self.next;
            if bucket == self.staged.buckets.len() {
                return Ok(None);
            }
            self.next += 1;
            let staged = &mut self.staged;
            let payload = std::mem::replace(
                &mut staged.buckets[bucket],
                new_bucket(&staged.group_types, &staged.aggrs),
            );
            if staged.tables[bucket].is_none() && staged.buffered[bucket] == 0 {
                // Only rows of partial aggregate states: aggregated in place.
                if payload.len() != 0 {
                    let arena_bytes = payload.arena.allocated_bytes() / payload.len();
                    self.pending.push(StagedPart {
                        bucket,
                        split_bit: 48 - staged.shift,
                        payload,
                        arena_bytes,
                    });
                }
                continue;
            }
            // The bucket has a table: its buffers and its rows go to it.
            staged.flush(bucket, flush_state)?;
            let mut table = staged.tables[bucket].take().unwrap();
            table.combine_payload(&payload, flush_state)?;
            drop(payload);
            flush_state.clear();
            // A table of one partition; its index is dropped here.
            self.current = table
                .into_payloads()
                .pop()
                .map(|payload| Current::Table(bucket, payload));
        }
    }

    /// Split `part` by the next bits of the hash into parts of about the target size.
    fn split(&mut self, mut part: StagedPart) {
        let bits = (part
            .build_bytes()
            .div_ceil(BUILD_TARGET_BYTES)
            .next_power_of_two()
            .trailing_zeros() as u64)
            .min(48 - part.split_bit)
            .min(8);
        let (mask, shift) = radix(part.split_bit, bits);
        // The parts keep the values in the arena of the bucket.
        let mut payloads = (0..1usize << bits)
            .map(|_| part.payload.empty_like())
            .collect::<Vec<_>>();
        part.payload.move_rows_into(&mut payloads, mask, shift, &[]);
        for payload in payloads.into_iter().rev() {
            self.pending.push(StagedPart {
                bucket: part.bucket,
                split_bit: part.split_bit + bits,
                payload,
                arena_bytes: part.arena_bytes,
            });
        }
    }

    /// The bits of the group hash the buckets of `StagedGroups` are by.
    pub fn bucket_bits(&self) -> u64 {
        self.staged.bits
    }
}

/// Aggregates the rows of a payload in place, see the module: the first row of each group is
/// its group, `leaders`, and the states of the others are merged into it.
#[derive(Default)]
struct InPlaceBuilder {
    /// Open addressing by the low bits of the hash, see `Slot`.
    slots: Vec<u64>,
    /// The slots of `build_by_bytes` for buckets of fewer groups than they number, half as big.
    narrow_slots: Vec<u32>,
    leaders: Vec<RowPtr>,
    places: Vec<StateAddr>,
    rhses: Vec<StateAddr>,
    /// The states merged into the states of their group, kept with `keep_merged`.
    merged: Vec<StateAddr>,
    keep_merged: bool,
    /// The offsets in a row of the group values whose bytes are in an arena.
    bytes_offsets: Vec<usize>,
}

/// A slot of the index of `InPlaceBuilder`: high bits of the hash of the group, then its number
/// in `leaders` plus one; 0 is empty.
trait Slot: Copy + Default + Eq {
    /// The most groups the slots number.
    const MAX_GROUPS: usize;

    fn new(hash: u64, group: usize) -> Self;

    /// Whether a group of hash `hash` may be in the slot.
    fn tag_matches(self, hash: u64) -> bool;

    fn group(self) -> usize;
}

impl Slot for u64 {
    const MAX_GROUPS: usize = u32::MAX as usize - 1;

    #[inline(always)]
    fn new(hash: u64, group: usize) -> Self {
        ((hash >> 32) << 32) | (group as u64 + 1)
    }

    #[inline(always)]
    fn tag_matches(self, hash: u64) -> bool {
        self >> 32 == hash >> 32
    }

    #[inline(always)]
    fn group(self) -> usize {
        (self as u32 - 1) as usize
    }
}

impl Slot for u32 {
    const MAX_GROUPS: usize = (1 << 24) - 2;

    #[inline(always)]
    fn new(hash: u64, group: usize) -> Self {
        (((hash >> 56) as u32) << 24) | (group as u32 + 1)
    }

    #[inline(always)]
    fn tag_matches(self, hash: u64) -> bool {
        self >> 24 == (hash >> 56) as u32
    }

    #[inline(always)]
    fn group(self) -> usize {
        ((self & 0xff_ffff) - 1) as usize
    }
}

/// The slots of an index of the groups of `rows` rows, by group hash: the new group of a row
/// takes the first empty slot from the one of its hash.
struct SlotIndex<'a, S> {
    slots: &'a mut Vec<S>,
    mask: usize,
    /// Whether to fetch the slots of the rows ahead: the index is bigger than the caches.
    prefetch: bool,
}

impl<'a, S: Slot> SlotIndex<'a, S> {
    fn new(slots: &'a mut Vec<S>, rows: usize) -> Self {
        let capacity = (rows + rows / 2).next_power_of_two().max(16);
        slots.clear();
        slots.resize(capacity, S::default());
        SlotIndex {
            slots,
            mask: capacity - 1,
            prefetch: capacity * size_of::<S>() > PREFETCH_SLOT_BYTES,
        }
    }

    #[inline(always)]
    fn prefetch(&self, hash: u64) {
        if self.prefetch {
            prefetch_read_data::<_, 3>(self.slots.as_ptr().wrapping_add(hash as usize & self.mask));
        }
    }

    /// The number in `leaders` of the group of the key of `key_len` bytes at `row`, of hash
    /// `hash`: a group whose row starts with the same bytes, or else a new group, `row` pushed to
    /// `leaders`; and whether it is new.
    #[inline(always)]
    fn find_or_insert(
        &mut self,
        leaders: &mut Vec<RowPtr>,
        row: *const u8,
        hash: u64,
        key_len: usize,
    ) -> (usize, bool) {
        let mut slot = hash as usize & self.mask;
        loop {
            // SAFETY: `slot` is masked by `mask`, and there are `mask + 1` slots.
            let s = unsafe { *self.slots.get_unchecked(slot) };
            if s == S::default() {
                let group = leaders.len();
                unsafe { *self.slots.get_unchecked_mut(slot) = S::new(hash, group) };
                leaders.push(RowPtr::new(row));
                return (group, true);
            }
            if s.tag_matches(hash) {
                let group = s.group();
                // SAFETY: a slot numbers a group of `leaders`, whose row starts with its key, as
                // `row` does.
                let leader = unsafe { *leaders.get_unchecked(group) };
                if unsafe { key_bytes_eq(leader.as_ptr(), row, key_len) } {
                    return (group, false);
                }
            }
            slot = (slot + 1) & self.mask;
        }
    }
}

/// Whether two values of `data_type` in a row are the same group exactly when their bytes are
/// equal: integers, decimals, dates, timestamps and booleans. Not floats (-0.0, NaN), nor a
/// nullable value, whose bytes under a NULL are not defined, nor values in an arena.
fn compares_by_bytes(data_type: &DataType) -> bool {
    match data_type {
        DataType::Number(number) => !number.is_float(),
        DataType::Decimal(_) | DataType::Date | DataType::Timestamp | DataType::Boolean => true,
        _ => false,
    }
}

/// Whether the first `len` bytes at `a` and `b` are equal.
#[inline(always)]
unsafe fn key_bytes_eq(a: *const u8, b: *const u8, len: usize) -> bool {
    unsafe {
        match len {
            4 => (a as *const u32).read_unaligned() == (b as *const u32).read_unaligned(),
            8 => (a as *const u64).read_unaligned() == (b as *const u64).read_unaligned(),
            12 => {
                (a as *const u64).read_unaligned() == (b as *const u64).read_unaligned()
                    && (a.add(8) as *const u32).read_unaligned()
                        == (b.add(8) as *const u32).read_unaligned()
            }
            16 => (a as *const u128).read_unaligned() == (b as *const u128).read_unaligned(),
            _ => std::slice::from_raw_parts(a, len) == std::slice::from_raw_parts(b, len),
        }
    }
}

impl InPlaceBuilder {
    /// Forget the groups of the last build.
    fn clear(&mut self) {
        self.leaders.clear();
        self.places.clear();
        self.rhses.clear();
        self.merged.clear();
    }

    /// Aggregate the rows of `payload` in place: the first row of each group is its group, see
    /// `leaders`. With `keep_merged`, the states of the other rows are kept in `merged`.
    fn build(
        &mut self,
        payload: &Payload,
        flush_state: &mut PayloadFlushState,
        keep_merged: bool,
    ) -> Result<()> {
        let rows = payload.len();
        self.clear();
        self.leaders.reserve(rows);
        self.keep_merged = keep_merged;
        if payload.group_types.iter().all(compares_by_bytes) {
            return match rows <= u32::MAX_GROUPS {
                true => {
                    let mut slots = std::mem::take(&mut self.narrow_slots);
                    let result = self.build_by_bytes(&mut slots, payload);
                    self.narrow_slots = slots;
                    result
                }
                false => {
                    let mut slots = std::mem::take(&mut self.slots);
                    let result = self.build_by_bytes(&mut slots, payload);
                    self.slots = slots;
                    result
                }
            };
        }
        self.build_by_rows(payload, flush_state)
    }

    /// `build` for any group types: the rows of a batch are probed together, and the group
    /// values of those whose slot matches compared with those of the group at once.
    fn build_by_rows(
        &mut self,
        payload: &Payload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        let n = payload.len();
        self.bytes_offsets = arena_offsets(payload);
        let capacity = (n + n / 2).next_power_of_two().max(16);
        let mask = capacity - 1;
        self.slots.clear();
        self.slots.resize(capacity, 0);
        let layout = &payload.row_layout;
        let has_states = layout.states_layout.is_some();
        // The slots of a bucket bigger than the caches: fetch those of the rows ahead.
        let prefetch = capacity * 8 > PREFETCH_SLOT_BYTES;

        flush_state.clear();
        while payload.scan_rows(flush_state) {
            let rows = flush_state.row_count;
            let state = &mut *flush_state.probe_state;
            for row in 0..rows {
                state.no_match_vector[row] = RowID::from(row);
                state.slots[row] = state.group_hashes[row] as usize & mask;
            }
            let mut remaining = rows;
            let mut first = true;
            while remaining > 0 {
                let mut compare = 0;
                for i in 0..remaining {
                    if prefetch && first && i + PREFETCH_ROWS < remaining {
                        let ahead = state.slots[i + PREFETCH_ROWS];
                        prefetch_read_data::<_, 3>(self.slots.as_ptr().wrapping_add(ahead));
                    }
                    let row = state.no_match_vector[i];
                    let hash = state.group_hashes[row];
                    let mut slot = state.slots[row];
                    loop {
                        // SAFETY: `slot` is masked by `mask`, and there are `mask + 1` slots.
                        let s = unsafe { *self.slots.get_unchecked(slot) };
                        if s == 0 {
                            self.slots[slot] = u64::new(hash, self.leaders.len());
                            self.leaders.push(flush_state.addresses[row]);
                            break;
                        }
                        if s.tag_matches(hash) {
                            state.addresses[row] = self.leaders[s.group()];
                            state.slots[row] = slot;
                            state.group_compare_vector[compare] = row;
                            compare += 1;
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
                first = false;
                if compare == 0 {
                    break;
                }
                // The bytes of strings are elsewhere, mostly out of the caches: fetch those of
                // every pair to compare first, so that the misses overlap.
                for &offset in &self.bytes_offsets {
                    for &row in &state.group_compare_vector[..compare] {
                        unsafe {
                            prefetch_read_data::<_, 3>(
                                state.addresses[row].read::<u64>(offset + 4) as *const u8,
                            );
                            prefetch_read_data::<_, 3>(
                                flush_state.addresses[row].read::<u64>(offset + 4) as *const u8,
                            );
                        }
                    }
                }
                let no_match = CompareState {
                    address: &state.addresses,
                    compare: &mut state.group_compare_vector,
                    no_matched: &mut state.no_match_vector,
                }
                .row_match_rows(
                    &flush_state.addresses,
                    &payload.group_types,
                    layout,
                    (compare, 0),
                );
                // The rows that matched their group are first in `group_compare_vector`.
                if has_states {
                    for &row in &state.group_compare_vector[..compare - no_match] {
                        self.places.push(state.addresses[row].state_addr(layout));
                        self.rhses.push(flush_state.state_places[row]);
                    }
                }
                for &row in &state.no_match_vector[..no_match] {
                    state.slots[row] = (state.slots[row] + 1) & mask;
                }
                remaining = no_match;
            }
            if self.places.len() >= BATCH_SIZE {
                self.merge(payload)?;
            }
        }
        self.merge(payload)?;
        flush_state.clear();
        Ok(())
    }

    /// `build` for group keys that compare by their bytes, see `compares_by_bytes`: the key is
    /// the bytes of a row before its hash, compared as the row is probed.
    fn build_by_bytes<S: Slot>(&mut self, slots: &mut Vec<S>, payload: &Payload) -> Result<()> {
        let layout = &payload.row_layout;
        let has_states = layout.states_layout.is_some();
        let key_len = layout.hash_offset;
        let row_size = payload.tuple_size;
        let mut index = SlotIndex::new(slots, payload.len());
        for page in &payload.pages {
            let base = page.base();
            let row_at = |r: usize| unsafe { base.add(r * row_size) };
            let hash_at = |r: usize| RowPtr::new(row_at(r)).hash(layout);
            for r in 0..page.rows {
                if r + PREFETCH_ROWS < page.rows {
                    index.prefetch(hash_at(r + PREFETCH_ROWS));
                }
                let row = row_at(r);
                let (group, new) =
                    index.find_or_insert(&mut self.leaders, row, hash_at(r), key_len);
                if !new && has_states {
                    self.places.push(self.leaders[group].state_addr(layout));
                    self.rhses.push(RowPtr::new(row).state_addr(layout));
                    if self.places.len() == BATCH_SIZE {
                        self.merge(payload)?;
                    }
                }
            }
        }
        self.merge(payload)
    }

    fn merge(&mut self, payload: &Payload) -> Result<()> {
        if let Some(states) = payload.row_layout.states_layout.as_ref() {
            for (aggr, loc) in payload.aggrs.iter().zip(states.states_loc.iter()) {
                aggr.merge_states_keys(AggregateStateSet::new(&self.places, loc), &self.rhses)?;
            }
        }
        if self.keep_merged {
            self.merged.extend_from_slice(&self.rhses);
        }
        self.places.clear();
        self.rhses.clear();
        Ok(())
    }
}
