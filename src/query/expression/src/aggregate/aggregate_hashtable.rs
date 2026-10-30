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

// A new AggregateHashtable which inspired by duckdb's https://duckdb.org/2022/03/07/aggregate-hashtable.html

use std::sync::Arc;
use std::sync::atomic::Ordering;

use bumpalo::Bump;
use databend_common_exception::Result;
use smallvec::SmallVec;

use super::BATCH_SIZE;
use super::HashIndex;
use super::HashTableConfig;
use super::LOAD_FACTOR;
use super::MAX_PAGE_SIZE;
use super::Payload;
use super::group_hash_entries;
use super::hash_index_adapter::AdapterImpl;
use super::partitioned_payload::PartitionedPayload;
use super::payload_flush::PayloadFlushState;
use super::probe_state::ProbeState;
use super::row_ptr::RowPtr;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::ProjectedBlock;
use crate::aggregate::AggrState;
use crate::aggregate::aggregate_function::AggregateCallRef;
use crate::aggregate::aggregate_function::AggregateStateSet;
use crate::types::DataType;

const SMALL_CAPACITY_RESIZE_COUNT: usize = 4;

#[derive(Default)]
struct GroupKeyState {
    dict_hashes: Option<Vec<u64>>,
    // Independent code dimensions: (group column index, mixed-radix stride).
    code_columns: SmallVec<[(usize, usize); 4]>,
    rank_of_code: Vec<u32>,
    code_addresses: Vec<RowPtr>,
    row_offset: usize,
}

impl GroupKeyState {
    fn init_codes(&mut self, group_columns: ProjectedBlock, row_count: usize) {
        let mut code_space = 1usize;
        for (index, entry) in group_columns.iter().enumerate() {
            let BlockEntry::Column(Column::Encoded(encoded)) = entry else {
                return;
            };
            let (codes, count) = encoded.codes();
            // Dictionary projections retain their input keys. Check sharing once per block.
            if self.code_columns.iter().any(|(column, _)| {
                matches!(
                    &group_columns[*column],
                    BlockEntry::Column(Column::Encoded(other))
                        if codes.shares_storage(other.codes().0)
                )
            }) {
                continue;
            }
            let stride = code_space;
            let Ok(count) = usize::try_from(count) else {
                return;
            };
            let Some(next_space) = code_space.checked_mul(count) else {
                return;
            };
            code_space = next_space;
            if code_space.saturating_mul(4) > row_count {
                return;
            }
            self.code_columns.push((index, stride));
        }
        if u32::try_from(code_space).is_err() {
            return;
        }
        self.rank_of_code.resize(code_space, u32::MAX);
        self.code_addresses.resize(code_space, RowPtr::null());
    }

    fn dict_hashes(&self, row_count: usize) -> Option<&[u64]> {
        self.dict_hashes
            .as_deref()
            .map(|hashes| &hashes[self.row_offset..self.row_offset + row_count])
    }

    fn clear_codes(&mut self) {
        self.code_addresses.fill(RowPtr::null());
        self.rank_of_code.fill(u32::MAX);
    }
}

pub struct AggregateHashTable {
    // Hash index entries store RowRef values into these payload pages. Any path
    // that replaces or repartitions payload must clear or rebuild the index
    // before probing it again.
    pub payload: PartitionedPayload,
    // use for append rows directly during deserialize
    pub direct_append: bool,
    pub config: HashTableConfig,

    current_radix_bits: u64,
    hash_index: HashIndex,
    hash_index_resize_count: usize,
}

unsafe impl Send for AggregateHashTable {}
unsafe impl Sync for AggregateHashTable {}

impl AggregateHashTable {
    pub fn new(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        config: HashTableConfig,
        arena: Arc<Bump>,
    ) -> Self {
        let capacity = Self::initial_capacity();
        Self::new_with_capacity(group_types, aggrs, config, capacity, arena)
    }

    pub fn new_with_capacity(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        config: HashTableConfig,
        capacity: usize,
        arena: Arc<Bump>,
    ) -> Self {
        Self {
            direct_append: false,
            current_radix_bits: config.initial_radix_bits,
            payload: PartitionedPayload::new_with_start_bit(
                group_types,
                aggrs,
                1 << config.initial_radix_bits,
                config.partition_start_bit,
                vec![arena],
            ),
            hash_index: HashIndex::with_capacity(capacity),
            config,
            hash_index_resize_count: 0,
        }
    }

    pub fn new_with_partitioned_arenas(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        config: HashTableConfig,
    ) -> Self {
        // Repartition transfers raw aggregate state addresses between payloads. Separate arenas
        // are only safe for a final hash table whose partitions will never be repartitioned.
        assert!(
            !config.partial_agg,
            "partition-local aggregate arenas cannot be used by a repartitioning hash table"
        );
        let capacity = Self::initial_capacity();
        let partition_count = 1 << config.initial_radix_bits;
        let arenas = (0..partition_count)
            .map(|_| Arc::new(Bump::new()))
            .collect();
        Self {
            direct_append: false,
            current_radix_bits: config.initial_radix_bits,
            payload: PartitionedPayload::new_with_start_bit(
                group_types,
                aggrs,
                partition_count,
                config.partition_start_bit,
                arenas,
            ),
            hash_index: HashIndex::with_capacity(capacity),
            config,
            hash_index_resize_count: 0,
        }
    }

    pub fn into_payloads(self) -> Vec<Payload> {
        self.payload
            .into_bucket_payloads()
            .map(|(_, payload)| payload)
            .collect()
    }

    pub fn new_directly(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        config: HashTableConfig,
        capacity: usize,
        arena: Arc<Bump>,
        need_init_entry: bool,
    ) -> Self {
        debug_assert!(capacity.is_power_of_two());
        // if need_init_entry is false, we will directly append rows without probing hash index
        // so we can use a dummy hash index, which is not allowed to insert any entry
        let hash_index = if need_init_entry {
            HashIndex::with_capacity(capacity)
        } else {
            HashIndex::dummy()
        };
        Self {
            direct_append: !need_init_entry,
            current_radix_bits: config.initial_radix_bits,
            payload: PartitionedPayload::new_with_start_bit(
                group_types,
                aggrs,
                1 << config.initial_radix_bits,
                config.partition_start_bit,
                vec![arena],
            ),
            hash_index,
            config,
            hash_index_resize_count: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.payload.len()
    }

    pub fn add_groups(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        agg_states: ProjectedBlock,
        row_count: usize,
    ) -> Result<usize> {
        let mut group_state = GroupKeyState::default();
        if !self.direct_append {
            group_state.init_codes(group_columns, row_count);
        }
        if row_count <= BATCH_SIZE {
            self.add_groups_inner(
                state,
                group_columns,
                params,
                agg_states,
                row_count,
                &mut group_state,
            )
        } else {
            // Probing by codes hashes only one representative row per code combination, so
            // hashing every row of the block up front is only worth it without codes.
            group_state.dict_hashes = (group_state.code_addresses.is_empty()
                && group_columns.iter().any(BlockEntry::is_dict_encoded))
            .then(|| {
                let mut hashes = vec![0; row_count];
                group_hash_entries(group_columns, &mut hashes);
                hashes
            });
            let mut new_count = 0;
            for start in (0..row_count).step_by(BATCH_SIZE) {
                let end = (start + BATCH_SIZE).min(row_count);
                let step_group_columns = group_columns
                    .iter()
                    .map(|entry| entry.slice(start..end))
                    .collect::<Vec<_>>();

                let step_params: Vec<Vec<BlockEntry>> = params
                    .iter()
                    .map(|c| c.iter().map(|x| x.slice(start..end)).collect())
                    .collect();
                let step_params = step_params.iter().map(|v| v.into()).collect::<Vec<_>>();
                let agg_states = agg_states
                    .iter()
                    .map(|c| c.slice(start..end))
                    .collect::<Vec<_>>();

                group_state.row_offset = start;
                new_count += self.add_groups_inner(
                    state,
                    (&step_group_columns).into(),
                    &step_params,
                    (&agg_states).into(),
                    end - start,
                    &mut group_state,
                )?;
            }
            Ok(new_count)
        }
    }

    fn fill_group_hashes(
        group_columns: ProjectedBlock,
        hashes: &mut [u64],
        dict_hashes: Option<&[u64]>,
        rows: impl IntoIterator<Item = usize>,
    ) {
        if let Some(dict_hashes) = dict_hashes {
            for (hash, row) in hashes.iter_mut().zip(rows) {
                *hash = dict_hashes[row];
            }
        } else {
            group_hash_entries(group_columns, hashes);
        }
    }

    // Add new groups and combine the states
    fn add_groups_inner(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        agg_states: ProjectedBlock,
        row_count: usize,
        group_state: &mut GroupKeyState,
    ) -> Result<usize> {
        #[cfg(debug_assertions)]
        {
            for (i, group_column) in group_columns.iter().enumerate() {
                if !self.payload.group_types[i].matches_physical_type(&group_column.data_type()) {
                    return Err(databend_common_exception::ErrorCode::UnknownException(
                        format!(
                            "group_column type not match in index {}, expect: {:?}, actual: {:?}",
                            i,
                            self.payload.group_types[i],
                            group_column.data_type()
                        ),
                    ));
                }
            }
        }

        let new_group_count = if self.direct_append {
            state.row_count = row_count;
            Self::fill_group_hashes(
                group_columns,
                &mut state.group_hashes[..row_count],
                group_state.dict_hashes(row_count),
                0..row_count,
            );
            for (i, entry) in state.empty_vector[..row_count].iter_mut().enumerate() {
                *entry = i.into();
            }
            self.payload.append_rows(state, row_count, group_columns);
            row_count
        } else if let Some(new_group_count) =
            self.probe_and_create_by_codes(state, group_columns, row_count, group_state)
        {
            new_group_count
        } else {
            state.row_count = row_count;
            Self::fill_group_hashes(
                group_columns,
                &mut state.group_hashes[..row_count],
                group_state.dict_hashes(row_count),
                0..row_count,
            );
            self.probe_and_create(state, group_columns, row_count)
        };

        if !self.payload.aggrs.is_empty() {
            for i in 0..row_count {
                state.state_places[i] = state.addresses[i].state_addr(&self.payload.row_layout);
            }

            let state_places = &state.state_places.as_slice()[0..row_count];
            let states_layout = self.payload.row_layout.states_layout.as_ref().unwrap();
            if agg_states.is_empty() {
                for ((func, params), loc) in self
                    .payload
                    .aggrs
                    .iter()
                    .zip(params.iter())
                    .zip(states_layout.states_loc.iter())
                {
                    func.accumulate_keys(AggregateStateSet::new(state_places, loc), *params)?;
                }
            } else {
                for ((func, state), loc) in self
                    .payload
                    .aggrs
                    .iter()
                    .zip(agg_states.iter())
                    .zip(states_layout.states_loc.iter())
                {
                    func.merge_serialized(AggregateStateSet::new(state_places, loc), state)?;
                }
            }
        }

        if self.config.partial_agg {
            // check size
            if self.hash_index.count() + BATCH_SIZE > self.hash_index.resize_threshold()
                && self.hash_index.capacity() >= self.config.max_partial_capacity
            {
                self.clear_ht();
            }

            // check maybe_repartition
            if self.maybe_repartition() {
                self.clear_ht();
                group_state.clear_codes();
            }
        }

        Ok(new_group_count)
    }

    /// Probes once per distinct combination of codes when every group key is an encoded column
    /// with few codes (see [`EncodedColumn::codes`]). The mixed-radix code indexes addresses
    /// shared by batches of one input block. Unseen codes are deduplicated within the batch,
    /// and their representatives still go through the regular value-aware hash and probe.
    ///
    /// For example, grouping by `(flag, status)` with codes `N=0, R=1` and `F=0, O=1`:
    ///
    /// ```text
    /// row:                0     1     2     3     4     5
    /// keys:             (N,F) (R,O) (N,F) (N,F) (R,O) (N,F)
    /// flag + status * 2:  0     3     0     0     3     0     1. code of each row
    /// representatives:   [0, 1]                               2. first row of each code
    /// probe rows 0 and 1:  (N,F) -> a0, (R,O) -> a1           3. regular hash and probe
    /// addresses:          a0    a1    a0    a0    a1    a0    4. by code
    /// ```
    ///
    /// `None` if the keys do not qualify (a key is not encoded, or there are not several rows
    /// per combination); nothing is changed then.
    fn probe_and_create_by_codes(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        row_count: usize,
        group_state: &mut GroupKeyState,
    ) -> Option<usize> {
        if group_state.code_addresses.is_empty() {
            return None;
        }

        // Code of a row = mixed radix number of its independent keys' codes.
        let mut row_codes = vec![0u32; row_count];
        for &(column, stride) in &group_state.code_columns {
            let (codes, _) = group_columns[column].as_column()?.as_encoded()?.codes();
            for (row_code, code) in row_codes.iter_mut().zip(codes.iter()) {
                *row_code += (code as usize * stride) as u32;
            }
        }

        // Only codes without a cached address need a representative in this batch.
        let mut representatives = Vec::new();
        for (row, code) in row_codes.iter().enumerate() {
            if !group_state.code_addresses[*code as usize]
                .as_ptr()
                .is_null()
            {
                continue;
            }
            let rank = &mut group_state.rank_of_code[*code as usize];
            if *rank == u32::MAX {
                *rank = representatives.len() as u32;
                representatives.push(row as u32);
            }
        }

        let new_group_count = if representatives.is_empty() {
            0
        } else {
            // The representative rows stay encoded.
            let keys = group_columns
                .iter()
                .map(|entry| entry.take(representatives.as_slice()))
                .collect::<Result<Vec<_>>>()
                .inspect_err(|_| group_state.rank_of_code.fill(u32::MAX))
                .ok()?;

            let distinct = representatives.len();
            state.row_count = distinct;
            Self::fill_group_hashes(
                (&keys).into(),
                &mut state.group_hashes[..distinct],
                group_state.dict_hashes(row_count),
                representatives.iter().map(|row| *row as usize),
            );
            // Functions map dictionary values without changing codes, so distinct codes can
            // produce equal values. Probe representatives normally to find their actual groups
            // before reusing the addresses for rows with the same code.
            let count = self.probe_and_create(state, (&keys).into(), distinct);
            for (rank, row) in representatives.iter().enumerate() {
                let code = row_codes[*row as usize] as usize;
                group_state.code_addresses[code] = state.addresses[rank];
                group_state.rank_of_code[code] = u32::MAX;
            }
            count
        };

        for (address, code) in state.addresses[..row_count].iter_mut().zip(&row_codes) {
            *address = group_state.code_addresses[*code as usize];
        }
        state.row_count = row_count;
        Some(new_group_count)
    }

    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        row_count: usize,
    ) -> usize {
        // exceed capacity or should resize
        if row_count + self.hash_index.count() > self.hash_index.resize_threshold() {
            let new_capacity = self.next_resize_capacity();
            self.resize(new_capacity);
        }

        let mut adapter = AdapterImpl {
            payload: &mut self.payload,
            group_columns,
        };
        self.hash_index
            .probe_and_create(state, row_count, &mut adapter)
    }

    fn next_resize_capacity(&self) -> usize {
        // Use *4 for the first few resizes, then switch back to *2.
        // SMALL_CAPACITY_RESIZE_COUNT = 4:
        //
        // | Quad resizes used | Equivalent double-resize steps |
        // | 0                 | 0                              |
        // | 1                 | 2                              |
        // | 2                 | 4                              |
        // | 3                 | 6                              |
        // | 4                 | 8                              |
        // | 5                 | 9                              |
        // | 6                 | 10                             |
        let current = self.hash_index.capacity();
        if self.hash_index_resize_count < SMALL_CAPACITY_RESIZE_COUNT {
            current * 4
        } else {
            current * 2
        }
    }

    pub fn combine(&mut self, other: Self, flush_state: &mut PayloadFlushState) -> Result<()> {
        self.combine_payloads(&other.payload, flush_state)
    }

    pub fn combine_payloads(
        &mut self,
        payloads: &PartitionedPayload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        for payload in payloads.payloads.iter() {
            self.combine_payload(payload, flush_state)?;
        }
        Ok(())
    }

    pub fn combine_payload(
        &mut self,
        payload: &Payload,
        flush_state: &mut PayloadFlushState,
    ) -> Result<()> {
        flush_state.clear();

        while payload.flush(flush_state) {
            let row_count = flush_state.row_count;

            let state = &mut *flush_state.probe_state;
            let _ = self.probe_and_create(state, (&flush_state.group_columns).into(), row_count);

            let places = &mut state.state_places[..row_count];

            // set state places
            if !self.payload.aggrs.is_empty() {
                for (place, ptr) in places.iter_mut().zip(&state.addresses[..row_count]) {
                    *place = ptr.state_addr(&self.payload.row_layout)
                }
            }

            if let Some(layout) = self.payload.row_layout.states_layout.as_ref() {
                let rhses = &flush_state.state_places[..row_count];
                for (aggr, loc) in self.payload.aggrs.iter().zip(layout.states_loc.iter()) {
                    for (place, rhs) in places.iter().zip(rhses.iter()) {
                        aggr.merge_states(AggrState::new(*place, loc), AggrState::new(*rhs, loc))?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn merge_result(&mut self, flush_state: &mut PayloadFlushState) -> Result<bool> {
        if !self.payload.flush(flush_state) {
            return Ok(false);
        }

        let row_count = flush_state.row_count;
        flush_state.aggregate_results.clear();
        if let Some(states_layout) = self.payload.row_layout.states_layout.as_ref() {
            for (aggr, loc) in self
                .payload
                .aggrs
                .iter()
                .zip(states_layout.states_loc.iter().cloned())
            {
                let return_type = aggr.signature().return_type.clone();
                let mut builder = ColumnBuilder::with_capacity(&return_type, row_count * 4);

                for place in &flush_state.state_places.as_slice()[0..row_count] {
                    aggr.merge_result(AggrState::new(*place, &loc), &mut builder)?;
                }
                flush_state.aggregate_results.push(builder.build().into());
            }
        }
        Ok(true)
    }

    fn maybe_repartition(&mut self) -> bool {
        // already final stage or the max radix bits
        if !self.config.partial_agg || (self.current_radix_bits == self.config.max_radix_bits) {
            return false;
        }

        let bytes_per_partition = self.payload.memory_size() / self.payload.partition_count();

        let mut new_radix_bits = self.current_radix_bits;

        if bytes_per_partition > MAX_PAGE_SIZE * self.config.block_fill_factor as usize {
            new_radix_bits += self.config.repartition_radix_bits_incr;
        }

        loop {
            let current_max_radix_bits = self.config.current_max_radix_bits.load(Ordering::SeqCst);
            if current_max_radix_bits < new_radix_bits
                && self
                    .config
                    .current_max_radix_bits
                    .compare_exchange(
                        current_max_radix_bits,
                        new_radix_bits,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_err()
            {
                continue;
            }
            break;
        }

        let current_max_radix_bits = self.config.current_max_radix_bits.load(Ordering::SeqCst);

        if current_max_radix_bits > self.current_radix_bits {
            let temp_payload = PartitionedPayload::new(
                self.payload.group_types.clone(),
                self.payload.aggrs.clone(),
                1,
                vec![Arc::new(Bump::new())],
            );
            let payload = std::mem::replace(&mut self.payload, temp_payload);
            let mut state = PayloadFlushState::default();

            self.current_radix_bits = current_max_radix_bits;
            self.payload = payload.repartition(1 << current_max_radix_bits, &mut state);
            return true;
        }
        false
    }

    // scan payload to reconstruct PointArray
    fn resize(&mut self, new_capacity: usize) {
        if self.config.partial_agg {
            let target = new_capacity.min(self.config.max_partial_capacity);
            if target == self.hash_index.capacity() {
                return;
            }
            self.hash_index_resize_count += 1;
            self.hash_index = HashIndex::with_capacity(target);
            return;
        }

        self.hash_index_resize_count += 1;

        let mut hash_index = HashIndex::with_capacity(new_capacity);
        // iterate over payloads and copy to new entries
        for payload in self.payload.payloads.iter() {
            for page in payload.pages.iter() {
                for idx in 0..page.rows {
                    let row_ptr = payload.data_ptr(page, idx);
                    let hash = row_ptr.hash(&payload.row_layout);

                    hash_index.probe_slot_and_set(hash, row_ptr);
                }
            }
        }

        self.hash_index = hash_index
    }

    fn initial_capacity() -> usize {
        8192 * 4
    }

    pub fn get_capacity_for_count(count: usize) -> usize {
        ((count.max(Self::initial_capacity()) as f64 * LOAD_FACTOR) as usize).next_power_of_two()
    }

    fn clear_ht(&mut self) {
        self.payload.mark_min_cardinality();
        self.hash_index.reset();
    }

    pub fn allocated_bytes(&self) -> usize {
        self.payload.memory_size()
            + self
                .payload
                .arenas
                .iter()
                .map(|arena| arena.allocated_bytes())
                .sum::<usize>()
            + self.hash_index.allocated_bytes()
    }

    pub fn hash_index_resize_count(&self) -> usize {
        self.hash_index_resize_count
    }
}
