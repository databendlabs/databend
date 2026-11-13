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

use bumpalo::Bump;
use databend_common_exception::Result;
use smallvec::SmallVec;

use super::BATCH_SIZE;
use super::DictValueHashes;
use super::HASH_INDEX_LOAD_FACTOR;
use super::HashIndex;
use super::HashTableConfig;
use super::Payload;
use super::StateAddr;
use super::group_hash_entries;
use super::hash_index::PREFETCH_DISTANCE;
use super::hash_index_adapter::AdapterImpl;
use super::partial_controller::DistinctSampler;
use super::partial_controller::PartialAddStats;
use super::partitioned_payload::PartitionedPayload;
use super::payload::GroupRows;
use super::payload::merge_flushed_states;
use super::payload_flush::PayloadFlushState;
use super::probe_state::ProbeState;
use super::row_ptr::RowPtr;
use crate::BlockEntry;
use crate::Column;
use crate::ProjectedBlock;
use crate::aggregate::aggregate_function::AggregateCallRef;
use crate::aggregate::aggregate_function::AggregateStateSet;
use crate::types::DataType;

const SMALL_CAPACITY_RESIZE_COUNT: usize = 4;

#[derive(Default)]
struct GroupKeyState {
    dict_hashes: Option<DictValueHashes>,
    // Independent code dimensions: (group column index, mixed-radix stride).
    code_columns: SmallVec<[(usize, usize); 4]>,
    rank_of_code: Vec<u32>,
    code_addresses: Vec<RowPtr>,
    /// The states of the groups of `code_addresses`.
    code_places: Vec<StateAddr>,
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
        self.code_places.resize(code_space, StateAddr::null());
    }
}

/// Index capacity of a new table, and of a partial table restarted after it emits.
const INITIAL_CAPACITY: usize = 8192 * 4;

pub struct AggregateHashTable {
    // Hash index entries store the ids of rows in these payload pages. Any path that replaces
    // the payload must clear or rebuild the index before probing it again.
    pub payload: PartitionedPayload,
    config: HashTableConfig,

    hash_index: HashIndex,
    hash_index_resize_count: usize,
    /// The partial index restarted in place since the payload was taken, so the payload holds
    /// more rows than the index bounds.
    partial_restarted: bool,
}

impl AggregateHashTable {
    pub fn new(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        config: HashTableConfig,
        arena: Arc<Bump>,
    ) -> Self {
        let payload = Self::new_payload(&config, group_types, aggrs, arena);
        Self::with_payload(config, payload)
    }

    fn new_payload(
        config: &HashTableConfig,
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        arena: Arc<Bump>,
    ) -> PartitionedPayload {
        if config.route_partitions > 1 {
            return PartitionedPayload::new_routed(
                group_types,
                aggrs,
                config.route_partitions,
                vec![arena],
            );
        }
        PartitionedPayload::new_with_start_bit(
            group_types,
            aggrs,
            1 << config.initial_radix_bits,
            config.partition_start_bit,
            vec![arena],
        )
    }

    pub fn new_with_partitioned_arenas(
        group_types: Vec<DataType>,
        aggrs: Vec<AggregateCallRef>,
        config: HashTableConfig,
    ) -> Self {
        assert!(
            !config.partial_agg,
            "partition-local aggregate arenas cannot be used by a repartitioning hash table"
        );
        let partition_count = 1 << config.initial_radix_bits;
        let arenas = (0..partition_count)
            .map(|_| Arc::new(Bump::new()))
            .collect();
        let payload = PartitionedPayload::new_with_start_bit(
            group_types,
            aggrs,
            partition_count,
            config.partition_start_bit,
            arenas,
        );
        Self::with_payload(config, payload)
    }

    fn with_payload(config: HashTableConfig, payload: PartitionedPayload) -> Self {
        let hash_index = Self::initial_index(&payload);
        Self {
            payload,
            config,
            hash_index,
            hash_index_resize_count: 0,
            partial_restarted: false,
        }
    }

    /// An index of the initial capacity, for the rows `payload` may add until it grows.
    fn initial_index(payload: &PartitionedPayload) -> HashIndex {
        let new_rows = HashIndex::threshold_for_capacity(HashIndex::slots_for(INITIAL_CAPACITY));
        HashIndex::with_capacity(INITIAL_CAPACITY, payload.max_row_id(new_rows))
    }

    pub fn into_payloads(self) -> Vec<Payload> {
        self.payload
            .into_bucket_payloads()
            .map(|(_, payload)| payload)
            .collect()
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
        group_state.init_codes(group_columns, row_count);
        // Probing by codes hashes only one representative row per code combination, so
        // hashing the dictionaries up front is only worth it without codes.
        if group_state.code_addresses.is_empty() {
            group_state.dict_hashes = DictValueHashes::new(group_columns, row_count);
        }

        let mut new_count = 0;
        Self::for_each_batch(
            group_columns,
            params,
            agg_states,
            row_count,
            |group_columns, params, agg_states, _, rows| {
                new_count += self.add_groups_inner(
                    state,
                    group_columns,
                    params,
                    agg_states,
                    rows,
                    &mut group_state,
                )?;
                Ok(())
            },
        )?;
        Ok(new_count)
    }

    /// Aggregate raw input rows, reusing the probe state of `flush_state`.
    pub fn add_raw_groups(
        &mut self,
        flush_state: &mut PayloadFlushState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        row_count: usize,
    ) -> Result<usize> {
        self.add_groups(
            &mut flush_state.probe_state,
            group_columns,
            params,
            (&[]).into(),
            row_count,
        )
    }

    /// Merge serialized aggregate states into their groups, reusing the probe state of
    /// `flush_state`.
    pub fn add_serialized_groups(
        &mut self,
        flush_state: &mut PayloadFlushState,
        group_columns: ProjectedBlock,
        agg_states: ProjectedBlock,
        row_count: usize,
    ) -> Result<usize> {
        self.add_groups(
            &mut flush_state.probe_state,
            group_columns,
            &[],
            agg_states,
            row_count,
        )
    }

    /// Adaptive partial aggregation entry point. When the index is full it grows at once to
    /// `capacity`, or restarts in place once it is that large; the group hashes are fed to
    /// `sampler` once it has started. `dict_hashes` hash the dictionaries of the group columns
    /// once for all the batches, see [`DictValueHashes`].
    #[allow(clippy::too_many_arguments)]
    pub fn add_groups_partial(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        row_count: usize,
        capacity: usize,
        mut sampler: Option<&mut DistinctSampler>,
        dict_hashes: Option<&DictValueHashes>,
    ) -> Result<PartialAddStats> {
        debug_assert!(self.config.partial_agg);
        let mut stats = PartialAddStats::default();
        Self::for_each_batch(
            group_columns,
            params,
            (&[]).into(),
            row_count,
            |group_columns, params, _, _, rows| {
                Self::fill_group_hashes(
                    group_columns,
                    &mut state.group_hashes[..rows],
                    dict_hashes,
                );
                self.add_groups_partial_inner(
                    state,
                    group_columns,
                    params,
                    rows,
                    capacity,
                    sampler.as_deref_mut(),
                    &mut stats,
                )
            },
        )?;
        Ok(stats)
    }

    /// Calls `f` with each batch of at most `BATCH_SIZE` rows and the offset of its first row.
    fn for_each_batch(
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        agg_states: ProjectedBlock,
        row_count: usize,
        mut f: impl FnMut(ProjectedBlock, &[ProjectedBlock], ProjectedBlock, usize, usize) -> Result<()>,
    ) -> Result<()> {
        if row_count <= BATCH_SIZE {
            f(group_columns, params, agg_states, 0, row_count)
        } else {
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

                f(
                    (&step_group_columns).into(),
                    &step_params,
                    (&agg_states).into(),
                    start,
                    end - start,
                )?;
            }
            Ok(())
        }
    }

    /// Hash a batch of group keys, by the hashes of their dictionaries when there are some.
    fn fill_group_hashes(
        group_columns: ProjectedBlock,
        hashes: &mut [u64],
        dict_hashes: Option<&DictValueHashes>,
    ) {
        match dict_hashes {
            Some(dict_hashes) => dict_hashes.group_hash_entries(group_columns, hashes),
            None => group_hash_entries(group_columns, hashes),
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

        let new_group_count = if let Some(new_group_count) =
            self.probe_and_create_by_codes(state, group_columns, row_count, group_state)
        {
            new_group_count
        } else {
            state.row_count = row_count;
            Self::fill_group_hashes(
                group_columns,
                &mut state.group_hashes[..row_count],
                group_state.dict_hashes.as_ref(),
            );
            self.probe_and_create(state, GroupRows::Columns(group_columns), row_count, None)
        };

        self.accumulate_states(state, params, agg_states, row_count)?;
        Ok(new_group_count)
    }

    #[allow(clippy::too_many_arguments)]
    fn add_groups_partial_inner(
        &mut self,
        state: &mut ProbeState,
        group_columns: ProjectedBlock,
        params: &[ProjectedBlock],
        row_count: usize,
        capacity: usize,
        mut sampler: Option<&mut DistinctSampler>,
        stats: &mut PartialAddStats,
    ) -> Result<()> {
        // The caller filled `state.group_hashes`.
        state.row_count = row_count;
        stats.rows += row_count;

        if row_count + self.hash_index.count() > self.hash_index.resize_threshold() {
            if capacity > self.hash_index.capacity() {
                // Grow at once. The index was not restarted since the payload was emitted, so
                // the payload holds one row per key and can rebuild it.
                self.resize(capacity);
            } else {
                // The first window holds exactly the distinct groups seen so far, one row each in
                // the payload, as the index was not restarted since the payload was taken.
                if let Some(sampler) = sampler.as_deref_mut()
                    && !sampler.is_started()
                {
                    sampler.start(self.hash_index.count());
                    self.payload
                        .for_each_hash(|hash| sampler.observe_hash(hash));
                }
                // Restart the index in place and keep appending to the payload.
                self.restart_index();
                self.partial_restarted = true;
                stats.windows += 1;
            }
        }
        if let Some(sampler) = sampler.filter(|sampler| sampler.is_started()) {
            sampler.observe(&state.group_hashes[..row_count]);
        }

        let mut adapter = AdapterImpl {
            payload: &mut self.payload,
            groups: GroupRows::Columns(group_columns),
            source_states: None,
        };
        stats.new_groups += self
            .hash_index
            .probe_and_create(state, row_count, &mut adapter);
        self.accumulate_states(state, params, (&[]).into(), row_count)
    }

    fn accumulate_states(
        &self,
        state: &mut ProbeState,
        params: &[ProjectedBlock],
        agg_states: ProjectedBlock,
        row_count: usize,
    ) -> Result<()> {
        if !self.payload.aggrs.is_empty() {
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
        Ok(())
    }

    /// Bytes of an index of `capacity` slots.
    pub fn index_bytes_for(capacity: usize) -> usize {
        HashIndex::allocated_bytes_for(capacity)
    }

    /// Index capacity that holds `groups` groups without growing.
    pub fn capacity_for_groups(groups: usize) -> usize {
        // The index rounds up to whole chunks, a power of two of them: round the slots only once.
        HashIndex::slots_for(
            ((groups as f64 * HASH_INDEX_LOAD_FACTOR) as usize + 1).max(INITIAL_CAPACITY),
        )
    }

    /// Bytes held by the groups of the partial table, excluding the index.
    pub fn partial_payload_bytes(&self) -> usize {
        self.payload.memory_size() + self.arena_allocated_bytes()
    }

    /// Whether the partial index restarted in place since the payload was last taken, so that
    /// the payload is no longer bounded by the index.
    pub fn partial_restarted(&self) -> bool {
        self.partial_restarted
    }

    /// Move the groups out and restart with an empty index of the initial capacity.
    pub fn take_partial_payload(&mut self) -> PartitionedPayload {
        let fresh = Self::new_payload(
            &self.config,
            self.payload.group_types.clone(),
            self.payload.aggrs.clone(),
            Arc::new(Bump::new()),
        );
        let payload = std::mem::replace(&mut self.payload, fresh);
        self.hash_index = HashIndex::dummy();
        self.hash_index = Self::initial_index(&self.payload);
        self.partial_restarted = false;
        payload
    }

    fn arena_allocated_bytes(&self) -> usize {
        self.payload
            .arenas
            .iter()
            .map(|arena| arena.allocated_bytes())
            .sum::<usize>()
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
            group_hash_entries((&keys).into(), &mut state.group_hashes[..distinct]);
            // Functions map dictionary values without changing codes, so distinct codes can
            // produce equal values. Probe representatives normally to find their actual groups
            // before reusing the addresses for rows with the same code.
            let count =
                self.probe_and_create(state, GroupRows::Columns((&keys).into()), distinct, None);
            for (rank, row) in representatives.iter().enumerate() {
                let code = row_codes[*row as usize] as usize;
                group_state.code_addresses[code] = state.addresses[rank];
                group_state.code_places[code] = state.state_places[rank];
                group_state.rank_of_code[code] = u32::MAX;
            }
            count
        };

        for (row, code) in row_codes.iter().enumerate() {
            state.addresses[row] = group_state.code_addresses[*code as usize];
            state.state_places[row] = group_state.code_places[*code as usize];
        }
        state.row_count = row_count;
        Some(new_group_count)
    }

    fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        groups: GroupRows,
        row_count: usize,
        source_states: Option<&[StateAddr; BATCH_SIZE]>,
    ) -> usize {
        // exceed capacity or should resize
        if row_count + self.hash_index.count() > self.hash_index.resize_threshold() {
            let new_capacity = self.next_resize_capacity();
            self.resize(new_capacity);
        }

        let mut adapter = AdapterImpl {
            payload: &mut self.payload,
            groups,
            source_states,
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

        // Probe with the rows, without building their group columns.
        while payload.scan_rows(flush_state) {
            let row_count = flush_state.row_count;

            let state = &mut *flush_state.probe_state;
            state.new_groups[..row_count].fill(false);
            let _ = self.probe_and_create(
                state,
                GroupRows::Rows(&flush_state.addresses),
                row_count,
                Some(&flush_state.state_places),
            );

            let Some(layout) = self.payload.row_layout.states_layout.as_ref() else {
                continue;
            };
            let places = &mut state.state_places[..row_count];
            let rhses = &mut flush_state.state_places[..row_count];
            let aggrs = self.payload.aggrs.iter().zip(layout.states_loc.iter());

            // The new groups initialized the states that need manual drop,
            for (aggr, loc) in aggrs.clone().filter(|(a, _)| a.state().need_manual_drop()) {
                aggr.merge_states_keys(AggregateStateSet::new(places, loc), rhses)?;
            }
            // and copied the others: only merge those of the found groups.
            let mut found = 0;
            for row in 0..row_count {
                if !state.new_groups[row] {
                    places[found] = places[row];
                    rhses[found] = rhses[row];
                    found += 1;
                }
            }
            for (aggr, loc) in aggrs.filter(|(a, _)| !a.state().need_manual_drop()) {
                let places = AggregateStateSet::new(&places[..found], loc);
                aggr.merge_states_keys(places, &rhses[..found])?;
            }
        }

        Ok(())
    }

    pub fn merge_result(&mut self, flush_state: &mut PayloadFlushState) -> Result<bool> {
        if !self.payload.flush(flush_state) {
            return Ok(false);
        }

        merge_flushed_states(&self.payload.row_layout, &self.payload.aggrs, flush_state)?;
        Ok(true)
    }

    /// Rebuild the index from the payload with at least `new_capacity` slots.
    fn resize(&mut self, new_capacity: usize) {
        self.hash_index_resize_count += 1;

        // The new index is rebuilt from the payload alone: free the old one before allocating
        // it, so that the two never take memory at the same time.
        self.hash_index = HashIndex::dummy();
        // Every row of the payload is in the index, and the index grows again before it holds
        // more groups than its threshold: the ids it can meet until then are known now.
        let new_capacity = HashIndex::slots_for(new_capacity);
        let new_rows =
            HashIndex::threshold_for_capacity(new_capacity).saturating_sub(self.payload.len());
        let mut hash_index =
            HashIndex::with_capacity(new_capacity, self.payload.max_row_id(new_rows));
        self.payload.renumber_pages();
        let row_ids = &self.payload.row_ids;
        for (partition, payload) in self.payload.payloads.iter().enumerate() {
            for (page_index, page) in payload.pages.iter().enumerate() {
                let first_row_id = row_ids.first_row_id(partition, page_index);
                for idx in 0..page.rows {
                    // Setting a slot reads its chunk first: fetch the chunks of the rows ahead,
                    // the rows of a page being read in order.
                    if idx + PREFETCH_DISTANCE < page.rows {
                        let ahead = payload.data_ptr(page, idx + PREFETCH_DISTANCE);
                        hash_index.prefetch(ahead.hash(&payload.row_layout));
                    }
                    let row_ptr = payload.data_ptr(page, idx);
                    let hash = row_ptr.hash(&payload.row_layout);

                    hash_index.probe_slot_and_set(hash, first_row_id | idx as u64);
                }
            }
        }

        self.hash_index = hash_index
    }

    /// Empty the index for the next window of a partial table, whose payload keeps its rows and
    /// grows by at most what the index holds: the ids of the window must fit the width of the
    /// index, as they do after a resize.
    fn restart_index(&mut self) {
        let capacity = self.hash_index.capacity();
        let max_id = self.payload.max_row_id(self.hash_index.resize_threshold());
        if self.hash_index.holds_id(max_id) {
            self.hash_index.reset();
        } else {
            // Free the old index before allocating the new one, as `resize` does.
            self.hash_index = HashIndex::dummy();
            self.hash_index = HashIndex::with_capacity(capacity, max_id);
        }
    }

    pub fn hash_index_resize_count(&self) -> usize {
        self.hash_index_resize_count
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bumpalo::Bump;

    use super::*;
    use crate::DataBlock;
    use crate::FromData;
    use crate::types::NumberDataType;
    use crate::types::UInt64Type;

    fn key_blocks(groups: u64, repeats: u64) -> Vec<DataBlock> {
        // Every key `repeats` times, spread over the blocks in a scrambled order: multiplying by a
        // prime that does not divide `rows` permutes 0..rows.
        let rows = groups * repeats;
        let keys = (0..rows)
            .map(|i| (i * 1_000_003 % rows) % groups)
            .collect::<Vec<_>>();
        keys.chunks(8192)
            .map(|chunk| DataBlock::new_from_columns(vec![UInt64Type::from_data(chunk.to_vec())]))
            .collect()
    }

    fn add_blocks(ht: &mut AggregateHashTable, blocks: &[DataBlock]) -> usize {
        let mut state = ProbeState::default();
        let mut new_groups = 0;
        for block in blocks {
            let groups = ProjectedBlock::project(&[0], block);
            new_groups += ht
                .add_groups(&mut state, groups, &[], (&[]).into(), block.num_rows())
                .unwrap();
        }
        new_groups
    }

    #[test]
    fn test_hash_index_widens_ids_with_the_rows() {
        let groups = 200_000;
        let blocks = key_blocks(groups, 3);
        // The ids of the table fit 4 bytes, unless the table counts pages that take them past.
        for (id_bits, id_bytes) in [(0, 4), (32, 8)] {
            let mut ht = AggregateHashTable::new(
                vec![DataType::Number(NumberDataType::UInt64)],
                vec![],
                HashTableConfig::default().with_initial_radix_bits(2),
                Arc::new(Bump::new()),
            );
            if id_bits != 0 {
                let pages = ht.payload.row_ids.pages_for_id_bits(id_bits);
                ht.payload.row_ids.set_extra_pages(pages);
            }

            assert_eq!(add_blocks(&mut ht, &blocks), groups as usize);
            assert_eq!(ht.payload.len(), groups as usize);
            assert_eq!(ht.hash_index.id_bytes(), id_bytes);
            // Every key finds its group again, so no two groups share an id.
            assert_eq!(add_blocks(&mut ht, &blocks), 0);
            assert_eq!(ht.payload.len(), groups as usize);
        }
    }

    #[test]
    fn test_row_ids_name_their_rows() {
        let groups = 200_000;
        let mut ht = AggregateHashTable::new(
            vec![DataType::Number(NumberDataType::UInt64)],
            vec![],
            HashTableConfig::default().with_initial_radix_bits(2),
            Arc::new(Bump::new()),
        );
        assert_eq!(add_blocks(&mut ht, &key_blocks(groups, 1)), groups as usize);
        assert!(ht.payload.payloads.iter().all(|p| p.pages.len() > 1));

        // Each id names its own row, numbered as the rows were appended or as the index was
        // rebuilt.
        for renumber in [false, true] {
            if renumber {
                ht.payload.renumber_pages();
            }
            let payload = &ht.payload;
            let mut rows = std::collections::HashSet::new();
            for (partition, p) in payload.payloads.iter().enumerate() {
                for (page_index, page) in p.pages.iter().enumerate() {
                    let first_row_id = payload.row_ids.first_row_id(partition, page_index);
                    for row in 0..page.rows {
                        let id = first_row_id | row as u64;
                        assert_eq!(payload.row_ptr(id), p.data_ptr(page, row));
                        assert!(rows.insert(id));
                    }
                }
            }
            assert_eq!(rows.len(), groups as usize);
        }
    }

    #[test]
    fn test_partial_restart_widens_ids_with_the_rows() {
        // Distinct keys over several windows of the initial index, whose payload keeps growing.
        let keys = 200_000;
        let blocks = key_blocks(keys, 1);
        for widen in [false, true] {
            let mut ht = AggregateHashTable::new(
                vec![DataType::Number(NumberDataType::UInt64)],
                vec![],
                HashTableConfig::partial_aggregate(),
                Arc::new(Bump::new()),
            );
            let capacity = ht.hash_index.capacity();
            let mut state = ProbeState::default();
            let mut new_groups = 0;
            for (i, block) in blocks.iter().enumerate() {
                // After the first window, the ids of the next windows outgrow 4 bytes.
                if widen && i == 1 {
                    assert_eq!(ht.hash_index.id_bytes(), 4);
                    let pages = ht.payload.row_ids.pages_for_id_bits(32);
                    ht.payload.row_ids.set_extra_pages(pages);
                }
                let groups = ProjectedBlock::project(&[0], block);
                let stats = ht
                    .add_groups_partial(
                        &mut state,
                        groups,
                        &[],
                        block.num_rows(),
                        capacity,
                        None,
                        None,
                    )
                    .unwrap();
                new_groups += stats.new_groups;
            }
            assert!(ht.partial_restarted());
            assert_eq!(ht.hash_index.capacity(), capacity);
            assert_eq!(ht.hash_index.id_bytes(), if widen { 8 } else { 4 });
            // Each key comes once, so it is a new group in its window.
            assert_eq!(new_groups, keys as usize);
            assert_eq!(ht.payload.len(), keys as usize);
        }
    }
}
