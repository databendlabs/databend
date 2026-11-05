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

use databend_common_base::hints::assume;
use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;
use databend_common_expression::KeyAccessor;

use crate::pipelines::processors::transforms::hash_join_table::RawEntry;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::hash_join_table::STRING_EARLY_SIZE;
use crate::pipelines::processors::transforms::hash_join_table::StringRawEntry;
use crate::pipelines::processors::transforms::hash_join_table::prefetch_read;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;

/// How many chains ahead of the current one a round prefetches the entry of.
const CHAIN_PREFETCH_DISTANCE: usize = 32;

/// Set on an active probe row once it has matched, used by streams that report unmatched rows.
const MATCHED_FLAG: u32 = 1 << 31;

/// Reads the hash table entries of one key type and compares them with the probe keys.
pub(super) trait ChainEntries {
    /// Whether the key of `entry` equals the key of probe row `row`.
    ///
    /// # Safety
    /// `entry` must point to a live entry of the hash table and `row` must be a probe row.
    unsafe fn matches(&self, row: usize, entry: u64) -> bool;

    /// The build row that `entry` points to.
    ///
    /// # Safety
    /// `entry` must point to a live entry of the hash table.
    unsafe fn row_ptr(&self, entry: u64) -> RowPtr;

    /// The next entry of the chain, 0 at the end of the chain.
    ///
    /// # Safety
    /// `entry` must point to a live entry of the hash table.
    unsafe fn next(&self, entry: u64) -> u64;
}

pub(super) struct FixedKeyEntries<K> {
    keys: Buffer<K>,
}

impl<K> FixedKeyEntries<K> {
    pub fn new(keys: Buffer<K>) -> Self {
        FixedKeyEntries { keys }
    }
}

impl<K: PartialEq> ChainEntries for FixedKeyEntries<K> {
    #[inline(always)]
    unsafe fn matches(&self, row: usize, entry: u64) -> bool {
        unsafe { *self.keys.get_unchecked(row) == (*(entry as *const RawEntry<K>)).key }
    }

    #[inline(always)]
    unsafe fn row_ptr(&self, entry: u64) -> RowPtr {
        unsafe { (*(entry as *const RawEntry<K>)).row_ptr }
    }

    #[inline(always)]
    unsafe fn next(&self, entry: u64) -> u64 {
        unsafe { (*(entry as *const RawEntry<K>)).next }
    }
}

pub(super) struct BinaryKeyEntries {
    keys: Box<dyn KeyAccessor<Key = [u8]>>,
}

impl BinaryKeyEntries {
    pub fn new(keys: Box<dyn KeyAccessor<Key = [u8]>>) -> Self {
        BinaryKeyEntries { keys }
    }
}

impl ChainEntries for BinaryKeyEntries {
    #[inline(always)]
    unsafe fn matches(&self, row: usize, entry: u64) -> bool {
        unsafe {
            let key = self.keys.key_unchecked(row);
            let raw_entry = &*(entry as *const StringRawEntry);
            // Compare `early` and the length of the string, the size of `early` is 4.
            let min_len = std::cmp::min(STRING_EARLY_SIZE, key.len());
            if raw_entry.length as usize != key.len()
                || key[0..min_len] != raw_entry.early[0..min_len]
            {
                return false;
            }
            let key_ref =
                std::slice::from_raw_parts(raw_entry.key as *const u8, raw_entry.length as usize);
            key == key_ref
        }
    }

    #[inline(always)]
    unsafe fn row_ptr(&self, entry: u64) -> RowPtr {
        unsafe { (*(entry as *const StringRawEntry)).row_ptr }
    }

    #[inline(always)]
    unsafe fn next(&self, entry: u64) -> u64 {
        unsafe { (*(entry as *const StringRawEntry)).next }
    }
}

/// Walks the hash chains of a probe block breadth first.
///
/// Each round advances every active chain by one entry. The chains of a round are independent,
/// so the entry `CHAIN_PREFETCH_DISTANCE` chains ahead can be prefetched: every hop of every
/// chain overlaps with the work on other chains instead of waiting for the previous hop.
///
/// The matches of one probe row are therefore not contiguous in the output; every join stream
/// only relies on the (probe row, build row) pairs, not on their order.
///
/// - `MATCHED`: only matched rows are returned; otherwise probe rows without any match are
///   returned in `ProbedRows::unmatched` as well.
/// - `MATCH_FIRST`: a probe row stops at its first match (the hash table keeps unique keys).
pub(super) struct ChainProbeStream<
    'a,
    E: ChainEntries,
    const MATCHED: bool,
    const MATCH_FIRST: bool,
> {
    entries: E,
    /// The probe row of each active chain; `MATCHED_FLAG` records a match if `!MATCHED`.
    rows: &'a mut Vec<u32>,
    /// The current entry of each active chain, aligned with `rows`.
    chains: Vec<u64>,
    /// Probe rows without any candidate entry, returned first if `!MATCHED`.
    unmatched: &'a [u32],
    returned_unmatched: bool,
    /// Number of active chains in the current round.
    active: usize,
    /// Position of the next chain to advance in the current round.
    pos: usize,
    /// Number of chains kept for the next round, compacted in place at the front.
    kept: usize,
}

impl<'a, E: ChainEntries + 'a, const MATCHED: bool, const MATCH_FIRST: bool>
    ChainProbeStream<'a, E, MATCHED, MATCH_FIRST>
{
    /// Creates the stream from the result of `HashJoinHashtableLike::probe`: `pointers[row]` is
    /// the first entry of the chain of probe row `row`, 0 if there is none. `rows` and
    /// `unmatched` are reused buffers; their previous content is discarded.
    pub fn from_pointers(
        entries: E,
        mut pointers: Vec<u64>,
        rows: &'a mut Vec<u32>,
        unmatched: &'a mut Vec<u32>,
    ) -> Box<dyn ProbeStream + 'a> {
        let num_rows = pointers.len();
        assert!(num_rows <= MATCHED_FLAG as usize);

        rows.clear();
        rows.reserve(num_rows);
        unmatched.clear();
        if !MATCHED {
            unmatched.reserve(num_rows);
        }

        // Keep the chains in place: the write position never passes the read position.
        let mut active = 0;
        for row in 0..num_rows {
            let entry = pointers[row];
            if entry != 0 {
                pointers[active] = entry;
                rows.push(row as u32);
                active += 1;
            } else if !MATCHED {
                unmatched.push(row as u32);
            }
        }
        pointers.truncate(active);

        Self::create(entries, pointers, rows, unmatched)
    }

    /// Creates the stream from the result of the early filtering probes: only the rows in
    /// `selection` (ascending) have a chain, `pointers[row]` is its first entry. `unmatched`
    /// holds the filtered out rows when `!MATCHED`.
    pub fn from_selection(
        entries: E,
        mut pointers: Vec<u64>,
        selection: &'a mut Vec<u32>,
        unmatched: &'a [u32],
    ) -> Box<dyn ProbeStream + 'a> {
        assert!(pointers.len() <= MATCHED_FLAG as usize);

        // `selection` is ascending, so `selection[pos] >= pos`: moving the chains to the front
        // in order never overwrites the entry of a row not moved yet.
        for (pos, row) in selection.iter().enumerate() {
            debug_assert!(*row as usize >= pos);
            pointers[pos] = pointers[*row as usize];
        }
        pointers.truncate(selection.len());

        Self::create(entries, pointers, selection, unmatched)
    }

    fn create(
        entries: E,
        chains: Vec<u64>,
        rows: &'a mut Vec<u32>,
        unmatched: &'a [u32],
    ) -> Box<dyn ProbeStream + 'a> {
        debug_assert_eq!(rows.len(), chains.len());
        Box::new(ChainProbeStream::<'a, E, MATCHED, MATCH_FIRST> {
            entries,
            active: chains.len(),
            rows,
            chains,
            unmatched,
            returned_unmatched: false,
            pos: 0,
            kept: 0,
        })
    }
}

impl<'a, E: ChainEntries, const MATCHED: bool, const MATCH_FIRST: bool> ProbeStream
    for ChainProbeStream<'a, E, MATCHED, MATCH_FIRST>
{
    fn advance(&mut self, res: &mut ProbedRows, max_rows: usize) -> Result<()> {
        if !MATCHED && !self.returned_unmatched {
            self.returned_unmatched = true;
            res.unmatched
                .extend(self.unmatched.iter().map(|row| *row as u64));
        }

        // Matches are written without a branch: the pair of every visited entry is written and
        // only kept when the keys match. Make room for `max_rows` pairs up front.
        let mut matched = res.matched_probe.len();
        assert_eq!(matched, res.matched_build.len());
        res.matched_probe.reserve(max_rows.saturating_sub(matched));
        res.matched_build.reserve(max_rows.saturating_sub(matched));
        let probe_out = res.matched_probe.as_mut_ptr();
        let build_out = res.matched_build.as_mut_ptr();

        'walk: while self.active != 0 {
            assume(self.active <= self.rows.len());
            assume(self.active <= self.chains.len());

            while self.pos < self.active {
                if matched >= max_rows || (!MATCHED && res.unmatched.len() >= max_rows) {
                    break 'walk;
                }

                let pos = self.pos;
                assume(self.kept <= pos);

                if pos + CHAIN_PREFETCH_DISTANCE < self.active {
                    prefetch_read(self.chains[pos + CHAIN_PREFETCH_DISTANCE] as *const u8);
                }

                let mut row = self.rows[pos];
                let entry = self.chains[pos];
                let probe_row = (row & !MATCHED_FLAG) as usize;

                // # Safety
                // `entry` is a non-zero entry of the hash table, `probe_row` is a probe row and
                // `matched < max_rows <= capacity` of both output vectors.
                let next = unsafe {
                    let is_match = self.entries.matches(probe_row, entry);
                    probe_out.add(matched).write(probe_row as u64);
                    build_out.add(matched).write(self.entries.row_ptr(entry));
                    matched += is_match as usize;

                    if !MATCHED {
                        row |= (is_match as u32) << 31;
                    }

                    match MATCH_FIRST && is_match {
                        true => 0,
                        false => self.entries.next(entry),
                    }
                };

                if !MATCHED && next == 0 && row & MATCHED_FLAG == 0 {
                    res.unmatched.push(probe_row as u64);
                }

                self.rows[self.kept] = row;
                self.chains[self.kept] = next;
                self.kept += (next != 0) as usize;
                self.pos += 1;
            }

            // The round is finished, the kept chains form the next round.
            self.active = self.kept;
            self.kept = 0;
            self.pos = 0;
        }

        // # Safety
        // The first `matched` pairs are initialized.
        unsafe {
            res.matched_probe.set_len(matched);
            res.matched_build.set_len(matched);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use databend_common_column::bitmap::Bitmap;
    use databend_common_exception::Result;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::HashMethodFixedKeys;
    use databend_common_expression::HashMethodSingleBinary;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::UInt64Type;

    use crate::pipelines::processors::transforms::FixedKeyHashJoinHashTable;
    use crate::pipelines::processors::transforms::SingleBinaryHashJoinHashTable;
    use crate::pipelines::processors::transforms::hash_join_table::BinaryHashJoinHashMap;
    use crate::pipelines::processors::transforms::hash_join_table::HashJoinHashMap;
    use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
    use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeHashStatistics;
    use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
    use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;

    const BUILD_CHUNKS: usize = 3;
    const BUILD_ROWS_PER_CHUNK: usize = 1000;
    const BUILD_DISTINCT_KEYS: u64 = 1700;
    const PROBE_ROWS: usize = 2600;
    const MAX_ROWS: [usize; 4] = [1, 3, 64, 4096];

    /// Build keys repeat about twice; the hash table is large enough for chains of different
    /// keys, duplicates are chained in insertion order across chunks.
    fn build_keys() -> Vec<Vec<u64>> {
        (0..BUILD_CHUNKS)
            .map(|chunk| {
                (0..BUILD_ROWS_PER_CHUNK)
                    .map(|row| {
                        ((chunk * BUILD_ROWS_PER_CHUNK + row) as u64 * 7919) % BUILD_DISTINCT_KEYS
                    })
                    .collect()
            })
            .collect()
    }

    /// About half of the probe keys exist in the build side.
    fn probe_keys() -> Vec<u64> {
        (0..PROBE_ROWS as u64)
            .map(|row| (row * 31) % (2 * BUILD_DISTINCT_KEYS))
            .collect()
    }

    fn probe_valids(with_nulls: bool) -> Option<Bitmap> {
        with_nulls.then(|| Bitmap::from_iter((0..PROBE_ROWS).map(|row| row % 13 != 0)))
    }

    /// Keys share a 4-byte prefix or differ in length, so the full comparison is exercised.
    fn binary_key(key: u64) -> Vec<u8> {
        match key % 3 {
            0 => format!("k{key}").into_bytes(),
            _ => format!("key_{key:06}").into_bytes(),
        }
    }

    /// The expected build rows (chunk, row) of every probe row, by nested loop.
    fn expected_matches(build: &[Vec<u64>], probe: &[u64]) -> Vec<Vec<(u32, u32)>> {
        probe
            .iter()
            .map(|key| {
                let mut rows = vec![];
                for (chunk, keys) in build.iter().enumerate() {
                    for (row, build_key) in keys.iter().enumerate() {
                        if build_key == key {
                            rows.push((chunk as u32, row as u32));
                        }
                    }
                }
                rows
            })
            .collect()
    }

    /// The build rows (chunk, row) matched by each probe row, and the unmatched probe rows.
    type Drained = (BTreeMap<u64, Vec<(u32, u32)>>, Vec<u64>);

    fn drain(mut stream: Box<dyn ProbeStream + '_>, max_rows: usize) -> Result<Drained> {
        let mut res = ProbedRows::new(
            Vec::with_capacity(max_rows),
            Vec::with_capacity(max_rows),
            Vec::with_capacity(max_rows),
        );
        let mut matched = BTreeMap::<u64, Vec<(u32, u32)>>::new();
        let mut unmatched = vec![];
        loop {
            res.clear();
            stream.advance(&mut res, max_rows)?;
            if res.is_empty() {
                break;
            }
            assert!(res.matched_probe.len() <= max_rows);
            for (probe, build) in res.matched_probe.iter().zip(res.matched_build.iter()) {
                matched
                    .entry(*probe)
                    .or_default()
                    .push((build.chunk_index, build.row_index));
            }
            unmatched.extend_from_slice(&res.unmatched);
        }
        for rows in matched.values_mut() {
            rows.sort();
        }
        unmatched.sort();
        Ok((matched, unmatched))
    }

    fn check(
        expected: &[Vec<(u32, u32)>],
        valids: &Option<Bitmap>,
        (matched, unmatched): Drained,
        report_unmatched: bool,
        match_first: bool,
        case: &str,
    ) {
        let is_valid = |row: usize| valids.as_ref().is_none_or(|v| v.get_bit(row));
        let mut expected_unmatched = vec![];
        for (row, rows) in expected.iter().enumerate() {
            let got = matched.get(&(row as u64));
            if !is_valid(row) || rows.is_empty() {
                assert!(got.is_none(), "{case}: row {row} must not match");
                expected_unmatched.push(row as u64);
            } else if match_first {
                let got = got.unwrap_or_else(|| panic!("{case}: row {row} must match"));
                assert_eq!(got.len(), 1, "{case}: row {row} must match once");
                assert!(rows.contains(&got[0]), "{case}: row {row} wrong match");
            } else {
                assert_eq!(got, Some(rows), "{case}: row {row}");
            }
        }
        assert!(matched.keys().all(|row| (*row as usize) < expected.len()));
        match report_unmatched {
            true => assert_eq!(unmatched, expected_unmatched, "{case}: unmatched"),
            false => assert!(unmatched.is_empty(), "{case}: unmatched"),
        }
    }

    fn new_statistics(early_filtering: bool) -> ProbeHashStatistics {
        let mut statistics = ProbeHashStatistics::new(PROBE_ROWS);
        statistics.clear(PROBE_ROWS);
        if early_filtering {
            // A low observed match rate turns early filtering on.
            statistics.probed_rows = 100;
            statistics.matched_rows = 0;
        }
        statistics
    }

    macro_rules! check_all_cases {
        ($table:expr, $probe_block:expr, $expected:expr, $unique:expr) => {
            for with_nulls in [false, true] {
                let valids = probe_valids(with_nulls);
                for early_filtering in [false, true] {
                    for report_unmatched in [false, true] {
                        for max_rows in MAX_ROWS {
                            let case = format!(
                                "unique={} nulls={with_nulls} early_filtering={early_filtering} \
                                 unmatched={report_unmatched} max_rows={max_rows}",
                                $unique
                            );
                            let mut statistics = new_statistics(early_filtering);
                            let data = ProbeData::new(
                                $probe_block.clone(),
                                valids.clone(),
                                &mut statistics,
                            );
                            let stream = match report_unmatched {
                                true => $table.probe(data)?,
                                false => $table.probe_matched(data)?,
                            };
                            let got = drain(stream, max_rows)?;
                            check($expected, &valids, got, report_unmatched, $unique, &case);
                        }
                    }
                }
            }
        };
    }

    fn check_fixed_keys<const UNIQUE: bool>() -> Result<()> {
        let build = build_keys();
        let probe = probe_keys();
        let expected = expected_matches(&build, &probe);

        let table = FixedKeyHashJoinHashTable::<u64, UNIQUE>::new(
            HashJoinHashMap::<u64, UNIQUE>::with_build_row_num(BUILD_CHUNKS * BUILD_ROWS_PER_CHUNK),
            HashMethodFixedKeys::<u64>::default(),
        );
        let mut arenas = vec![];
        for (chunk, keys) in build.iter().enumerate() {
            let mut arena = vec![];
            let block = DataBlock::new_from_columns(vec![UInt64Type::from_data(keys.clone())]);
            table.insert(block, chunk, &mut arena)?;
            arenas.push(arena);
        }

        let probe_block = DataBlock::new_from_columns(vec![UInt64Type::from_data(probe)]);
        check_all_cases!(table, probe_block, &expected, UNIQUE);
        drop(arenas);
        Ok(())
    }

    fn check_binary_keys<const UNIQUE: bool>() -> Result<()> {
        let build = build_keys();
        let probe = probe_keys();
        let expected = expected_matches(&build, &probe);

        let table = SingleBinaryHashJoinHashTable::<UNIQUE>::new(
            BinaryHashJoinHashMap::<UNIQUE>::with_build_row_num(
                BUILD_CHUNKS * BUILD_ROWS_PER_CHUNK,
            ),
            HashMethodSingleBinary::default(),
        );
        let mut arenas = vec![];
        for (chunk, keys) in build.iter().enumerate() {
            let mut arena = vec![];
            let keys = keys.iter().map(|key| binary_key(*key)).collect::<Vec<_>>();
            let block = DataBlock::new_from_columns(vec![BinaryType::from_data(keys)]);
            table.insert(block, chunk, &mut arena)?;
            arenas.push(arena);
        }

        let probe = probe.iter().map(|key| binary_key(*key)).collect::<Vec<_>>();
        let probe_block = DataBlock::new_from_columns(vec![BinaryType::from_data(probe)]);
        check_all_cases!(table, probe_block, &expected, UNIQUE);
        drop(arenas);
        Ok(())
    }

    #[test]
    fn test_fixed_key_chain_probe() -> Result<()> {
        check_fixed_keys::<false>()?;
        check_fixed_keys::<true>()
    }

    #[test]
    fn test_binary_key_chain_probe() -> Result<()> {
        check_binary_keys::<false>()?;
        check_binary_keys::<true>()
    }
}
