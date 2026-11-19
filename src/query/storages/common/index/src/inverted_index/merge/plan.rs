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

//! Cutting the term space of one field into windows of roughly equal posting bytes.
//!
//! Every source reports checkpoints: the cumulative postings and positions bytes of its terms
//! up to a key, one checkpoint per `interval` bytes. The planner merges the checkpoints of all
//! sources by key and cuts a window whenever the summed bytes grow past the window size. A
//! window is a key interval `(lower, upper]` applied to every source, so the windows of one
//! field are disjoint and, taken in id order, cover every term.

use tantivy::index::InvertedIndexReader;
use tantivy::schema::Field;

/// Encoded postings and positions bytes a window merger decodes in one go.
///
/// Decoded slices are about 15 times the encoded bytes for text with positions, and up to
/// `threads + threads / 2` windows are in flight, so this bounds the merge's working set:
/// 1 MiB windows keep 48 windows near 1 GiB. The window size no longer drives storage
/// requests; the source streams fetch in [`super::streaming_file::STREAMING_CHUNK_SIZE`] chunks.
pub const TERM_WINDOW_BYTES: u64 = 1024 * 1024;

/// Checkpoint spacing while scanning one source; the cut error per source stays below it.
pub const TERM_COST_CHECKPOINT_BYTES: u64 = 64 * 1024;

/// Cumulative postings and positions bytes of one source's field through `key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermCostCheckpoint {
    pub key: Vec<u8>,
    pub cost: u64,
}

/// Cost profile of one field of one source.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldCostTable {
    /// Ascending keys and costs; the last entry is the last term with the field total.
    pub checkpoints: Vec<TermCostCheckpoint>,
    pub num_terms: u64,
}

impl FieldCostTable {
    pub fn total(&self) -> u64 {
        self.checkpoints
            .last()
            .map_or(0, |checkpoint| checkpoint.cost)
    }
}

/// Scans the term dictionary of `inverted` once and records a checkpoint every `interval`
/// bytes of postings and positions.
pub fn scan_field_costs(
    inverted: &InvertedIndexReader,
    interval: u64,
) -> tantivy::Result<FieldCostTable> {
    let interval = interval.max(1);
    let mut stream = inverted.terms().stream()?;
    let mut table = FieldCostTable::default();
    let mut cost = 0u64;
    let mut since_checkpoint = 0u64;
    let mut last_key = Vec::new();
    while stream.advance() {
        let info = stream.value();
        let bytes = (info.postings_range.len() + info.positions_range.len()) as u64;
        cost += bytes;
        since_checkpoint += bytes;
        table.num_terms += 1;
        last_key.clear();
        last_key.extend_from_slice(stream.key());
        if since_checkpoint >= interval {
            table.checkpoints.push(TermCostCheckpoint {
                key: last_key.clone(),
                cost,
            });
            since_checkpoint = 0;
        }
    }
    if table.num_terms > 0
        && table
            .checkpoints
            .last()
            .is_none_or(|checkpoint| checkpoint.key != last_key)
    {
        table.checkpoints.push(TermCostCheckpoint {
            key: last_key,
            cost,
        });
    }
    Ok(table)
}

/// One unit of merge work: the terms of `field` in `(lower, upper]` across every source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermWindow {
    /// Global position in the task; every output replays windows in this order.
    pub id: u32,
    /// Which inverted index of the table the window belongs to.
    pub index: u16,
    pub field: Field,
    /// Exclusive lower key; `None` for the first window of a field.
    pub lower: Option<Vec<u8>>,
    /// Inclusive upper key; `None` for the last window of a field.
    pub upper: Option<Vec<u8>>,
}

impl TermWindow {
    /// Whether `key` lies in the window.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.lower.as_deref().is_none_or(|lower| key > lower)
            && self.upper.as_deref().is_none_or(|upper| key <= upper)
    }
}

/// Cuts the windows of one field. `tables` has one entry per source. Fields without any term
/// still get one window so every output opens the field, exactly as indexing it would.
pub fn plan_field_windows(
    tables: &[FieldCostTable],
    window_bytes: u64,
    index: u16,
    field: Field,
    first_id: u32,
) -> Vec<TermWindow> {
    let window_bytes = window_bytes.max(1);
    let mut checkpoints = Vec::new();
    for (source, table) in tables.iter().enumerate() {
        for checkpoint in &table.checkpoints {
            checkpoints.push((checkpoint.key.as_slice(), source, checkpoint.cost));
        }
    }
    checkpoints.sort_by(|left, right| left.0.cmp(right.0).then(left.1.cmp(&right.1)));

    let mut windows = Vec::new();
    let mut current = vec![0u64; tables.len()];
    let mut total = 0u64;
    let mut window_start = 0u64;
    let mut lower: Option<Vec<u8>> = None;
    let mut next_id = first_id;
    for (key, source, cost) in checkpoints {
        total += cost - current[source];
        current[source] = cost;
        if total - window_start >= window_bytes && lower.as_deref() != Some(key) {
            windows.push(TermWindow {
                id: next_id,
                index,
                field,
                lower: lower.clone(),
                upper: Some(key.to_vec()),
            });
            next_id += 1;
            lower = Some(key.to_vec());
            window_start = total;
        }
    }
    windows.push(TermWindow {
        id: next_id,
        index,
        field,
        lower,
        upper: None,
    });
    windows
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(points: &[(&str, u64)]) -> FieldCostTable {
        FieldCostTable {
            checkpoints: points
                .iter()
                .map(|(key, cost)| TermCostCheckpoint {
                    key: key.as_bytes().to_vec(),
                    cost: *cost,
                })
                .collect(),
            num_terms: points.len() as u64,
        }
    }

    #[test]
    fn test_windows_cover_the_key_space_in_order() {
        let tables = [
            table(&[("b", 10), ("d", 20), ("f", 30)]),
            table(&[("a", 15), ("c", 30), ("e", 45)]),
        ];
        let windows = plan_field_windows(&tables, 25, 3, Field::from_field_id(1), 7);
        let bounds = windows
            .iter()
            .map(|window| {
                (
                    window.lower.as_deref().map(|k| k.to_vec()),
                    window.upper.as_deref().map(|k| k.to_vec()),
                )
            })
            .collect::<Vec<_>>();
        // Cumulative totals at a, b, c, d, e, f: 15, 25, 40, 50, 65, 75.
        assert_eq!(bounds, vec![
            (None, Some(b"b".to_vec())),
            (Some(b"b".to_vec()), Some(b"d".to_vec())),
            (Some(b"d".to_vec()), Some(b"f".to_vec())),
            (Some(b"f".to_vec()), None),
        ]);
        assert_eq!(
            windows.iter().map(|window| window.id).collect::<Vec<_>>(),
            vec![7, 8, 9, 10]
        );
        assert!(windows.iter().all(|window| window.index == 3));
        assert!(windows[1].contains(b"c"));
        assert!(windows[1].contains(b"d"));
        assert!(!windows[1].contains(b"b"));
        assert!(!windows[1].contains(b"e"));
        for key in [b"a".as_slice(), b"c", b"zz", b""] {
            assert_eq!(
                windows.iter().filter(|window| window.contains(key)).count(),
                1,
                "{key:?}"
            );
        }
    }

    #[test]
    fn test_equal_keys_and_empty_fields() {
        // The same key from two sources never yields an empty window.
        let tables = [table(&[("k", 100)]), table(&[("k", 100)])];
        let windows = plan_field_windows(&tables, 50, 0, Field::from_field_id(0), 0);
        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].upper.as_deref(), Some(b"k".as_slice()));
        assert_eq!(windows[1].lower.as_deref(), Some(b"k".as_slice()));

        let windows = plan_field_windows(
            &[FieldCostTable::default()],
            50,
            0,
            Field::from_field_id(0),
            4,
        );
        assert_eq!(windows, vec![TermWindow {
            id: 4,
            index: 0,
            field: Field::from_field_id(0),
            lower: None,
            upper: None,
        }]);
    }
}
