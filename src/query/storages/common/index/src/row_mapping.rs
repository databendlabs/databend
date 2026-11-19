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

use std::io;
use std::ops::Range;

use crate::index_common::invalid_input as invalid;

/// Consecutive rows of one source, in the order they appear in an output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRows {
    pub source: u32,
    pub rows: Range<u32>,
}

const UNASSIGNED: u32 = u32::MAX;

/// Where every row of one source goes: output ordinal and row within that output.
struct SourceMap {
    dest_ord: Vec<u16>,
    dest_row: Vec<u32>,
    /// Rows assigned to each output.
    rows_per_output: Vec<u32>,
}

/// Task-level source row to output row mapping of a recluster task, shared by every stage of
/// an index merge and independent of the index kind.
///
/// Built once from the lineage of every output; read concurrently by the unit mergers and the
/// output writers.
pub struct RowMapping {
    sources: Vec<SourceMap>,
    /// Lineage of every output in output row order, kept for the row-ordered
    /// passes (fieldnorms, fast fields) that walk an output from its first row.
    outputs: Vec<Vec<SourceRows>>,
    output_rows: Vec<u32>,
}

impl RowMapping {
    /// `source_rows[s]` is the row count of source `s`; `outputs[k]` lists where the rows of
    /// output `k` come from, in output row order. Every source row must be assigned exactly
    /// once: reclustering keeps all rows.
    pub fn build(source_rows: &[u32], outputs: Vec<Vec<SourceRows>>) -> io::Result<Self> {
        if outputs.len() > u16::MAX as usize {
            return Err(invalid(format!(
                "{} outputs, at most {}",
                outputs.len(),
                u16::MAX
            )));
        }
        let mut sources = source_rows
            .iter()
            .map(|&rows| SourceMap {
                dest_ord: vec![0; rows as usize],
                dest_row: vec![UNASSIGNED; rows as usize],
                rows_per_output: vec![0; outputs.len()],
            })
            .collect::<Vec<_>>();
        let mut output_rows = Vec::with_capacity(outputs.len());
        for (dest_ord, lineage) in outputs.iter().enumerate() {
            let mut dest_row = 0u32;
            for origin in lineage {
                if origin.rows.start >= origin.rows.end {
                    return Err(invalid("index merge source range is empty or inverted"));
                }
                let map = sources
                    .get_mut(origin.source as usize)
                    .ok_or_else(|| invalid(format!("unknown source {}", origin.source)))?;
                if origin.rows.end as usize > map.dest_row.len() {
                    return Err(invalid(format!(
                        "rows {:?} exceed source {} with {} rows",
                        origin.rows,
                        origin.source,
                        map.dest_row.len()
                    )));
                }
                for row in origin.rows.clone() {
                    let slot = &mut map.dest_row[row as usize];
                    if *slot != UNASSIGNED {
                        return Err(invalid(format!(
                            "source {} row {row} assigned twice",
                            origin.source
                        )));
                    }
                    *slot = dest_row;
                    map.dest_ord[row as usize] = dest_ord as u16;
                    dest_row = dest_row
                        .checked_add(1)
                        .ok_or_else(|| invalid("output doc count exceeds UInt32"))?;
                }
                map.rows_per_output[dest_ord] += origin.rows.end - origin.rows.start;
            }
            output_rows.push(dest_row);
        }
        for (source, map) in sources.iter().enumerate() {
            if let Some(row) = map.dest_row.iter().position(|slot| *slot == UNASSIGNED) {
                return Err(invalid(format!(
                    "source {source} row {row} is assigned to no output"
                )));
            }
        }
        Ok(Self {
            sources,
            outputs,
            output_rows,
        })
    }

    pub fn num_sources(&self) -> usize {
        self.sources.len()
    }

    pub fn num_outputs(&self) -> usize {
        self.outputs.len()
    }

    pub fn source_rows(&self, source: usize) -> u32 {
        self.sources[source].dest_row.len() as u32
    }

    pub fn output_rows(&self, output: usize) -> u32 {
        self.output_rows[output]
    }

    #[inline]
    pub fn destination(&self, source: usize, row: u32) -> (u16, u32) {
        let map = &self.sources[source];
        (map.dest_ord[row as usize], map.dest_row[row as usize])
    }

    /// Whether every row of `source` lands in output `dest_ord`.
    pub fn fully_in(&self, source: usize, dest_ord: usize) -> bool {
        let map = &self.sources[source];
        map.rows_per_output[dest_ord] as usize == map.dest_row.len()
    }

    /// Rows of `source` that land in output `dest_ord`.
    pub fn rows_in(&self, source: usize, dest_ord: usize) -> u32 {
        self.sources[source].rows_per_output[dest_ord]
    }

    /// Lineage ranges of `output` in output row order.
    pub fn output_lineage(&self, output: usize) -> &[SourceRows] {
        &self.outputs[output]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(source: u32, range: Range<u32>) -> SourceRows {
        SourceRows {
            source,
            rows: range,
        }
    }

    #[test]
    fn test_mapping_assigns_every_row_once() {
        let mapping = RowMapping::build(&[5, 3], vec![
            vec![rows(0, 0..2), rows(1, 0..1), rows(0, 2..3)],
            vec![rows(1, 1..3), rows(0, 3..5)],
        ])
        .unwrap();
        assert_eq!(mapping.num_outputs(), 2);
        assert_eq!(mapping.output_rows(0), 4);
        assert_eq!(mapping.output_rows(1), 4);
        assert_eq!(mapping.destination(0, 0), (0, 0));
        assert_eq!(mapping.destination(0, 1), (0, 1));
        assert_eq!(mapping.destination(1, 0), (0, 2));
        assert_eq!(mapping.destination(0, 2), (0, 3));
        assert_eq!(mapping.destination(1, 1), (1, 0));
        assert_eq!(mapping.destination(0, 4), (1, 3));
        assert!(!mapping.fully_in(0, 0));
        assert_eq!(mapping.rows_in(0, 0), 3);
        assert_eq!(mapping.rows_in(0, 1), 2);
        assert_eq!(mapping.output_lineage(1), &[rows(1, 1..3), rows(0, 3..5)]);
    }

    #[test]
    fn test_mapping_rejects_incomplete_or_duplicate_lineage() {
        assert!(RowMapping::build(&[2], vec![vec![rows(0, 0..1)]]).is_err());
        assert!(RowMapping::build(&[2], vec![vec![rows(0, 0..2), rows(0, 1..2)]]).is_err());
        assert!(RowMapping::build(&[2], vec![vec![rows(0, 0..3)]]).is_err());
        assert!(RowMapping::build(&[2], vec![vec![rows(1, 0..1)]]).is_err());
        assert!(RowMapping::build(&[2], vec![vec![rows(0, 1..1), rows(0, 0..2)]]).is_err());
        let fully = RowMapping::build(&[2], vec![vec![rows(0, 0..2)]]).unwrap();
        assert!(fully.fully_in(0, 0));
    }
}
