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

//! Merging one term window: decode the postings of every source once and hand each output the
//! docs it receives, already in output row order and delta-encoded for the serializer.

use std::io;
use std::ops::Range;

use tantivy::DocSet;
use tantivy::TERMINATED;
use tantivy::postings::Postings;
use tantivy::schema::Field;
use tantivy::schema::FieldType;
use tantivy::schema::IndexRecordOption;
use tantivy::termdict::TermMerger;

use super::invalid;
use super::plan::TermWindow;
use super::source::MergeSourceReader;
use super::source::json_term_record_option;
use crate::RowMapping;

/// One doc of a term in an output: its row there, its frequency and its delta-encoded
/// positions in [`OutputSlice::positions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliceDoc {
    pub dest_row: u32,
    pub term_freq: u32,
    pub positions: Range<u32>,
}

/// One term of an output: its key bytes in [`OutputSlice::term_bytes`] and its docs in
/// [`OutputSlice::docs`], ascending by row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliceTerm {
    pub key: Range<u32>,
    pub docs: Range<u32>,
    /// Whether the term records frequencies; JSON number, bool and date terms do not.
    pub record_freq: bool,
}

/// Everything one output receives from one window, ready to serialize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutputSlice {
    pub window: u32,
    pub index: u16,
    pub field: Field,
    pub output: u32,
    pub term_bytes: Vec<u8>,
    pub terms: Vec<SliceTerm>,
    pub docs: Vec<SliceDoc>,
    pub positions: Vec<u32>,
}

impl OutputSlice {
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    pub fn term_key(&self, term: &SliceTerm) -> &[u8] {
        &self.term_bytes[term.key.start as usize..term.key.end as usize]
    }

    pub fn term_docs(&self, term: &SliceTerm) -> &[SliceDoc] {
        &self.docs[term.docs.start as usize..term.docs.end as usize]
    }

    pub fn doc_positions(&self, doc: &SliceDoc) -> &[u32] {
        &self.positions[doc.positions.start as usize..doc.positions.end as usize]
    }

    /// Bytes held by the slice.
    pub fn memory_size(&self) -> usize {
        self.term_bytes.len()
            + self.terms.len() * size_of::<SliceTerm>()
            + self.docs.len() * size_of::<SliceDoc>()
            + self.positions.len() * size_of::<u32>()
    }
}

/// Decodes the postings of `window` from every source and distributes them to the outputs.
///
/// `sources[s]` is source `s`. The result has one slice per output, in output order, including
/// empty ones: every output replays every window to keep its term order complete.
pub fn merge_window(
    sources: &[MergeSourceReader],
    mapping: &RowMapping,
    window: &TermWindow,
    check_interrupt: &dyn Fn() -> io::Result<()>,
) -> tantivy::Result<Vec<OutputSlice>> {
    let field = window.field;
    let Some(first) = sources.first() else {
        return Err(invalid("a merge needs at least one source").into());
    };
    let field_type = first
        .reader
        .schema()
        .get_field_entry(field)
        .field_type()
        .clone();
    let field_option = field_type
        .get_index_record_option()
        .unwrap_or(IndexRecordOption::Basic);
    let json = matches!(field_type, FieldType::JsonObject(_));

    let mut inverted = Vec::with_capacity(sources.len());
    for source in sources {
        inverted.push(source.reader.inverted_index(field)?);
    }
    let mut streams = Vec::with_capacity(inverted.len());
    for reader in &inverted {
        let mut builder = reader.terms().range();
        if let Some(lower) = &window.lower {
            builder = builder.gt(lower.as_slice());
        }
        if let Some(upper) = &window.upper {
            builder = builder.le(upper.as_slice());
        }
        streams.push(builder.into_stream()?);
    }

    let mut buckets = (0..mapping.num_outputs())
        .map(|output| Bucket {
            slice: OutputSlice {
                window: window.id,
                index: window.index,
                field,
                output: output as u32,
                term_bytes: Vec::new(),
                terms: Vec::new(),
                docs: Vec::new(),
                positions: Vec::new(),
            },
            pending: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut merger = TermMerger::new(streams);
    let mut positions = Vec::new();
    let mut visited = 0usize;
    while merger.advance() {
        check_interrupt()?;
        let key = merger.key();
        let option = match json {
            true => json_term_record_option(field_option, key),
            false => field_option,
        };
        for (source, term_info) in merger.current_segment_ords_and_term_infos() {
            // The field option drives decoding: Tantivy recognizes JSON terms stored without
            // frequencies by their layout, like the index writer and the readers do.
            let mut cursor =
                inverted[source].read_postings_from_terminfo(&term_info, field_option)?;
            let mut doc = cursor.doc();
            while doc != TERMINATED {
                visited += 1;
                if visited % 4096 == 0 {
                    check_interrupt()?;
                }
                let (dest_ord, dest_row) = mapping.destination(source, doc);
                let bucket = &mut buckets[dest_ord as usize];
                let term_freq = if option.has_freq() {
                    cursor.term_freq()
                } else {
                    0
                };
                let start = bucket.slice.positions.len() as u32;
                if option.has_positions() {
                    positions.clear();
                    cursor.positions(&mut positions);
                    let mut last = 0;
                    for position in &positions {
                        bucket.slice.positions.push(position - last);
                        last = *position;
                    }
                }
                let end = bucket.slice.positions.len() as u32;
                bucket.pending.push(SliceDoc {
                    dest_row,
                    term_freq,
                    positions: start..end,
                });
                doc = cursor.advance();
            }
        }
        for bucket in &mut buckets {
            if bucket.pending.is_empty() {
                continue;
            }
            // Each source contributes its docs ascending by row; the runs of several sources
            // interleave, so order them. Arbitrary mappings are handled the same way.
            bucket.pending.sort_unstable_by_key(|doc| doc.dest_row);
            let slice = &mut bucket.slice;
            let key_start = slice.term_bytes.len() as u32;
            slice.term_bytes.extend_from_slice(key);
            let docs_start = slice.docs.len() as u32;
            slice.docs.append(&mut bucket.pending);
            slice.terms.push(SliceTerm {
                key: key_start..slice.term_bytes.len() as u32,
                docs: docs_start..slice.docs.len() as u32,
                record_freq: option.has_freq(),
            });
        }
    }
    Ok(buckets.into_iter().map(|bucket| bucket.slice).collect())
}

struct Bucket {
    slice: OutputSlice,
    pending: Vec<SliceDoc>,
}
