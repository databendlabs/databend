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

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io;
use std::ops::Range;
use std::path::Path;

use databend_storages_common_io::BLOCKING_WRITE_MAX_CHUNKS;
use databend_storages_common_io::BlockingWrite;
use databend_storages_common_io::create_blocking_write;
use opendal::Operator;
use tantivy::Directory;
use tantivy::DocSet;
use tantivy::Index;
use tantivy::IndexMeta;
use tantivy::Segment;
use tantivy::SegmentReader;
use tantivy::TERMINATED;
use tantivy::columnar::ColumnarReader;
use tantivy::columnar::MergeRowOrder;
use tantivy::columnar::RowAddr;
use tantivy::columnar::ShuffleMergeOrder;
use tantivy::columnar::merge_columnar;
use tantivy::directory::TerminatingWrite;
use tantivy::fieldnorm::FieldNormReader;
use tantivy::fieldnorm::FieldNormReaders;
use tantivy::fieldnorm::FieldNormsSerializer;
use tantivy::index::SegmentComponent;
use tantivy::postings::FieldSerializer;
use tantivy::postings::InvertedIndexSerializer;
use tantivy::postings::Postings;
use tantivy::postings::SegmentPostings;
use tantivy::schema::Field;
use tantivy::schema::FieldType;
use tantivy::schema::IndexRecordOption;
use tantivy::schema::Schema;
use tantivy::schema::TantivyDocument;
use tantivy::store::StoreWriter;
use tantivy::termdict::TermMerger;

use super::source::MergeSourceDirectory;
use super::source::json_term_record_option;
use crate::inverted_index::bundle::META_JSON_PATH;
use crate::inverted_index::bundle_builder::BundleSizes;
use crate::inverted_index::bundle_builder::InvertedIndexBundleBuilder;
use crate::inverted_index::output_directory::INVERTED_INDEX_STREAM_THRESHOLD;
use crate::inverted_index::output_directory::InvertedIndexOutputDirectory;

pub struct MergeSource {
    pub location: String,
    pub bundle_size: u64,
    pub num_rows: u32,
}

/// Consecutive rows of one source, in the order they appear in an output.
pub struct SourceRows {
    pub source: u32,
    pub rows: Range<u32>,
}

/// One output bundle: its location and, in output row order, where its rows come from.
pub struct MergeOutput {
    pub location: String,
    pub rows: Vec<SourceRows>,
}

const UNASSIGNED: u32 = u32::MAX;

/// Where every row of one source goes: output ordinal and row within that output.
struct SourceMapping {
    dest_ord: Vec<u16>,
    dest_row: Vec<u32>,
    /// Rows assigned to each output.
    rows_per_output: Vec<u32>,
    /// Along source order, `(dest_ord, dest_row)` never decreases. Lets a term's postings be
    /// merged with one cursor per source instead of collected and sorted.
    monotonic: bool,
}

impl SourceMapping {
    fn new(num_rows: u32, num_outputs: usize) -> Self {
        Self {
            dest_ord: vec![0; num_rows as usize],
            dest_row: vec![UNASSIGNED; num_rows as usize],
            rows_per_output: vec![0; num_outputs],
            monotonic: true,
        }
    }

    fn assign(&mut self, row: u32, dest_ord: u16, dest_row: u32) -> io::Result<()> {
        let slot = &mut self.dest_row[row as usize];
        if *slot != UNASSIGNED {
            return Err(invalid(format!("source row {row} assigned twice")));
        }
        *slot = dest_row;
        self.dest_ord[row as usize] = dest_ord;
        self.rows_per_output[dest_ord as usize] += 1;
        Ok(())
    }

    fn finish(&mut self) {
        let mut last = None;
        for row in 0..self.dest_row.len() {
            if self.dest_row[row] == UNASSIGNED {
                continue;
            }
            let current = (self.dest_ord[row], self.dest_row[row]);
            if let Some(last) = last {
                if current <= last {
                    self.monotonic = false;
                    return;
                }
            }
            last = Some(current);
        }
    }

    fn destination(&self, row: u32) -> Option<(u16, u32)> {
        let dest_row = self.dest_row[row as usize];
        if dest_row == UNASSIGNED {
            return None;
        }
        Some((self.dest_ord[row as usize], dest_row))
    }

    /// Whether every row of the source is assigned to exactly `dest_ord`.
    fn fully_in(&self, dest_ord: u16) -> bool {
        self.rows_per_output[dest_ord as usize] as usize == self.dest_row.len()
    }
}

struct Source {
    index: Index,
    reader: SegmentReader,
    mapping: SourceMapping,
}

struct Output {
    location: String,
    directory: InvertedIndexOutputDirectory,
    index: Index,
    segment: Segment,
    num_rows: u32,
    /// `dest_row -> (source, source_row)`, the order fast field merging wants.
    origins: Vec<RowAddr>,
}

/// Merges the single Tantivy segments of `sources` into one segment per output, following the
/// row order the outputs describe, without re-tokenizing. Every output is written through its
/// own [`InvertedIndexOutputDirectory`], exactly as the index writer does, so the sources are
/// read once while all outputs are open.
///
/// Blocking: reads and writes wait for the IO runtime, so this must run on an executor thread.
pub struct InvertedIndexMerger {
    operator: Operator,
    sources: Vec<Source>,
    outputs: Vec<Output>,
    schema: Schema,
    #[cfg(test)]
    force_sorted_path: bool,
}

impl InvertedIndexMerger {
    pub fn try_create(
        operator: Operator,
        sources: Vec<MergeSource>,
        outputs: Vec<MergeOutput>,
    ) -> tantivy::Result<Self> {
        Self::try_create_with_stream_threshold(
            operator,
            sources,
            outputs,
            INVERTED_INDEX_STREAM_THRESHOLD,
        )
    }

    /// `stream_threshold`: output `.idx` / `.pos` larger than this become sibling objects.
    pub fn try_create_with_stream_threshold(
        operator: Operator,
        sources: Vec<MergeSource>,
        outputs: Vec<MergeOutput>,
        stream_threshold: usize,
    ) -> tantivy::Result<Self> {
        if outputs.len() > u16::MAX as usize {
            return Err(invalid(format!("{} outputs, at most {}", outputs.len(), u16::MAX)).into());
        }
        let mut mappings = Vec::with_capacity(sources.len());
        for source in &sources {
            mappings.push(SourceMapping::new(source.num_rows, outputs.len()));
        }
        let mut origins_per_output = Vec::with_capacity(outputs.len());
        for (dest_ord, output) in outputs.iter().enumerate() {
            let mut origins = Vec::new();
            for source_rows in &output.rows {
                let mapping = mappings
                    .get_mut(source_rows.source as usize)
                    .ok_or_else(|| invalid(format!("unknown source {}", source_rows.source)))?;
                if source_rows.rows.end > mapping.dest_row.len() as u32 {
                    return Err(invalid(format!(
                        "rows {:?} exceed source {} with {} rows",
                        source_rows.rows,
                        source_rows.source,
                        mapping.dest_row.len()
                    ))
                    .into());
                }
                for row in source_rows.rows.clone() {
                    mapping.assign(row, dest_ord as u16, origins.len() as u32)?;
                    origins.push(RowAddr {
                        segment_ord: source_rows.source,
                        row_id: row,
                    });
                }
            }
            origins_per_output.push(origins);
        }
        for mapping in &mut mappings {
            mapping.finish();
        }

        let mut opened = Vec::with_capacity(sources.len());
        let mut schema = None;
        for (source, mapping) in sources.into_iter().zip(mappings) {
            let directory = MergeSourceDirectory::open(
                operator.clone(),
                source.location.clone(),
                source.bundle_size,
            )?;
            let index = directory.open_index()?;
            let searcher = index.reader()?.searcher();
            if searcher.segment_readers().len() != 1 {
                return Err(invalid(format!(
                    "{} has {} segments, expected one",
                    source.location,
                    searcher.segment_readers().len()
                ))
                .into());
            }
            let reader = searcher.segment_reader(0).clone();
            if reader.max_doc() != source.num_rows {
                return Err(invalid(format!(
                    "{} has {} rows, expected {}",
                    source.location,
                    reader.max_doc(),
                    source.num_rows
                ))
                .into());
            }
            match &schema {
                None => schema = Some(index.schema()),
                Some(schema) if *schema != index.schema() => {
                    return Err(
                        invalid(format!("{} has a different schema", source.location)).into(),
                    );
                }
                Some(_) => {}
            }
            opened.push(Source {
                index,
                reader,
                mapping,
            });
        }
        let schema = schema.ok_or_else(|| invalid("a merge needs at least one source"))?;
        check_schema(&schema)?;

        let settings = opened[0].index.settings().clone();
        let mut created = Vec::with_capacity(outputs.len());
        for (output, origins) in outputs.into_iter().zip(origins_per_output) {
            let directory = InvertedIndexOutputDirectory::with_stream_threshold(
                operator.clone(),
                output.location.clone(),
                stream_threshold,
            );
            let index = Index::create(directory.clone(), schema.clone(), settings.clone())?;
            let segment = index.new_segment();
            created.push(Output {
                location: output.location,
                directory,
                index,
                segment,
                num_rows: origins.len() as u32,
                origins,
            });
        }

        Ok(Self {
            operator,
            sources: opened,
            outputs: created,
            schema,
            #[cfg(test)]
            force_sorted_path: false,
        })
    }

    pub fn finish(mut self) -> tantivy::Result<Vec<BundleSizes>> {
        self.write_fieldnorms()?;
        self.write_postings()?;
        self.write_fast_fields()?;
        self.write_stores()?;
        self.write_metas()?;

        let mut sizes = Vec::with_capacity(self.outputs.len());
        for output in self.outputs {
            let external_files = output.directory.external_files();
            let builder = InvertedIndexBundleBuilder::try_create(
                output.directory,
                output.index,
                external_files,
            )?;
            let mut sink = create_blocking_write(
                self.operator.clone(),
                output.location,
                BLOCKING_WRITE_MAX_CHUNKS,
            );
            sizes.push(builder.write_to(&mut sink)?);
            sink.close()
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        Ok(sizes)
    }

    fn write_fieldnorms(&mut self) -> tantivy::Result<()> {
        let mut fields = Vec::new();
        for (field, entry) in self.schema.fields() {
            if entry.is_indexed() && entry.has_fieldnorms() {
                fields.push(field);
            }
        }
        let mut readers = Vec::with_capacity(fields.len());
        for field in &fields {
            readers.push(self.source_fieldnorms(*field)?);
        }
        for output in &mut self.outputs {
            let mut serializer = FieldNormsSerializer::from_write(
                output.segment.open_write(SegmentComponent::FieldNorms)?,
            )?;
            let mut data = vec![0u8; output.num_rows as usize];
            for (field, readers) in fields.iter().zip(&readers) {
                for (dest_row, origin) in output.origins.iter().enumerate() {
                    data[dest_row] =
                        readers[origin.segment_ord as usize].fieldnorm_id(origin.row_id);
                }
                serializer.serialize_field(*field, &data)?;
            }
            serializer.close()?;
        }
        Ok(())
    }

    fn source_fieldnorms(&self, field: Field) -> tantivy::Result<Vec<FieldNormReader>> {
        let mut readers = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            let reader = source
                .reader
                .fieldnorms_readers()
                .get_field(field)?
                .ok_or_else(|| invalid(format!("source lacks fieldnorms for {field:?}")))?;
            readers.push(reader);
        }
        Ok(readers)
    }

    fn write_postings(&mut self) -> tantivy::Result<()> {
        let mut serializers = Vec::with_capacity(self.outputs.len());
        let mut fieldnorms = Vec::with_capacity(self.outputs.len());
        for output in &mut self.outputs {
            let fieldnorm_file = output.segment.open_read(SegmentComponent::FieldNorms)?;
            fieldnorms.push(FieldNormReaders::open(fieldnorm_file)?);
            serializers.push(InvertedIndexSerializer::open(&mut output.segment)?);
        }

        for (field, entry) in self.schema.fields() {
            if !entry.is_indexed() {
                continue;
            }
            let total_num_tokens = self.total_num_tokens(field, entry.has_fieldnorms())?;
            let mut field_serializers = Vec::with_capacity(serializers.len());
            for (dest_ord, serializer) in serializers.iter_mut().enumerate() {
                let fieldnorm_reader = fieldnorms[dest_ord].get_field(field)?;
                field_serializers.push(serializer.new_field(
                    field,
                    total_num_tokens[dest_ord],
                    fieldnorm_reader,
                )?);
            }
            self.merge_field(field, entry.field_type(), &mut field_serializers)?;
            for field_serializer in field_serializers {
                field_serializer.close()?;
            }
        }
        for serializer in serializers {
            serializer.close()?;
        }
        Ok(())
    }

    /// Token total of `field` for every output. Exact for a source that lies entirely in one
    /// output; otherwise estimated from fieldnorms, as Tantivy does for segments with deletes.
    /// Only BM25's average length depends on it.
    fn total_num_tokens(&self, field: Field, has_fieldnorms: bool) -> tantivy::Result<Vec<u64>> {
        let mut totals = vec![0u64; self.outputs.len()];
        for source in &self.sources {
            let mapping = &source.mapping;
            let source_total = source.reader.inverted_index(field)?.total_num_tokens();
            let num_rows = mapping.dest_row.len();
            let fieldnorms = match has_fieldnorms {
                true => source.reader.fieldnorms_readers().get_field(field)?,
                false => None,
            };
            for (dest_ord, rows) in mapping.rows_per_output.iter().enumerate() {
                if *rows as usize == num_rows {
                    totals[dest_ord] += source_total;
                } else if *rows > 0 && fieldnorms.is_none() {
                    let ratio = *rows as f64 / num_rows as f64;
                    totals[dest_ord] += (source_total as f64 * ratio) as u64;
                }
            }
            let Some(fieldnorms) = fieldnorms else {
                continue;
            };
            for row in 0..num_rows as u32 {
                let Some((dest_ord, _)) = mapping.destination(row) else {
                    continue;
                };
                if !mapping.fully_in(dest_ord) {
                    let fieldnorm = FieldNormReader::id_to_fieldnorm(fieldnorms.fieldnorm_id(row));
                    totals[dest_ord as usize] += u64::from(fieldnorm);
                }
            }
        }
        Ok(totals)
    }

    fn merge_field(
        &self,
        field: Field,
        field_type: &FieldType,
        outputs: &mut [FieldSerializer<'_>],
    ) -> tantivy::Result<()> {
        let field_option = field_type
            .get_index_record_option()
            .unwrap_or(IndexRecordOption::Basic);
        let mut inverted = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            inverted.push(source.reader.inverted_index(field)?);
        }
        let mut streams = Vec::with_capacity(inverted.len());
        for reader in &inverted {
            streams.push(reader.terms().stream()?);
        }
        let mut monotonic = true;
        for source in &self.sources {
            monotonic &= source.mapping.monotonic;
        }
        #[cfg(test)]
        if self.force_sorted_path {
            monotonic = false;
        }

        let json = matches!(field_type, FieldType::JsonObject(_));
        let mut merger = TermMerger::new(streams);
        let mut doc_freqs = vec![0u32; outputs.len()];
        let mut positions = Vec::new();
        let mut deltas = Vec::new();
        while merger.advance() {
            let key = merger.key();
            let option = match json {
                true => json_term_record_option(field_option, key),
                false => field_option,
            };

            // Pass one: how many docs each output receives, needed before a term is opened.
            doc_freqs.fill(0);
            let mut postings = Vec::new();
            for (source_ord, term_info) in merger.current_segment_ords_and_term_infos() {
                let mut cursor =
                    inverted[source_ord].read_postings_from_terminfo(&term_info, field_option)?;
                let mapping = &self.sources[source_ord].mapping;
                let mut doc = cursor.doc();
                while doc != TERMINATED {
                    if let Some((dest_ord, _)) = mapping.destination(doc) {
                        doc_freqs[dest_ord as usize] += 1;
                    }
                    doc = cursor.advance();
                }
                postings.push((
                    source_ord,
                    inverted[source_ord].read_postings_from_terminfo(&term_info, field_option)?,
                ));
            }

            // Pass two: write the docs, in output order and ascending doc id within an output.
            let mut docs = OutputDocs {
                key,
                option,
                doc_freqs: &doc_freqs,
                outputs,
                open: None,
                positions: &mut positions,
                deltas: &mut deltas,
            };
            if monotonic {
                self.write_term_merged(postings, &mut docs)?;
            } else {
                self.write_term_sorted(postings, &mut docs)?;
            }
            docs.close()?;
        }
        Ok(())
    }

    /// Every source visits its docs in increasing `(dest_ord, dest_row)`, so a heap over one
    /// cursor per source yields the docs of each output in order.
    fn write_term_merged(
        &self,
        postings: Vec<(usize, SegmentPostings)>,
        docs: &mut OutputDocs<'_, '_>,
    ) -> tantivy::Result<()> {
        let mut heap = BinaryHeap::with_capacity(postings.len());
        let mut cursors = Vec::with_capacity(postings.len());
        for (index, (source_ord, cursor)) in postings.into_iter().enumerate() {
            cursors.push((source_ord, cursor));
            if let Some(key) = self.advance_to_assigned(&mut cursors[index]) {
                heap.push(Reverse((key, index)));
            }
        }
        while let Some(Reverse(((dest_ord, dest_row), index))) = heap.pop() {
            let (_, cursor) = &mut cursors[index];
            docs.write(dest_ord, dest_row, cursor)?;
            cursor.advance();
            if let Some(key) = self.advance_to_assigned(&mut cursors[index]) {
                heap.push(Reverse((key, index)));
            }
        }
        Ok(())
    }

    /// Moves `cursor` to its current or next assigned doc and returns where that doc goes.
    fn advance_to_assigned(&self, cursor: &mut (usize, SegmentPostings)) -> Option<(u16, u32)> {
        let mapping = &self.sources[cursor.0].mapping;
        let mut doc = cursor.1.doc();
        while doc != TERMINATED {
            if let Some(destination) = mapping.destination(doc) {
                return Some(destination);
            }
            doc = cursor.1.advance();
        }
        None
    }

    /// Arbitrary row orders: collect the term's docs, sort them, then write.
    fn write_term_sorted(
        &self,
        postings: Vec<(usize, SegmentPostings)>,
        docs: &mut OutputDocs<'_, '_>,
    ) -> tantivy::Result<()> {
        let has_freq = docs.option.has_freq();
        let has_positions = docs.option.has_positions();
        let mut collected = Vec::new();
        let mut all_positions = Vec::new();
        for (source_ord, mut cursor) in postings {
            let mapping = &self.sources[source_ord].mapping;
            let mut doc = cursor.doc();
            while doc != TERMINATED {
                if let Some((dest_ord, dest_row)) = mapping.destination(doc) {
                    let term_freq = if has_freq { cursor.term_freq() } else { 0 };
                    let start = all_positions.len();
                    if has_positions {
                        cursor.append_positions_with_offset(0, &mut all_positions);
                    }
                    collected.push((dest_ord, dest_row, term_freq, start..all_positions.len()));
                }
                doc = cursor.advance();
            }
        }
        collected.sort_unstable_by_key(|(dest_ord, dest_row, _, _)| (*dest_ord, *dest_row));
        for (dest_ord, dest_row, term_freq, positions) in collected {
            docs.write_positions(dest_ord, dest_row, term_freq, &all_positions[positions])?;
        }
        Ok(())
    }

    fn write_fast_fields(&mut self) -> tantivy::Result<()> {
        let mut columnars = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            let meta = source.index.searchable_segment_metas()?.remove(0);
            let path = meta.relative_path(SegmentComponent::FastFields);
            let file = source.index.directory().open_read(&path)?;
            columnars.push(ColumnarReader::open(file)?);
        }
        let mut readers = Vec::with_capacity(columnars.len());
        for columnar in &columnars {
            readers.push(columnar);
        }
        for output in &mut self.outputs {
            let mut write = output.segment.open_write(SegmentComponent::FastFields)?;
            let order = MergeRowOrder::Shuffled(ShuffleMergeOrder {
                new_row_id_to_old_row_id: output.origins.clone(),
                alive_bitsets: vec![None; readers.len()],
            });
            merge_columnar(&readers, &[], order, &mut write)?;
            write.terminate()?;
        }
        Ok(())
    }

    /// Databend stores no fields, so every doc is an empty document; writing them keeps the
    /// store identical to what indexing the rows produces.
    fn write_stores(&mut self) -> tantivy::Result<()> {
        let settings = self.outputs[0].index.settings().clone();
        let empty = TantivyDocument::default();
        for output in &mut self.outputs {
            let mut store = StoreWriter::new(
                output.segment.open_write(SegmentComponent::Store)?,
                settings.docstore_compression,
                settings.docstore_blocksize,
                false,
            )?;
            for _ in 0..output.num_rows {
                store.store(&empty, &self.schema)?;
            }
            store.close()?;
        }
        Ok(())
    }

    fn write_metas(&mut self) -> tantivy::Result<()> {
        for output in &mut self.outputs {
            let segment = output.segment.clone().with_max_doc(output.num_rows);
            let meta = IndexMeta {
                index_settings: output.index.settings().clone(),
                segments: vec![segment.meta().clone()],
                schema: self.schema.clone(),
                opstamp: 0,
                payload: None,
            };
            let json = serde_json::to_vec(&meta)?;
            output.index.directory().sync_directory()?;
            output
                .index
                .directory()
                .atomic_write(Path::new(META_JSON_PATH), &json)?;
        }
        Ok(())
    }
}

/// Writes one term's docs into the outputs, opening the term on an output at its first doc and
/// closing it when the docs move on to the next output.
struct OutputDocs<'a, 'b> {
    key: &'a [u8],
    option: IndexRecordOption,
    doc_freqs: &'a [u32],
    outputs: &'a mut [FieldSerializer<'b>],
    open: Option<u16>,
    positions: &'a mut Vec<u32>,
    deltas: &'a mut Vec<u32>,
}

impl OutputDocs<'_, '_> {
    fn write(
        &mut self,
        dest_ord: u16,
        dest_row: u32,
        cursor: &mut SegmentPostings,
    ) -> io::Result<()> {
        // Without frequencies Tantivy expects zero, matching the empty position list.
        let term_freq = if self.option.has_freq() {
            cursor.term_freq()
        } else {
            0
        };
        self.positions.clear();
        if self.option.has_positions() {
            cursor.positions(self.positions);
        }
        let positions = std::mem::take(self.positions);
        let result = self.write_positions(dest_ord, dest_row, term_freq, &positions);
        *self.positions = positions;
        result
    }

    fn write_positions(
        &mut self,
        dest_ord: u16,
        dest_row: u32,
        term_freq: u32,
        positions: &[u32],
    ) -> io::Result<()> {
        if self.open != Some(dest_ord) {
            self.close()?;
            self.outputs[dest_ord as usize].new_term(
                self.key,
                self.doc_freqs[dest_ord as usize],
                self.option.has_freq(),
            )?;
            self.open = Some(dest_ord);
        }
        self.deltas.clear();
        let mut last = 0;
        for position in positions {
            self.deltas.push(position - last);
            last = *position;
        }
        self.outputs[dest_ord as usize].write_doc(dest_row, term_freq, self.deltas);
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        if let Some(dest_ord) = self.open.take() {
            self.outputs[dest_ord as usize].close_term()?;
        }
        Ok(())
    }
}

/// The merger reproduces what Databend's index writer produces: indexed text and JSON fields,
/// fast fields only on JSON, nothing stored.
fn check_schema(schema: &Schema) -> io::Result<()> {
    for (_, entry) in schema.fields() {
        if entry.is_stored() {
            return Err(invalid(format!("field {} is stored", entry.name())));
        }
        if entry.is_fast() && !matches!(entry.field_type(), FieldType::JsonObject(_)) {
            return Err(invalid(format!(
                "field {} is a non-JSON fast field",
                entry.name()
            )));
        }
        if !matches!(
            entry.field_type(),
            FieldType::Str(_) | FieldType::JsonObject(_)
        ) {
            return Err(invalid(format!(
                "field {} is neither text nor JSON",
                entry.name()
            )));
        }
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {

    use databend_common_base::runtime::GlobalIORuntime;
    use opendal::services::Memory;
    use tantivy::IndexSettings;
    use tantivy::SingleSegmentIndexWriter;
    use tantivy::directory::CompositeFile;
    use tantivy::directory::RamDirectory;
    use tantivy::schema::JsonObjectOptions;
    use tantivy::schema::OwnedValue;
    use tantivy::schema::TextFieldIndexing;
    use tantivy::schema::TextOptions;

    use super::*;
    use crate::init_test_runtime;
    use crate::inverted_index::InvertedIndexOutputDirectory;
    use crate::inverted_index::merge::test_util::walk;

    fn schema(option: IndexRecordOption, json: bool) -> Schema {
        let indexing = TextFieldIndexing::default()
            .set_tokenizer("default")
            .set_index_option(option);
        let mut builder = Schema::builder();
        builder.add_text_field(
            "title",
            TextOptions::default().set_indexing_options(indexing.clone()),
        );
        builder.add_text_field(
            "body",
            TextOptions::default().set_indexing_options(indexing.clone()),
        );
        if json {
            builder.add_json_field(
                "meta",
                JsonObjectOptions::default()
                    .set_indexing_options(indexing)
                    .set_fast(Some("raw")),
            );
        }
        builder.build()
    }

    fn settings() -> IndexSettings {
        IndexSettings {
            docstore_compress_dedicated_thread: false,
            ..Default::default()
        }
    }

    /// Row `id` of the whole data set; the same text regardless of which segment holds it.
    fn document(schema: &Schema, id: u32) -> TantivyDocument {
        let mut doc = TantivyDocument::new();
        doc.add_text(
            schema.get_field("title").unwrap(),
            format!("alpha row{} beta w{}", id % 13, id),
        );
        doc.add_text(
            schema.get_field("body").unwrap(),
            format!(
                "gamma gamma w{} delta {}",
                id % 101,
                if id % 7 == 0 { "seven" } else { "" }
            ),
        );
        if let Ok(meta) = schema.get_field("meta") {
            let json =
                serde_json::json!({ "tag": format!("t{}", id % 5), "n": id, "flag": id % 2 == 0 });
            doc.add_field_value(meta, &OwnedValue::from(json));
        }
        doc
    }

    fn build<D: Directory>(directory: D, schema: &Schema, ids: &[u32]) -> Index {
        let index = Index::create(directory, schema.clone(), settings()).unwrap();
        let mut writer = SingleSegmentIndexWriter::new(index, 16 * 1024 * 1024).unwrap();
        for id in ids {
            writer.add_document(document(schema, *id)).unwrap();
        }
        writer.finalize().unwrap()
    }

    fn write_object(operator: &Operator, path: &str, data: Vec<u8>) {
        let operator = operator.clone();
        let path = path.to_string();
        GlobalIORuntime::instance()
            .block_on(async move { Ok(operator.write(&path, data).await?) })
            .unwrap();
    }

    fn write_source(
        operator: &Operator,
        schema: &Schema,
        location: &str,
        ids: &[u32],
        threshold: usize,
    ) -> MergeSource {
        let directory = InvertedIndexOutputDirectory::with_stream_threshold(
            operator.clone(),
            location.to_string(),
            threshold,
        );
        let index = build(directory.clone(), schema, ids);
        let external_files = directory.external_files();
        let mut bundle = Vec::new();
        let sizes = InvertedIndexBundleBuilder::try_create(directory, index, external_files)
            .unwrap()
            .write_to(&mut bundle)
            .unwrap();
        write_object(operator, location, bundle);
        MergeSource {
            location: location.to_string(),
            bundle_size: sizes.bundle,
            num_rows: ids.len() as u32,
        }
    }

    fn meta_path(component: SegmentComponent) -> &'static str {
        match component {
            SegmentComponent::Terms => ".term",
            SegmentComponent::Postings => ".idx",
            SegmentComponent::Positions => ".pos",
            SegmentComponent::FieldNorms => ".fieldnorm",
            SegmentComponent::FastFields => ".fast",
            SegmentComponent::Store => ".store",
            _ => "other",
        }
    }

    fn component_bytes<D: Directory>(
        directory: &D,
        index: &Index,
        component: SegmentComponent,
    ) -> Vec<u8> {
        let meta = index.searchable_segment_metas().unwrap().remove(0);
        directory
            .atomic_read(&meta.relative_path(component))
            .unwrap()
    }

    /// `.idx` with the 8-byte `total_num_tokens` at the start of every field section zeroed.
    fn postings_without_totals(index: &Index, schema: &Schema) -> Vec<u8> {
        let meta = index.searchable_segment_metas().unwrap().remove(0);
        let path = meta.relative_path(SegmentComponent::Postings);
        let slice = index.directory().open_read(&path).unwrap();
        let composite = CompositeFile::open(&slice).unwrap();
        let mut sections = Vec::new();
        for (field, entry) in schema.fields() {
            if !entry.is_indexed() {
                continue;
            }
            let section = composite.open_read(field).unwrap().read_bytes().unwrap();
            let mut section = section.as_slice().to_vec();
            section[..8].fill(0);
            sections.push(section);
        }
        sections.concat()
    }

    struct Merged {
        operator: Operator,
        sizes: Vec<BundleSizes>,
        locations: Vec<String>,
    }

    impl Merged {
        fn open(&self, output: usize) -> (MergeSourceDirectory, Index) {
            let directory = MergeSourceDirectory::open(
                self.operator.clone(),
                self.locations[output].clone(),
                self.sizes[output].bundle,
            )
            .unwrap();
            let index = directory.open_index().unwrap();
            (directory, index)
        }

        /// Every component equals a fresh index of the same rows in the same order; `.idx`
        /// may differ only in the per-field token totals unless `exact_totals`.
        fn assert_matches(&self, output: usize, schema: &Schema, ids: &[u32], exact_totals: bool) {
            let (directory, index) = self.open(output);
            let reference = build(RamDirectory::default(), schema, ids);
            let reference_directory = reference.directory().clone();
            assert_eq!(walk(&index), walk(&reference), "output {output}");
            assert_eq!(
                index.load_metas().unwrap().segments[0].max_doc(),
                ids.len() as u32
            );
            for component in [
                SegmentComponent::Terms,
                SegmentComponent::Positions,
                SegmentComponent::FieldNorms,
                SegmentComponent::FastFields,
                SegmentComponent::Store,
            ] {
                assert_eq!(
                    component_bytes(&directory, &index, component),
                    component_bytes(&reference_directory, &reference, component),
                    "output {output} {:?}",
                    meta_path(component)
                );
            }
            let merged = component_bytes(&directory, &index, SegmentComponent::Postings);
            let expected =
                component_bytes(&reference_directory, &reference, SegmentComponent::Postings);
            if exact_totals {
                assert_eq!(merged, expected, "output {output} postings");
            } else {
                assert_eq!(
                    merged.len(),
                    expected.len(),
                    "output {output} postings length"
                );
                assert_eq!(
                    postings_without_totals(&index, schema),
                    postings_without_totals(&reference, schema),
                    "output {output} postings"
                );
            }
        }
    }

    fn merge(
        operator: &Operator,
        sources: Vec<MergeSource>,
        outputs: Vec<MergeOutput>,
        stream_threshold: usize,
        force_sorted: bool,
    ) -> Merged {
        let locations = outputs
            .iter()
            .map(|output| output.location.clone())
            .collect();
        let mut merger = InvertedIndexMerger::try_create_with_stream_threshold(
            operator.clone(),
            sources,
            outputs,
            stream_threshold,
        )
        .unwrap();
        merger.force_sorted_path = force_sorted;
        let sizes = merger.finish().unwrap();
        Merged {
            operator: operator.clone(),
            sizes,
            locations,
        }
    }

    fn output(location: &str, rows: Vec<(u32, Range<u32>)>) -> MergeOutput {
        let mut source_rows = Vec::with_capacity(rows.len());
        for (source, range) in rows {
            source_rows.push(SourceRows {
                source,
                rows: range,
            });
        }
        MergeOutput {
            location: location.to_string(),
            rows: source_rows,
        }
    }

    fn ids(range: Range<u32>) -> Vec<u32> {
        range.collect()
    }

    fn setup() -> Operator {
        init_test_runtime();
        Operator::new(Memory::default()).unwrap().finish()
    }

    #[test]
    fn test_two_sources_into_one_output_are_byte_identical_to_rebuilding() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, true);
        let sources = vec![
            write_source(&operator, &schema, "s/a.index", &ids(0..700), usize::MAX),
            write_source(&operator, &schema, "s/b.index", &ids(700..1500), 1024),
        ];
        let outputs = vec![output("o/all.index", vec![(0, 0..700), (1, 0..800)])];
        let merged = merge(&operator, sources, outputs, usize::MAX, false);
        // Sources map whole into the output, so token totals are exact and every byte matches.
        merged.assert_matches(0, &schema, &ids(0..1500), true);
        assert_eq!(merged.sizes[0].siblings, 0);
    }

    #[test]
    fn test_interleaved_sources_split_across_outputs() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, true);
        // Three sorted sources whose rows interleave in the merged order, cut into two outputs.
        let a = ids(0..500);
        let b = ids(500..900);
        let c = ids(900..1200);
        let sources = vec![
            write_source(&operator, &schema, "s/a.index", &a, 2048),
            write_source(&operator, &schema, "s/b.index", &b, usize::MAX),
            write_source(&operator, &schema, "s/c.index", &c, 2048),
        ];
        let outputs = vec![
            output("o/0.index", vec![
                (0, 0..200),
                (1, 0..100),
                (2, 0..50),
                (0, 200..300),
            ]),
            output("o/1.index", vec![
                (1, 100..400),
                (0, 300..500),
                (2, 50..300),
            ]),
        ];
        let merged = merge(&operator, sources, outputs, usize::MAX, false);
        let mut expected_0 = Vec::new();
        expected_0.extend_from_slice(&a[0..200]);
        expected_0.extend_from_slice(&b[0..100]);
        expected_0.extend_from_slice(&c[0..50]);
        expected_0.extend_from_slice(&a[200..300]);
        let mut expected_1 = Vec::new();
        expected_1.extend_from_slice(&b[100..400]);
        expected_1.extend_from_slice(&a[300..500]);
        expected_1.extend_from_slice(&c[50..300]);
        merged.assert_matches(0, &schema, &expected_0, false);
        merged.assert_matches(1, &schema, &expected_1, false);
    }

    #[test]
    fn test_heap_and_sorted_paths_write_the_same_bytes() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, true);
        let sources = || {
            vec![
                write_source(&operator, &schema, "s/a.index", &ids(0..300), usize::MAX),
                write_source(&operator, &schema, "s/b.index", &ids(300..600), usize::MAX),
            ]
        };
        let outputs = |prefix: &str| {
            vec![
                output(&format!("{prefix}/0.index"), vec![(1, 0..100), (0, 0..150)]),
                output(&format!("{prefix}/1.index"), vec![
                    (0, 150..300),
                    (1, 100..300),
                ]),
            ]
        };
        let heap = merge(&operator, sources(), outputs("h"), usize::MAX, false);
        let sorted = merge(&operator, sources(), outputs("s"), usize::MAX, true);
        for output in 0..2 {
            let (heap_dir, heap_index) = heap.open(output);
            let (sorted_dir, sorted_index) = sorted.open(output);
            for component in [
                SegmentComponent::Terms,
                SegmentComponent::Postings,
                SegmentComponent::Positions,
                SegmentComponent::FieldNorms,
            ] {
                assert_eq!(
                    component_bytes(&heap_dir, &heap_index, component),
                    component_bytes(&sorted_dir, &sorted_index, component),
                    "output {output} {:?}",
                    meta_path(component)
                );
            }
        }
    }

    #[test]
    fn test_arbitrary_row_order_and_dropped_rows() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, false);
        let sources = vec![
            write_source(&operator, &schema, "s/a.index", &ids(0..400), usize::MAX),
            write_source(&operator, &schema, "s/b.index", &ids(400..600), usize::MAX),
        ];
        // Source 0 is visited out of order (not monotonic) and rows 100..150 of it are dropped.
        let outputs = vec![output("o/0.index", vec![
            (0, 300..400),
            (1, 0..200),
            (0, 0..100),
            (0, 150..300),
        ])];
        let merged = merge(&operator, sources, outputs, usize::MAX, false);
        let mut expected = ids(300..400);
        expected.extend(ids(400..600));
        expected.extend(ids(0..100));
        expected.extend(ids(150..300));
        merged.assert_matches(0, &schema, &expected, false);
    }

    #[test]
    fn test_record_options_without_positions() {
        let operator = setup();
        for option in [IndexRecordOption::Basic, IndexRecordOption::WithFreqs] {
            let schema = schema(option, true);
            let sources = vec![
                write_source(&operator, &schema, "s/a.index", &ids(0..300), usize::MAX),
                write_source(&operator, &schema, "s/b.index", &ids(300..500), usize::MAX),
            ];
            let outputs = vec![output("o/0.index", vec![(0, 0..300), (1, 0..200)])];
            let merged = merge(&operator, sources, outputs, usize::MAX, false);
            merged.assert_matches(0, &schema, &ids(0..500), true);
        }
    }

    #[test]
    fn test_large_outputs_get_sibling_objects() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, false);
        let sources = vec![write_source(
            &operator,
            &schema,
            "s/a.index",
            &ids(0..2000),
            usize::MAX,
        )];
        let outputs = vec![output("o/0.index", vec![(0, 0..2000)])];
        let merged = merge(&operator, sources, outputs, 1024, false);
        assert!(merged.sizes[0].siblings > 0);
        let (directory, _) = merged.open(0);
        assert_eq!(directory.footer().external_files.files.len(), 2);
        merged.assert_matches(0, &schema, &ids(0..2000), true);
    }

    #[test]
    fn test_invalid_row_assignments_are_rejected() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, false);
        let source = || write_source(&operator, &schema, "s/a.index", &ids(0..10), usize::MAX);
        let cases: Vec<(&str, Vec<MergeOutput>)> = vec![
            ("twice", vec![output("o/0.index", vec![
                (0, 0..5),
                (0, 3..10),
            ])]),
            ("unknown source", vec![output("o/0.index", vec![(1, 0..5)])]),
            ("out of range", vec![output("o/0.index", vec![(0, 0..11)])]),
        ];
        for (name, outputs) in cases {
            let result = InvertedIndexMerger::try_create(operator.clone(), vec![source()], outputs);
            assert!(result.is_err(), "{name}");
        }
    }
}
