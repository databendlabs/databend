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

//! The merge as a whole: open sources, scan their term costs, cut windows, merge every window
//! and feed the output writers. The pipeline spreads these steps over processors; the
//! in-process driver here runs them one after another, for tests and small merges.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::task::Poll;

use opendal::Operator;
use tantivy::IndexSettings;
use tantivy::schema::Field;
use tantivy::schema::Schema;

use super::output::MergeInterruptCheck;
use super::output::OutputWriteContext;
use super::output::OutputWriter;
use super::plan::FieldCostTable;
use super::plan::TERM_COST_CHECKPOINT_BYTES;
use super::plan::TERM_WINDOW_BYTES;
use super::plan::TermWindow;
use super::plan::plan_field_windows;
use super::plan::scan_field_costs;
use super::source::MergeSource;
use super::source::MergeSourceReader;
use super::source::open_merge_source;
use super::window::merge_window;
use crate::RowMapping;
use crate::SourceRows;
use crate::inverted_index::bundle_builder::BundleSizes;
use crate::inverted_index::output_directory::INVERTED_INDEX_STREAM_THRESHOLD;

/// Fields of `schema` the merge serializes, in the order every output writes them.
pub fn indexed_fields(schema: &Schema) -> Vec<Field> {
    schema
        .fields()
        .filter(|(_, entry)| entry.is_indexed())
        .map(|(field, _)| field)
        .collect()
}

/// Checkpoint spacing for `window_bytes` windows: the default spacing, or finer when the
/// windows themselves are small.
pub fn checkpoint_bytes_for(window_bytes: u64) -> u64 {
    TERM_COST_CHECKPOINT_BYTES.min(window_bytes / 64).max(1)
}

/// Cost tables of every indexed field of one source, with a checkpoint every `interval` bytes.
pub fn scan_source_costs(
    source: &MergeSourceReader,
    schema: &Schema,
    interval: u64,
) -> tantivy::Result<BTreeMap<Field, FieldCostTable>> {
    let mut tables = BTreeMap::new();
    for field in indexed_fields(schema) {
        let inverted = source.reader.inverted_index(field)?;
        tables.insert(field, scan_field_costs(&inverted, interval)?);
    }
    Ok(tables)
}

/// Cuts the windows of one index from the cost tables of its sources, numbering them from
/// `first_id`, field after field in schema order.
pub fn plan_index_windows(
    schema: &Schema,
    tables: &[BTreeMap<Field, FieldCostTable>],
    window_bytes: u64,
    index: u16,
    first_id: u32,
) -> Vec<TermWindow> {
    let mut windows = Vec::new();
    let mut next_id = first_id;
    for field in indexed_fields(schema) {
        let field_tables = tables
            .iter()
            .map(|source| source.get(&field).cloned().unwrap_or_default())
            .collect::<Vec<_>>();
        let field_windows = plan_field_windows(&field_tables, window_bytes, index, field, next_id);
        next_id += field_windows.len() as u32;
        windows.extend(field_windows);
    }
    windows
}

/// One output of an in-process merge.
#[derive(Debug, Clone)]
pub struct MergeOutput {
    pub location: String,
    pub rows: Vec<SourceRows>,
}

/// Knobs of an in-process merge.
#[derive(Debug, Clone, Copy)]
pub struct MergeOptions {
    /// Posting bytes per term window.
    pub window_bytes: u64,
    /// Output `.idx` / `.pos` larger than this become sibling objects.
    pub stream_threshold: usize,
}

impl Default for MergeOptions {
    fn default() -> Self {
        Self {
            window_bytes: TERM_WINDOW_BYTES,
            stream_threshold: INVERTED_INDEX_STREAM_THRESHOLD,
        }
    }
}

/// Merges `sources` into `outputs` on the calling thread, window by window. Blocking: reads
/// and writes wait for the IO runtime, so this must run on an executor thread.
pub fn merge_in_process(
    operator: Operator,
    sources: &[MergeSource],
    outputs: Vec<MergeOutput>,
    schema: &Schema,
    settings: IndexSettings,
    options: MergeOptions,
    check_interrupt: MergeInterruptCheck,
) -> tantivy::Result<Vec<BundleSizes>> {
    check_interrupt()?;
    if sources.is_empty() || outputs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "index merge requires nonempty sources and outputs",
        )
        .into());
    }
    let mut source_rows = Vec::with_capacity(sources.len());
    for source in sources {
        source_rows.push(source.num_rows);
    }
    let mut locations = Vec::with_capacity(outputs.len());
    let mut lineage = Vec::with_capacity(outputs.len());
    for output in outputs {
        locations.push(output.location);
        lineage.push(output.rows);
    }
    let mapping = Arc::new(RowMapping::build(&source_rows, lineage)?);

    let mut readers = Vec::with_capacity(sources.len());
    let mut tables = Vec::with_capacity(sources.len());
    for source in sources {
        check_interrupt()?;
        let reader = open_merge_source(operator.clone(), source, Some(schema))?;
        tables.push(scan_source_costs(
            &reader,
            schema,
            checkpoint_bytes_for(options.window_bytes),
        )?);
        readers.push(reader);
    }
    let windows = plan_index_windows(schema, &tables, options.window_bytes, 0, 0);

    let mut writers = Vec::with_capacity(locations.len());
    for (output, location) in locations.into_iter().enumerate() {
        let mut context = OutputWriteContext::new(
            operator.clone(),
            location,
            schema.clone(),
            settings.clone(),
            readers.clone(),
            mapping.clone(),
            output,
            check_interrupt.clone(),
        );
        context.stream_threshold = options.stream_threshold;
        writers.push(OutputWriter::new(context));
    }

    for window in &windows {
        let slices = merge_window(&readers, &mapping, window, check_interrupt.as_ref())?;
        for (writer, slice) in writers.iter_mut().zip(slices) {
            writer.push(slice);
            if let Poll::Ready(Err(error)) = writer.poll() {
                return Err(error);
            }
        }
    }

    let mut sizes = Vec::with_capacity(writers.len());
    for mut writer in writers {
        writer.close();
        match writer.poll() {
            Poll::Ready(result) => {
                result?;
                sizes.push(writer.into_result()?);
            }
            Poll::Pending => {
                return Err(io::Error::other("output writer did not finish after close").into());
            }
        }
    }
    Ok(sizes)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use databend_common_base::runtime::GlobalIORuntime;
    use opendal::Error as StorageError;
    use opendal::ErrorKind;
    use opendal::Result as StorageResult;
    use opendal::raw::Access;
    use opendal::raw::Layer;
    use opendal::raw::LayeredAccess;
    use opendal::raw::OpList;
    use opendal::raw::OpRead;
    use opendal::raw::OpWrite;
    use opendal::raw::RpDelete;
    use opendal::raw::RpList;
    use opendal::raw::RpRead;
    use opendal::raw::RpWrite;
    use opendal::services::Memory;
    use tantivy::Directory;
    use tantivy::Index;
    use tantivy::SingleSegmentIndexWriter;
    use tantivy::directory::CompositeFile;
    use tantivy::directory::RamDirectory;
    use tantivy::index::SegmentComponent;
    use tantivy::schema::IndexRecordOption;
    use tantivy::schema::JsonObjectOptions;
    use tantivy::schema::OwnedValue;
    use tantivy::schema::TantivyDocument;
    use tantivy::schema::TextFieldIndexing;
    use tantivy::schema::TextOptions;

    use super::*;
    use crate::init_test_runtime;
    use crate::inverted_index::InvertedIndexBundleBuilder;
    use crate::inverted_index::InvertedIndexOutputDirectory;
    use crate::inverted_index::MergeSourceDirectory;
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

    fn no_interrupt() -> MergeInterruptCheck {
        Arc::new(|| Ok(()))
    }

    fn merge(
        operator: &Operator,
        schema: &Schema,
        sources: Vec<MergeSource>,
        outputs: Vec<MergeOutput>,
        window_bytes: u64,
    ) -> Merged {
        let locations = outputs
            .iter()
            .map(|output| output.location.clone())
            .collect();
        let sizes = merge_in_process(
            operator.clone(),
            &sources,
            outputs,
            schema,
            settings(),
            MergeOptions {
                window_bytes,
                ..Default::default()
            },
            no_interrupt(),
        )
        .unwrap();
        Merged {
            operator: operator.clone(),
            sizes,
            locations,
        }
    }

    fn output(location: &str, rows: Vec<(u32, Range<u32>)>) -> MergeOutput {
        MergeOutput {
            location: location.to_string(),
            rows: rows
                .into_iter()
                .map(|(source, rows)| SourceRows { source, rows })
                .collect(),
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
        for window_bytes in [u64::MAX, 4096, 1] {
            let merged = merge(
                &operator,
                &schema,
                sources.clone(),
                outputs.clone(),
                window_bytes,
            );
            // Sources map whole into the output, so token totals are exact and every byte
            // matches, whatever the window size.
            merged.assert_matches(0, &schema, &ids(0..1500), true);
            assert_eq!(merged.sizes[0].siblings, 0);
        }
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
        let merged = merge(&operator, &schema, sources, outputs, 2048);
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
    fn test_arbitrary_row_order_is_written_correctly() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, false);
        let sources = vec![
            write_source(&operator, &schema, "s/a.index", &ids(0..400), usize::MAX),
            write_source(&operator, &schema, "s/b.index", &ids(400..600), usize::MAX),
        ];
        // Source 0 is visited out of order: every row still lands in the right place.
        let outputs = vec![output("o/0.index", vec![
            (0, 300..400),
            (1, 0..200),
            (0, 0..100),
            (0, 150..300),
            (0, 100..150),
        ])];
        let merged = merge(&operator, &schema, sources, outputs, 1024);
        let mut expected = ids(300..400);
        expected.extend(ids(400..600));
        expected.extend(ids(0..100));
        expected.extend(ids(150..300));
        expected.extend(ids(100..150));
        merged.assert_matches(0, &schema, &expected, true);
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
            let merged = merge(&operator, &schema, sources, outputs, 512);
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
        let outputs = [MergeOutput {
            location: "o/0.index".to_string(),
            rows: vec![SourceRows {
                source: 0,
                rows: 0..2000,
            }],
        }];
        let locations = vec!["o/0.index".to_string()];
        let mut readers = Vec::new();
        for source in &sources {
            readers.push(open_merge_source(operator.clone(), source, Some(&schema)).unwrap());
        }
        let mapping = Arc::new(
            RowMapping::build(&[2000], outputs.iter().map(|o| o.rows.clone()).collect()).unwrap(),
        );
        let tables = readers
            .iter()
            .map(|reader| scan_source_costs(reader, &schema, checkpoint_bytes_for(1024)).unwrap())
            .collect::<Vec<_>>();
        let windows = plan_index_windows(&schema, &tables, 1024, 0, 0);
        assert!(windows.len() > 2);
        let mut context = OutputWriteContext::new(
            operator.clone(),
            locations[0].clone(),
            schema.clone(),
            settings(),
            readers.clone(),
            mapping.clone(),
            0,
            no_interrupt(),
        );
        context.stream_threshold = 1024;
        let mut writer = OutputWriter::new(context);
        for window in &windows {
            let slices = merge_window(&readers, &mapping, window, &|| Ok(())).unwrap();
            writer.push(slices.into_iter().next().unwrap());
            assert!(writer.poll().is_pending());
        }
        writer.close();
        assert!(writer.poll().is_ready());
        let sizes = writer.into_result().unwrap();
        assert!(sizes.siblings > 0);
        let merged = Merged {
            operator: operator.clone(),
            sizes: vec![sizes],
            locations,
        };
        let (directory, _) = merged.open(0);
        assert_eq!(directory.footer().external_files.files.len(), 2);
        merged.assert_matches(0, &schema, &ids(0..2000), true);
    }

    #[test]
    fn test_windows_partition_every_term_exactly_once() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, true);
        let sources = [
            write_source(&operator, &schema, "s/a.index", &ids(0..900), usize::MAX),
            write_source(&operator, &schema, "s/b.index", &ids(300..1200), usize::MAX),
        ];
        let readers = sources
            .iter()
            .map(|source| open_merge_source(operator.clone(), source, Some(&schema)).unwrap())
            .collect::<Vec<_>>();
        let tables = readers
            .iter()
            .map(|reader| scan_source_costs(reader, &schema, checkpoint_bytes_for(1024)).unwrap())
            .collect::<Vec<_>>();
        let windows = plan_index_windows(&schema, &tables, 2048, 5, 100);
        assert_eq!(windows[0].id, 100);
        assert!(windows.windows(2).all(|pair| pair[0].id + 1 == pair[1].id));
        for field in indexed_fields(&schema) {
            let field_windows = windows
                .iter()
                .filter(|window| window.field == field)
                .collect::<Vec<_>>();
            assert!(field_windows.len() > 1, "{field:?}");
            assert!(field_windows[0].lower.is_none());
            assert!(field_windows.last().unwrap().upper.is_none());
            for pair in field_windows.windows(2) {
                assert_eq!(pair[0].upper, pair[1].lower);
            }
            for reader in &readers {
                let inverted = reader.reader.inverted_index(field).unwrap();
                let mut stream = inverted.terms().stream().unwrap();
                while stream.advance() {
                    let owners = field_windows
                        .iter()
                        .filter(|window| window.contains(stream.key()))
                        .count();
                    assert_eq!(owners, 1);
                }
            }
        }
    }

    #[derive(Clone, Debug)]
    struct IndexWriteFailureLayer;

    impl<A: Access> Layer<A> for IndexWriteFailureLayer {
        type LayeredAccess = IndexWriteFailureAccessor<A>;
        fn layer(&self, inner: A) -> Self::LayeredAccess {
            IndexWriteFailureAccessor { inner }
        }
    }

    #[derive(Debug)]
    struct IndexWriteFailureAccessor<A> {
        inner: A,
    }

    impl<A: Access> LayeredAccess for IndexWriteFailureAccessor<A> {
        type Inner = A;
        type Reader = A::Reader;
        type Writer = A::Writer;
        type Lister = A::Lister;
        type Deleter = A::Deleter;
        fn inner(&self) -> &A {
            &self.inner
        }
        async fn read(&self, path: &str, args: OpRead) -> StorageResult<(RpRead, Self::Reader)> {
            self.inner.read(path, args).await
        }
        async fn delete(&self) -> StorageResult<(RpDelete, Self::Deleter)> {
            self.inner.delete().await
        }
        async fn list(&self, path: &str, args: OpList) -> StorageResult<(RpList, Self::Lister)> {
            self.inner.list(path, args).await
        }
        async fn write(&self, path: &str, args: OpWrite) -> StorageResult<(RpWrite, Self::Writer)> {
            if path.starts_with("o/") {
                return Err(StorageError::new(
                    ErrorKind::PermissionDenied,
                    "injected index write failure",
                ));
            }
            self.inner.write(path, args).await
        }
    }

    #[test]
    fn test_write_failure_never_returns_completed_sizes() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, false);
        let source = write_source(
            &operator,
            &schema,
            "s/write.index",
            &ids(0..100),
            usize::MAX,
        );
        let failed = operator.clone().layer(IndexWriteFailureLayer);
        let result = merge_in_process(
            failed,
            &[source],
            vec![output("o/write.index", vec![(0, 0..100)])],
            &schema,
            settings(),
            MergeOptions::default(),
            no_interrupt(),
        );
        assert!(result.is_err());
        let exists = GlobalIORuntime::instance()
            .block_on(async { Ok(operator.exists("o/write.index").await?) })
            .unwrap();
        assert!(!exists);
    }

    #[test]
    fn test_cancel_during_postings_does_not_publish_bundle() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, false);
        let source = write_source(
            &operator,
            &schema,
            "s/cancel.index",
            &ids(0..10000),
            usize::MAX,
        );
        let armed = Arc::new(AtomicBool::new(false));
        let checks = Arc::new(AtomicUsize::new(0));
        let flag = armed.clone();
        let counter = checks.clone();
        let check: MergeInterruptCheck = Arc::new(move || {
            if flag.load(Ordering::Relaxed) && counter.fetch_add(1, Ordering::Relaxed) >= 20 {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "cancelled during postings",
                ));
            }
            Ok(())
        });
        armed.store(true, Ordering::Relaxed);
        let Err(tantivy::TantivyError::IoError(error)) = merge_in_process(
            operator.clone(),
            &[source],
            vec![output("o/cancel.index", vec![(0, 0..10000)])],
            &schema,
            settings(),
            MergeOptions {
                window_bytes: 4096,
                ..Default::default()
            },
            check,
        ) else {
            panic!("cancellation must preserve its IO error variant");
        };
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(checks.load(Ordering::Relaxed) >= 20);
        let exists = GlobalIORuntime::instance()
            .block_on(async { Ok(operator.exists("o/cancel.index").await?) })
            .unwrap();
        assert!(!exists);
    }

    #[test]
    fn test_invalid_sources_and_lineage_are_rejected() {
        let operator = setup();
        let schema = schema(IndexRecordOption::WithFreqsAndPositions, false);
        let source = || write_source(&operator, &schema, "s/a.index", &ids(0..10), usize::MAX);
        let cases: Vec<(&str, Vec<MergeOutput>)> = vec![
            ("twice", vec![output("o/0.index", vec![
                (0, 0..5),
                (0, 3..10),
            ])]),
            ("incomplete", vec![output("o/0.index", vec![(0, 0..5)])]),
            ("unknown source", vec![output("o/0.index", vec![(1, 0..5)])]),
            ("out of range", vec![output("o/0.index", vec![(0, 0..11)])]),
        ];
        for (name, outputs) in cases {
            let result = merge_in_process(
                operator.clone(),
                &[source()],
                outputs,
                &schema,
                settings(),
                MergeOptions::default(),
                no_interrupt(),
            );
            assert!(result.is_err(), "{name}");
        }
        let wrong = Schema::builder().build();
        assert!(
            merge_in_process(
                operator.clone(),
                &[source()],
                vec![output("o/schema.index", vec![(0, 0..10)])],
                &wrong,
                settings(),
                MergeOptions::default(),
                no_interrupt(),
            )
            .is_err()
        );
        let exists = GlobalIORuntime::instance()
            .block_on(async { Ok(operator.exists("o/schema.index").await?) })
            .unwrap();
        assert!(!exists);
        let mut missing = source();
        missing.num_rows = 11;
        assert!(open_merge_source(operator.clone(), &missing, Some(&schema)).is_err());
    }
}
