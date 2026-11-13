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

//! Writing one output segment from the slices its windows deliver, in window order.
//!
//! Tantivy serializes a segment front to back: fieldnorms, then every field's terms in order,
//! then fast fields, store and meta. Windows arrive over time, so the writer is an `async`
//! state machine that suspends while it waits for the next slice and is driven by hand from
//! the pipeline processor. No runtime is involved: a poll runs until the queue is empty.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use databend_storages_common_io::BLOCKING_WRITE_MAX_CHUNKS;
use databend_storages_common_io::BlockingWrite;
use databend_storages_common_io::create_blocking_write;
use opendal::Operator;
use tantivy::Directory;
use tantivy::Index;
use tantivy::IndexMeta;
use tantivy::IndexSettings;
use tantivy::Segment;
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
use tantivy::schema::Field;
use tantivy::schema::Schema;
use tantivy::schema::TantivyDocument;
use tantivy::store::StoreWriter;

use super::invalid;
use super::source::MergeSourceReader;
use super::window::OutputSlice;
use crate::RowMapping;
use crate::inverted_index::bundle::META_JSON_PATH;
use crate::inverted_index::bundle_builder::BundleSizes;
use crate::inverted_index::bundle_builder::InvertedIndexBundleBuilder;
use crate::inverted_index::output_directory::INVERTED_INDEX_STREAM_THRESHOLD;
use crate::inverted_index::output_directory::InvertedIndexOutputDirectory;

pub type MergeInterruptCheck = Arc<dyn Fn() -> io::Result<()> + Send + Sync>;

/// What one output segment is built from.
pub struct OutputWriteContext {
    pub operator: Operator,
    /// Bundle object location; large components stream to sibling objects.
    pub location: String,
    pub schema: Schema,
    pub settings: IndexSettings,
    /// Output `.idx` / `.pos` larger than this become sibling objects.
    pub stream_threshold: usize,
    /// Every source, in source order.
    pub sources: Vec<MergeSourceReader>,
    pub mapping: Arc<RowMapping>,
    pub output: usize,
    pub check_interrupt: MergeInterruptCheck,
}

impl OutputWriteContext {
    pub fn new(
        operator: Operator,
        location: String,
        schema: Schema,
        settings: IndexSettings,
        sources: Vec<MergeSourceReader>,
        mapping: Arc<RowMapping>,
        output: usize,
        check_interrupt: MergeInterruptCheck,
    ) -> Self {
        Self {
            operator,
            location,
            schema,
            settings,
            stream_threshold: INVERTED_INDEX_STREAM_THRESHOLD,
            sources,
            mapping,
            output,
            check_interrupt,
        }
    }
}

#[derive(Default)]
struct SliceQueue {
    items: VecDeque<OutputSlice>,
    closed: bool,
}

struct NextSlice<'a>(&'a Mutex<SliceQueue>);

impl Future for NextSlice<'_> {
    type Output = Option<OutputSlice>;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let mut queue = self.0.lock().unwrap();
        match queue.items.pop_front() {
            Some(slice) => Poll::Ready(Some(slice)),
            None if queue.closed => Poll::Ready(None),
            None => Poll::Pending,
        }
    }
}

/// Builds one output segment; feed it the output's slices in window order, then close it.
pub struct OutputWriter {
    queue: Arc<Mutex<SliceQueue>>,
    future: Pin<Box<dyn Future<Output = tantivy::Result<BundleSizes>> + Send>>,
    result: Option<tantivy::Result<BundleSizes>>,
}

impl OutputWriter {
    pub fn new(context: OutputWriteContext) -> Self {
        let queue = Arc::new(Mutex::new(SliceQueue::default()));
        let future = Box::pin(write_output(context, queue.clone()));
        Self {
            queue,
            future,
            result: None,
        }
    }

    /// Queues the next window's slice; slices must arrive in window order.
    pub fn push(&self, slice: OutputSlice) {
        self.queue.lock().unwrap().items.push_back(slice);
    }

    /// No more slices: the next poll finishes the segment and writes the bundle.
    pub fn close(&self) {
        self.queue.lock().unwrap().closed = true;
    }

    /// Runs the writer until it needs another slice or has finished. Blocking storage I/O
    /// happens inside, so call this from an executor thread. An error is returned once; the
    /// writer is unusable afterwards.
    pub fn poll(&mut self) -> Poll<tantivy::Result<()>> {
        if self.result.is_none() {
            let waker = Waker::noop();
            let mut context = Context::from_waker(waker);
            if let Poll::Ready(result) = self.future.as_mut().poll(&mut context) {
                self.result = Some(result);
            }
        }
        match self.result.as_mut() {
            None => Poll::Pending,
            Some(Ok(_)) => Poll::Ready(Ok(())),
            Some(slot @ Err(_)) => {
                let error = std::mem::replace(
                    slot,
                    Err(io::Error::other("output writer failed earlier").into()),
                );
                Poll::Ready(error.map(|_| ()))
            }
        }
    }

    /// The finished bundle sizes, once [`Self::poll`] returned ready.
    pub fn into_result(self) -> tantivy::Result<BundleSizes> {
        self.result.unwrap_or_else(|| {
            Err(io::Error::other("output writer was not polled to completion").into())
        })
    }
}

async fn write_output(
    context: OutputWriteContext,
    queue: Arc<Mutex<SliceQueue>>,
) -> tantivy::Result<BundleSizes> {
    let OutputWriteContext {
        operator,
        location,
        schema,
        settings,
        stream_threshold,
        sources,
        mapping,
        output,
        check_interrupt,
    } = context;
    check_interrupt()?;
    let directory = InvertedIndexOutputDirectory::with_stream_threshold(
        operator.clone(),
        location.clone(),
        stream_threshold,
    );
    let index = Index::create(directory.clone(), schema.clone(), settings.clone())?;
    let mut segment = index.new_segment();
    let num_rows = mapping.output_rows(output);
    let origins = output_row_addrs(&mapping, output);

    write_fieldnorms(&mut segment, &schema, &sources, &origins, &check_interrupt)?;
    let fieldnorm_readers =
        FieldNormReaders::open(segment.open_read(SegmentComponent::FieldNorms)?)?;

    {
        let mut serializer = InvertedIndexSerializer::open(&mut segment)?;
        let mut previous_field: Option<Field> = None;
        // First slice of the next field, read while finishing the previous one.
        let mut carried: Option<OutputSlice> = None;
        let mut closed = false;
        while !closed {
            let first = match carried.take() {
                Some(slice) => slice,
                None => match NextSlice(&queue).await {
                    Some(slice) => slice,
                    None => break,
                },
            };
            check_interrupt()?;
            let field = first.field;
            if previous_field.is_some_and(|previous| previous >= field) {
                return Err(invalid("output slices must arrive in field order").into());
            }
            previous_field = Some(field);
            let total_num_tokens = total_num_tokens(&sources, &mapping, output, field)?;
            // The field serializer borrows the segment serializer; keep it in a block that
            // ends before the next field opens.
            let mut field_serializer = serializer.new_field(
                field,
                total_num_tokens,
                fieldnorm_readers.get_field(field)?,
            )?;
            write_slice(&mut field_serializer, &first, &check_interrupt)?;
            loop {
                let Some(slice) = NextSlice(&queue).await else {
                    closed = true;
                    break;
                };
                if slice.field != field {
                    carried = Some(slice);
                    break;
                }
                check_interrupt()?;
                write_slice(&mut field_serializer, &slice, &check_interrupt)?;
            }
            field_serializer.close()?;
        }
        serializer.close()?;
    }

    check_interrupt()?;
    write_fast_fields(&mut segment, &sources, origins)?;
    check_interrupt()?;
    write_stores(&mut segment, &schema, &settings, num_rows, &check_interrupt)?;
    write_metas(&mut segment, &index, &schema, num_rows)?;
    check_interrupt()?;

    let external_files = directory.external_files();
    let builder = InvertedIndexBundleBuilder::try_create(directory, index, external_files)?;
    let mut sink = create_blocking_write(operator, location, BLOCKING_WRITE_MAX_CHUNKS);
    let sizes = builder.write_to(&mut sink)?;
    sink.close()
        .map_err(|error| io::Error::other(error.to_string()))?;
    Ok(sizes)
}

/// Writes the terms of one slice through the open field serializer.
fn write_slice(
    field_serializer: &mut FieldSerializer<'_>,
    slice: &OutputSlice,
    check_interrupt: &MergeInterruptCheck,
) -> tantivy::Result<()> {
    let mut written = 0usize;
    for term in &slice.terms {
        let docs = slice.term_docs(term);
        field_serializer.new_term(slice.term_key(term), docs.len() as u32, term.record_freq)?;
        for doc in docs {
            written += 1;
            if written % 4096 == 0 {
                check_interrupt()?;
            }
            field_serializer.write_doc(doc.dest_row, doc.term_freq, slice.doc_positions(doc));
        }
        field_serializer.close_term()?;
    }
    Ok(())
}

fn write_fieldnorms(
    segment: &mut Segment,
    schema: &Schema,
    sources: &[MergeSourceReader],
    origins: &[RowAddr],
    check_interrupt: &MergeInterruptCheck,
) -> tantivy::Result<()> {
    let mut serializer =
        FieldNormsSerializer::from_write(segment.open_write(SegmentComponent::FieldNorms)?)?;
    let mut data = vec![0u8; origins.len()];
    for (field, entry) in schema.fields() {
        if !entry.is_indexed() || !entry.has_fieldnorms() {
            continue;
        }
        let mut readers = Vec::with_capacity(sources.len());
        for source in sources {
            let Some(reader) = source.reader.fieldnorms_readers().get_field(field)? else {
                return Err(invalid(format!("source lacks fieldnorms for {field:?}")).into());
            };
            readers.push(reader);
        }
        for (dest_row, origin) in origins.iter().enumerate() {
            if dest_row % 4096 == 0 {
                check_interrupt()?;
            }
            data[dest_row] = readers[origin.segment_ord as usize].fieldnorm_id(origin.row_id);
        }
        serializer.serialize_field(field, &data)?;
    }
    serializer.close()?;
    Ok(())
}

/// Token total of `field` for the output. Exact for a source that lies entirely in the output;
/// otherwise estimated from fieldnorms, as Tantivy does for segments with deletes. Only BM25's
/// average length depends on it.
fn total_num_tokens(
    sources: &[MergeSourceReader],
    mapping: &RowMapping,
    output: usize,
    field: Field,
) -> tantivy::Result<u64> {
    let mut total = 0u64;
    for (source_ord, source) in sources.iter().enumerate() {
        let rows = mapping.rows_in(source_ord, output);
        if rows == 0 {
            continue;
        }
        let source_total = source.reader.inverted_index(field)?.total_num_tokens();
        if mapping.fully_in(source_ord, output) {
            total += source_total;
            continue;
        }
        let num_rows = mapping.source_rows(source_ord);
        match source.reader.fieldnorms_readers().get_field(field)? {
            Some(fieldnorms) => {
                for row in 0..num_rows {
                    if mapping.destination(source_ord, row).0 as usize == output {
                        total += u64::from(FieldNormReader::id_to_fieldnorm(
                            fieldnorms.fieldnorm_id(row),
                        ));
                    }
                }
            }
            None => {
                let ratio = rows as f64 / num_rows as f64;
                total += (source_total as f64 * ratio) as u64;
            }
        }
    }
    Ok(total)
}

/// Tantivy address `(source, source row)` of every row of `output`, in output row order.
fn output_row_addrs(mapping: &RowMapping, output: usize) -> Vec<RowAddr> {
    let mut origins = Vec::with_capacity(mapping.output_rows(output) as usize);
    for lineage in mapping.output_lineage(output) {
        for row in lineage.rows.clone() {
            origins.push(RowAddr {
                segment_ord: lineage.source,
                row_id: row,
            });
        }
    }
    origins
}

fn write_fast_fields(
    segment: &mut Segment,
    sources: &[MergeSourceReader],
    origins: Vec<RowAddr>,
) -> tantivy::Result<()> {
    let mut columnars = Vec::with_capacity(sources.len());
    for source in sources {
        let meta = source.index.searchable_segment_metas()?.remove(0);
        let path = meta.relative_path(SegmentComponent::FastFields);
        let file = source.index.directory().open_read(&path)?;
        columnars.push(ColumnarReader::open(file)?);
    }
    let readers = columnars.iter().collect::<Vec<_>>();
    let mut write = segment.open_write(SegmentComponent::FastFields)?;
    let order = MergeRowOrder::Shuffled(ShuffleMergeOrder {
        new_row_id_to_old_row_id: origins,
        alive_bitsets: vec![None; readers.len()],
    });
    merge_columnar(&readers, &[], order, &mut write)?;
    write.terminate()?;
    Ok(())
}

/// Databend stores no fields, so every doc is an empty document; writing them keeps the store
/// identical to what indexing the rows produces.
fn write_stores(
    segment: &mut Segment,
    schema: &Schema,
    settings: &IndexSettings,
    num_rows: u32,
    check_interrupt: &MergeInterruptCheck,
) -> tantivy::Result<()> {
    let empty = TantivyDocument::default();
    let mut store = StoreWriter::new(
        segment.open_write(SegmentComponent::Store)?,
        settings.docstore_compression,
        settings.docstore_blocksize,
        false,
    )?;
    for row in 0..num_rows {
        if row % 4096 == 0 {
            check_interrupt()?;
        }
        store.store(&empty, schema)?;
    }
    store.close()?;
    Ok(())
}

fn write_metas(
    segment: &mut Segment,
    index: &Index,
    schema: &Schema,
    num_rows: u32,
) -> tantivy::Result<()> {
    let segment = segment.clone().with_max_doc(num_rows);
    let meta = IndexMeta {
        index_settings: index.settings().clone(),
        segments: vec![segment.meta().clone()],
        schema: schema.clone(),
        opstamp: 0,
        payload: None,
    };
    let json = serde_json::to_vec(&meta)?;
    index.directory().sync_directory()?;
    index
        .directory()
        .atomic_write(Path::new(META_JSON_PATH), &json)?;
    Ok(())
}
