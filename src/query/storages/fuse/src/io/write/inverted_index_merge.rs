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
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_generate_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_index::MergeInterruptCheck;
use databend_storages_common_index::MergeSource;
use databend_storages_common_index::MergeSourceReader;
use databend_storages_common_index::OutputSlice;
use databend_storages_common_index::OutputWriteContext;
use databend_storages_common_index::OutputWriter;
use databend_storages_common_index::RowMapping;
use databend_storages_common_index::TermWindow;
use databend_storages_common_index::checkpoint_bytes_for;
use databend_storages_common_index::merge_window;
use databend_storages_common_index::open_merge_source;
use databend_storages_common_index::plan_index_windows;
use databend_storages_common_index::scan_source_costs;
use databend_storages_common_table_meta::meta::BlockMeta;
use tantivy::IndexSettings;
use tantivy::schema::Schema;

use super::block_index::BlockIndexMergeContext;
use super::block_index::BlockIndexMergeSource;
use super::block_index::BlockIndexMergeWriter;
use super::block_index::BlockIndexMerger;
use super::block_index::BlockIndexSlice;
use super::block_index::WrittenInvertedIndex;
use super::inverted_index_writer::InvertedIndexBuilder;
use super::inverted_index_writer::create_index_schema;

/// The bundles of `builder`'s index in every source, if each source has a mergeable one.
pub(super) fn merge_sources(
    builder: &InvertedIndexBuilder,
    inputs: &[BlockIndexMergeSource<'_>],
) -> Option<Vec<MergeSource>> {
    if inputs.is_empty() {
        return None;
    }
    let mut sources = Vec::with_capacity(inputs.len());
    for input in inputs {
        let mut matched = None;
        for meta in input.indexes {
            if meta.index_name == builder.name
                && meta.index_version == builder.version
                && meta.location.1 == INVERTED_INDEX_FILE_FORMAT_VERSION
                && meta.size > 0
            {
                matched = Some(meta);
                break;
            }
        }
        let meta = matched?;
        sources.push(MergeSource {
            location: meta.location.0.clone(),
            bundle_size: meta.size,
            num_rows: input.num_rows,
        });
    }
    Some(sources)
}

/// Merge of one inverted index: a partition is a [`TermWindow`], a slice an [`OutputSlice`].
pub(super) struct InvertedIndexMerger {
    name: String,
    version: String,
    schema: Schema,
    settings: IndexSettings,
    context: BlockIndexMergeContext,
    check_interrupt: MergeInterruptCheck,
    sources: Vec<MergeSourceReader>,
    windows: Vec<TermWindow>,
}

impl InvertedIndexMerger {
    /// Opens every source and cuts the term space into windows.
    pub(super) fn try_create(
        builder: &InvertedIndexBuilder,
        inputs: &[BlockIndexMergeSource<'_>],
        context: &BlockIndexMergeContext,
    ) -> Result<Arc<dyn BlockIndexMerger>> {
        let Some(sources) = merge_sources(builder, inputs) else {
            return Err(ErrorCode::Internal(format!(
                "inverted index {} cannot be merged from these sources",
                builder.name
            )));
        };
        let (schema, _) = create_index_schema(Arc::new(builder.schema.clone()), &builder.options)?;
        let abort_checker = context.abort_checker.clone();
        let check_interrupt: MergeInterruptCheck = Arc::new(move || -> io::Result<()> {
            match abort_checker.try_check_aborting() {
                Ok(()) => Ok(()),
                Err(error) => Err(io::Error::other(error.to_string())),
            }
        });

        let start = Instant::now();
        let checkpoint_bytes = checkpoint_bytes_for(context.partition_bytes);
        let mut readers = Vec::with_capacity(sources.len());
        let mut tables = Vec::with_capacity(sources.len());
        for source in &sources {
            context.abort_checker.try_check_aborting()?;
            let reader = open_merge_source(context.operator.clone(), source, Some(&schema))
                .map_err(|error| {
                    ErrorCode::StorageOther(format!(
                        "open inverted index source {} failed: {error}",
                        source.location
                    ))
                })?;
            let costs = scan_source_costs(&reader, &schema, checkpoint_bytes)
                .map_err(|error| ErrorCode::StorageOther(error.to_string()))?;
            readers.push(reader);
            tables.push(costs);
        }
        let windows = plan_index_windows(&schema, &tables, context.partition_bytes, 0, 0);
        let settings = readers[0].index.settings().clone();
        log::info!(
            "inverted index merge of {} opened {} sources and cut {} partitions in {:?}",
            builder.name,
            readers.len(),
            windows.len(),
            start.elapsed()
        );
        Ok(Arc::new(Self {
            name: builder.name.clone(),
            version: builder.version.clone(),
            schema,
            settings,
            context: context.clone(),
            check_interrupt,
            sources: readers,
            windows,
        }))
    }
}

impl BlockIndexMerger for InvertedIndexMerger {
    fn num_partitions(&self) -> usize {
        self.windows.len()
    }

    fn partition(&self, mapping: &RowMapping, partition: usize) -> Result<Vec<BlockIndexSlice>> {
        let Some(window) = self.windows.get(partition) else {
            return Err(ErrorCode::Internal(format!(
                "inverted index {} has no partition {partition}",
                self.name
            )));
        };
        let slices = merge_window(
            &self.sources,
            mapping,
            window,
            self.check_interrupt.as_ref(),
        )
        .map_err(|error| {
            ErrorCode::StorageOther(format!(
                "read partition {partition} of inverted index {} failed: {error}",
                self.name
            ))
        })?;
        let mut result = Vec::with_capacity(slices.len());
        for slice in slices {
            result.push(BlockIndexSlice::new(slice.output, slice));
        }
        Ok(result)
    }

    fn merge_writer(
        &self,
        mapping: Arc<RowMapping>,
        output: usize,
    ) -> Result<Box<dyn BlockIndexMergeWriter>> {
        let location = self
            .context
            .locations
            .gen_inverted_index_v2_location(&self.version);
        let writer = OutputWriter::new(OutputWriteContext::new(
            self.context.operator.clone(),
            location.clone(),
            self.schema.clone(),
            self.settings.clone(),
            self.sources.clone(),
            mapping,
            output,
            self.check_interrupt.clone(),
        ));
        Ok(Box::new(InvertedIndexMergeWriter {
            name: self.name.clone(),
            version: self.version.clone(),
            location,
            writer,
            started: Instant::now(),
        }))
    }
}

/// One output of one inverted index while its slices arrive.
struct InvertedIndexMergeWriter {
    name: String,
    version: String,
    location: String,
    writer: OutputWriter,
    started: Instant,
}

impl InvertedIndexMergeWriter {
    fn write_error(&self, error: impl std::fmt::Display) -> ErrorCode {
        ErrorCode::StorageOther(format!(
            "write merged inverted index {} to {} failed: {error}",
            self.name, self.location
        ))
    }
}

impl BlockIndexMergeWriter for InvertedIndexMergeWriter {
    fn append(&mut self, slice: BlockIndexSlice) -> Result<()> {
        let slice: OutputSlice = slice.downcast()?;
        self.writer.push(slice);
        if let Poll::Ready(Err(error)) = self.writer.poll() {
            return Err(self.write_error(error));
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>, block: &mut BlockMeta) -> Result<()> {
        self.writer.close();
        match self.writer.poll() {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Err(self.write_error(error)),
            Poll::Pending => {
                return Err(ErrorCode::Internal(
                    "merged inverted index writer did not finish after close",
                ));
            }
        }
        let sizes = match self.writer.into_result() {
            Ok(sizes) => sizes,
            Err(error) => {
                return Err(ErrorCode::StorageOther(format!(
                    "write merged inverted index {} to {} failed: {error}",
                    self.name, self.location
                )));
            }
        };
        let Some(total_size) = sizes.bundle.checked_add(sizes.siblings) else {
            return Err(ErrorCode::Internal("inverted index total size overflow"));
        };
        let elapsed_ms = self.started.elapsed().as_millis() as u64;
        metrics_inc_block_inverted_index_generate_milliseconds(elapsed_ms);
        metrics_inc_block_inverted_index_write_milliseconds(elapsed_ms);
        metrics_inc_block_inverted_index_write_nums(1);
        metrics_inc_block_inverted_index_write_bytes(total_size);
        let written = WrittenInvertedIndex {
            index_name: self.name,
            index_version: self.version,
            location: (self.location, INVERTED_INDEX_FILE_FORMAT_VERSION),
            bundle_size: sizes.bundle,
            total_size,
        };
        apply_written(block, written)
    }
}

/// Records a merged inverted index in the output's block metadata.
fn apply_written(block: &mut BlockMeta, written: WrittenInvertedIndex) -> Result<()> {
    let metas = block.inverted_index_metas.get_or_insert_with(Vec::new);
    for meta in metas.iter() {
        if meta.index_name == written.index_name {
            return Err(ErrorCode::Internal("duplicate merged inverted index"));
        }
    }
    let current_size = block.inverted_index_size.unwrap_or(0);
    let Some(total_size) = current_size.checked_add(written.total_size) else {
        return Err(ErrorCode::Internal("inverted index size overflow"));
    };
    block.inverted_index_size = Some(total_size);
    metas.push(written.to_block_index_meta());
    metas.sort_unstable_by(|a, b| a.index_name.cmp(&b.index_name));
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;
    use databend_common_expression::types::DataType;
    use databend_storages_common_table_meta::meta::BlockIndexMeta;

    use super::*;
    use crate::io::block_index::BlockIndexSpec;

    #[test]
    fn test_inverted_spec_reused_by_full_and_low_level_writers() -> Result<()> {
        use databend_common_expression::DataBlock;
        use databend_common_expression::FromData;
        use databend_common_expression::TableDataType;
        use databend_common_expression::TableField;
        use databend_common_expression::TableSchema;
        use databend_common_expression::types::StringType;
        use databend_storages_common_index::MergeSourceDirectory;
        use opendal::Operator;
        use opendal::services::Memory;

        use crate::io::TableMetaLocationGenerator;
        use crate::io::WriteSettings;
        use crate::io::block_index::BlockIndexWriteContext;

        crate::test_utils::init_test_globals()?;
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "content",
            TableDataType::String,
        )]));
        let builder = InvertedIndexBuilder {
            name: "text_idx".into(),
            version: "v1".into(),
            schema: DataSchema::new(vec![DataField::new("content", DataType::String)]),
            options: BTreeMap::new(),
            user_dictionary: None,
        };
        let spec = builder.into_write_spec();
        let context = BlockIndexWriteContext {
            func_ctx: Default::default(),
            physical_schema: schema,
            block_location: ("data.parquet".into(), 0),
            meta_locations: TableMetaLocationGenerator::new("reuse".into()),
            bloom_location: ("bloom.parquet".into(), 0),
            operator: Operator::new(Memory::default()).unwrap().finish(),
            write_settings: WriteSettings::default(),
        };
        let column = StringType::from_data(vec!["alpha", "beta"]);
        let mut writer = spec.new_writer(context.clone())?;
        writer.write(&DataBlock::new_from_columns(vec![column.clone()]))?;
        let full = writer.finish()?;
        let writer = spec.new_low_level_writer(context.clone())?;
        let mut field = writer.next_column()?;
        field.write(&column)?;
        let low = field.finish()?.finish()?;
        assert_ne!(full.inverted[0].location, low.inverted[0].location);
        for output in [full.inverted, low.inverted] {
            let meta = &output[0];
            let directory = MergeSourceDirectory::open(
                context.operator.clone(),
                meta.location.0.clone(),
                meta.bundle_size,
            )
            .unwrap();
            let index = directory.open_index().unwrap();
            let reader = index.reader().unwrap();
            assert_eq!(reader.searcher().num_docs(), 2);
        }
        Ok(())
    }

    #[test]
    fn test_inverted_spec_merge_admission() -> Result<()> {
        let builder = InvertedIndexBuilder {
            name: "text_idx".into(),
            version: "v1".into(),
            schema: DataSchema::new(vec![DataField::new("content", DataType::String)]),
            options: BTreeMap::new(),
            user_dictionary: None,
        };
        let spec: Box<dyn BlockIndexSpec> = Box::new(builder.into_write_spec());
        assert_eq!(spec.index_name(), Some("text_idx"));
        assert!(!spec.can_merge(&[])?);
        let mut metas = vec![BlockIndexMeta {
            index_name: "text_idx".into(),
            index_version: "v1".into(),
            location: ("source.index".into(), INVERTED_INDEX_FILE_FORMAT_VERSION),
            size: 128,
        }];
        let can_merge = |metas: &[BlockIndexMeta]| {
            spec.can_merge(&[
                BlockIndexMergeSource {
                    num_rows: 10,
                    indexes: metas,
                },
                BlockIndexMergeSource {
                    num_rows: 20,
                    indexes: metas,
                },
            ])
        };
        assert!(can_merge(&metas)?);
        assert!(!can_merge(&[])?);
        metas[0].index_version = "old".into();
        assert!(!can_merge(&metas)?);
        metas[0].index_version = "v1".into();
        metas[0].location.1 = INVERTED_INDEX_FILE_FORMAT_VERSION + 1;
        assert!(!can_merge(&metas)?);
        metas[0].location.1 = INVERTED_INDEX_FILE_FORMAT_VERSION;
        metas[0].size = 0;
        assert!(!can_merge(&metas)?);
        Ok(())
    }
}
