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

use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_generate_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_storages_common_index::INVERTED_INDEX_FILE_FORMAT_VERSION;
use databend_storages_common_index::InvertedIndexMerger;
use databend_storages_common_index::MergeOutput;
use databend_storages_common_index::MergeSource;
use databend_storages_common_index::SourceRows;
use databend_storages_common_table_meta::meta::BlockMeta;
use tantivy::schema::Schema;

use super::block_index::BlockIndexMerge;
use super::block_index::BlockIndexMergeContext;
use super::block_index::BlockIndexMergeSource;
use super::block_index::WrittenBlockIndexOutput;
use super::block_index::WrittenInvertedIndex;
use super::inverted_index_writer::InvertedIndexBuilder;
use super::inverted_index_writer::create_index_schema;

pub(super) struct InvertedIndexMerge {
    name: String,
    version: String,
    sources: Vec<MergeSource>,
    schema: Schema,
}

impl InvertedIndexMerge {
    pub(super) fn try_create(
        builder: &InvertedIndexBuilder,
        inputs: &[BlockIndexMergeSource<'_>],
    ) -> Result<Option<Arc<dyn BlockIndexMerge>>> {
        if inputs.is_empty() {
            return Ok(None);
        }
        let mut sources = Vec::with_capacity(inputs.len());
        for input in inputs {
            let Some(meta) = input.indexes.iter().find(|meta| {
                meta.index_name == builder.name
                    && meta.index_version == builder.version
                    && meta.location.1 == INVERTED_INDEX_FILE_FORMAT_VERSION
                    && meta.size > 0
            }) else {
                return Ok(None);
            };
            sources.push(MergeSource {
                location: meta.location.0.clone(),
                bundle_size: meta.size,
                num_rows: input.num_rows,
            });
        }
        let (schema, _) = create_index_schema(Arc::new(builder.schema.clone()), &builder.options)?;
        Ok(Some(Arc::new(Self {
            name: builder.name.clone(),
            version: builder.version.clone(),
            sources,
            schema,
        })))
    }
}

impl BlockIndexMerge for InvertedIndexMerge {
    fn index_name(&self) -> &str {
        &self.name
    }

    fn merge(&self, context: BlockIndexMergeContext) -> Result<Vec<WrittenBlockIndexOutput>> {
        let start = Instant::now();
        let mut locations = Vec::with_capacity(context.outputs.len());
        let mut outputs = Vec::with_capacity(context.outputs.len());
        for rows in context.outputs {
            let location = context
                .locations
                .gen_inverted_index_v2_location(&self.version);
            locations.push((location.clone(), INVERTED_INDEX_FILE_FORMAT_VERSION));
            let rows = rows
                .into_iter()
                .map(|origin| SourceRows {
                    source: origin.source,
                    rows: origin.rows,
                })
                .collect();
            outputs.push(MergeOutput { location, rows });
        }
        let sources = self
            .sources
            .iter()
            .map(|source| MergeSource {
                location: source.location.clone(),
                bundle_size: source.bundle_size,
                num_rows: source.num_rows,
            })
            .collect();
        let merger = InvertedIndexMerger::try_create_for_recluster_batch(
            context.operator,
            sources,
            outputs,
            &self.schema,
            context.check_interrupt,
        )
        .map_err(|err| {
            ErrorCode::StorageOther(format!("open recluster inverted index merge: {err}"))
        })?;
        let sizes = merger.finish().map_err(|err| {
            ErrorCode::StorageOther(format!("finish recluster inverted index merge: {err}"))
        })?;
        let elapsed_ms = start.elapsed().as_millis() as u64;
        metrics_inc_block_inverted_index_generate_milliseconds(elapsed_ms);
        metrics_inc_block_inverted_index_write_milliseconds(elapsed_ms);
        if sizes.len() != locations.len() {
            return Err(ErrorCode::Internal("index merger output count mismatch"));
        }
        let mut outputs = Vec::with_capacity(sizes.len());
        for (location, sizes) in locations.into_iter().zip(sizes) {
            let total_size = sizes
                .bundle
                .checked_add(sizes.siblings)
                .ok_or_else(|| ErrorCode::Internal("inverted index total size overflow"))?;
            metrics_inc_block_inverted_index_write_nums(1);
            metrics_inc_block_inverted_index_write_bytes(total_size);
            outputs.push(WrittenBlockIndexOutput {
                inverted: vec![WrittenInvertedIndex {
                    index_name: self.name.clone(),
                    index_version: self.version.clone(),
                    location,
                    bundle_size: sizes.bundle,
                    total_size,
                }],
                ..Default::default()
            });
        }
        Ok(outputs)
    }

    fn apply_output(&self, block: &mut BlockMeta, output: WrittenBlockIndexOutput) -> Result<()> {
        if output.bloom.is_some()
            || output.vector.is_some()
            || output.spatial.is_some()
            || output.inverted.len() != 1
        {
            return Err(ErrorCode::Internal(
                "unexpected inverted index merge output",
            ));
        }
        let written = output.inverted.into_iter().next().unwrap();
        if written.index_name != self.name || written.index_version != self.version {
            return Err(ErrorCode::Internal(
                "inverted index merge output definition mismatch",
            ));
        }
        let metas = block.inverted_index_metas.get_or_insert_with(Vec::new);
        if metas.iter().any(|meta| meta.index_name == self.name) {
            return Err(ErrorCode::Internal("duplicate merged inverted index"));
        }
        let current_size = block.inverted_index_size.unwrap_or(0);
        let total_size = current_size
            .checked_add(written.total_size)
            .ok_or_else(|| ErrorCode::Internal("inverted index size overflow"))?;
        block.inverted_index_size = Some(total_size);
        metas.push(written.to_block_index_meta());
        metas.sort_unstable_by(|a, b| a.index_name.cmp(&b.index_name));
        Ok(())
    }
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
    fn test_inverted_spec_merge_capability_admission() -> Result<()> {
        let builder = InvertedIndexBuilder {
            name: "text_idx".into(),
            version: "v1".into(),
            schema: DataSchema::new(vec![DataField::new("content", DataType::String)]),
            options: BTreeMap::new(),
        };
        let spec: Box<dyn BlockIndexSpec> = Box::new(builder.into_write_spec());
        assert!(spec.prepare_merge(&[])?.is_none());
        let mut metas = vec![BlockIndexMeta {
            index_name: "text_idx".into(),
            index_version: "v1".into(),
            location: ("source.index".into(), INVERTED_INDEX_FILE_FORMAT_VERSION),
            size: 128,
        }];
        let prepare = |metas: &[BlockIndexMeta]| {
            spec.prepare_merge(&[
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
        let merge = prepare(&metas)?.unwrap();
        assert_eq!(merge.index_name(), "text_idx");
        assert!(prepare(&[])?.is_none());
        metas[0].index_version = "old".into();
        assert!(prepare(&metas)?.is_none());
        metas[0].index_version = "v1".into();
        metas[0].location.1 = INVERTED_INDEX_FILE_FORMAT_VERSION + 1;
        assert!(prepare(&metas)?.is_none());
        metas[0].location.1 = INVERTED_INDEX_FILE_FORMAT_VERSION;
        metas[0].size = 0;
        assert!(prepare(&metas)?.is_none());
        Ok(())
    }
}
