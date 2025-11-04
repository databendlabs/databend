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

use std::ops::Range;
use std::sync::Arc;

use arrow_schema::ArrowError;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_metrics::storage::metrics_inc_omit_filter_rowgroups;
use databend_common_metrics::storage::metrics_inc_omit_filter_rows;
use databend_common_storage::OperatorRegistry;
use futures::future::BoxFuture;
use futures::StreamExt;
use futures::TryFutureExt;
use opendal::Reader;
use parquet::arrow::arrow_reader::ArrowPredicateFn;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::MetadataLoader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescPtr;

use crate::meta::check_parquet_schema;
use crate::parquet_reader::predicate::ParquetPredicate;
use crate::parquet_reader::utils::transform_record_batch;
use crate::parquet_reader::utils::transform_record_batch_by_field_paths;
use crate::parquet_reader::utils::FieldPaths;
use crate::parquet_reader::DataBlockIterator;
use crate::transformer::RecordBatchTransformer;
use crate::ParquetPruner;

/// The reader to read a whole parquet file.
pub struct ParquetWholeFileReader {
    pub(super) op_registry: Arc<dyn OperatorRegistry>,
    pub(super) expect_file_schema: Option<(SchemaDescPtr, String)>,
    pub(super) output_schema: TableSchemaRef,
    pub(super) predicate: Option<Arc<ParquetPredicate>>,

    /// Columns to output.
    pub(super) projection: ProjectionMask,
    /// Field paths helping to traverse columns.
    ///
    /// If we use [`ProjectionMask`] to get inner columns of a struct,
    /// the columns will be contained in a struct array in the read [`RecordBatch`].
    ///
    /// Therefore, if `field_paths` is [Some],
    /// we should extract inner columns from the struct manually by traversing the nested column;
    /// if `field_paths` is [None], we can skip the traversing.
    pub(super) field_paths: Arc<Option<FieldPaths>>,

    pub(super) pruner: Option<ParquetPruner>,

    pub(super) transformer: Option<RecordBatchTransformer>,

    // Options
    pub(super) need_page_index: bool,
    pub(super) batch_size: usize,
}

impl ParquetWholeFileReader {
    // partition_fields is only used for delta table engine.
    pub async fn prepare_data_stream(
        &self,
        loc: &str,
        size: u64,
        partition_fields: Option<&[(TableField, Scalar)]>,
    ) -> Result<ParquetRecordBatchStream<ParquetFileReader>> {
        let partition_values_map = partition_fields.map(|arr| {
            arr.iter()
                .map(|(f, v)| (f.name().to_string(), v.clone()))
                .collect::<std::collections::HashMap<String, Scalar>>()
        });
        let (op, path) = self.op_registry.get_operator_path(loc)?;
        let reader: Reader = op.reader(path).await?;
        let reader = ParquetFileReader::new(reader, size);
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(
            reader,
            ArrowReaderOptions::new().with_page_index(self.need_page_index),
        )
        .await?
        .with_projection(self.projection.clone())
        .with_batch_size(self.batch_size);

        let mut all_pruned = false;

        let file_meta = builder.metadata().clone();

        // Prune row groups.
        if let Some(pruner) = &self.pruner {
            let (selected_row_groups, omits, _) =
                pruner.prune_row_groups(&file_meta, None, partition_values_map.as_ref())?;
            all_pruned = omits.iter().all(|x| *x);
            builder = builder.with_row_groups(selected_row_groups.clone());

            if !all_pruned {
                let row_selection = pruner.prune_pages(
                    &file_meta,
                    &selected_row_groups,
                    partition_values_map.as_ref(),
                )?;

                if let Some(row_selection) = row_selection {
                    builder = builder.with_row_selection(row_selection);
                }
            } else {
                metrics_inc_omit_filter_rowgroups(file_meta.num_row_groups() as u64);
                metrics_inc_omit_filter_rows(file_meta.file_metadata().num_rows() as u64);
            }
        }

        if !all_pruned {
            if let Some(predicate) = self.predicate.as_ref() {
                let projection = predicate.projection().clone();
                let predicate = predicate.clone();
                let partition_block_scalars = partition_fields
                    .map(|arr| {
                        Arc::<[_]>::from_iter(
                            arr.iter()
                                .map(|(f, v)| (DataType::from(f.data_type()), v.clone())),
                        )
                    })
                    .unwrap_or_default();
                let predicate_fn = move |batch| {
                    predicate
                        .evaluate(&batch, partition_block_scalars.clone())
                        .map_err(|e| ArrowError::from_external_error(Box::new(e)))
                };
                builder = builder.with_row_filter(RowFilter::new(vec![Box::new(
                    ArrowPredicateFn::new(projection, predicate_fn),
                )]));
            }
        }

        Ok(builder.build()?)
    }

    /// Read a [`DataBlock`] from parquet file using native apache arrow-rs stream API.
    pub async fn read_block_from_stream(
        &self,
        stream: &mut ParquetRecordBatchStream<ParquetFileReader>,
    ) -> Result<Option<DataBlock>> {
        let record_batch = stream.next().await.transpose()?;

        if let Some(batch) = record_batch {
            let blocks = transform_record_batch(
                &self.output_schema.as_ref().into(),
                &batch,
                &self.field_paths,
            )?;
            Ok(Some(blocks))
        } else {
            Ok(None)
        }
    }

    /// Read a [`DataBlock`] from bytes.
    pub fn read_blocks_from_binary(&self, bytes: Bytes, path: &str) -> Result<DataBlockIterator> {
        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, ArrowReaderOptions::new())
                .map_err(|e| format!("Invalid Parquet file '{path}': {e}"))?
                .with_projection(self.projection.clone())
                .with_batch_size(self.batch_size);

        // Prune row groups.
        let file_meta = builder.metadata().clone();
        if let Some((expect_schema, expect_schema_from)) = &self.expect_file_schema {
            check_parquet_schema(
                expect_schema,
                file_meta.file_metadata().schema_descr(),
                path,
                expect_schema_from,
            )?;
        }

        let mut full_match = false;
        if let Some(pruner) = &self.pruner {
            let (selected_row_groups, omits, _) =
                pruner.prune_row_groups(&file_meta, None, None)?;

            full_match = omits.iter().all(|x| *x);
            builder = builder.with_row_groups(selected_row_groups.clone());

            if !full_match {
                let row_selection = pruner.prune_pages(&file_meta, &selected_row_groups, None)?;

                if let Some(row_selection) = row_selection {
                    builder = builder.with_row_selection(row_selection);
                }
            } else {
                metrics_inc_omit_filter_rowgroups(file_meta.num_row_groups() as u64);
                metrics_inc_omit_filter_rows(file_meta.file_metadata().num_rows() as u64);
            }
        }

        if !full_match {
            if let Some(predicate) = self.predicate.as_ref() {
                let projection = predicate.projection().clone();
                let predicate = predicate.clone();
                let predicate_fn = move |batch| {
                    predicate
                        .evaluate(&batch, Default::default())
                        .map_err(|e| ArrowError::from_external_error(Box::new(e)))
                };
                builder = builder.with_row_filter(RowFilter::new(vec![Box::new(
                    ArrowPredicateFn::new(projection, predicate_fn),
                )]));
            }
        }
        let reader = builder.build()?;
        let iter = reader.into_iter().map(|r| r.map_err(ErrorCode::from));
        let output_data_schema: DataSchema = self.output_schema.as_ref().into();
        let output_data_schema = Arc::new(output_data_schema);
        let field_paths = self.field_paths.clone();
        let mut transformer = self.transformer.clone();

        if let Some(field_paths) = field_paths.as_ref() {
            let field_paths = field_paths.clone();
            let iter = iter.map(move |r| {
                r.and_then(|mut batch| {
                    if let Some(transformer) = &mut transformer {
                        batch = transformer.process_record_batch(batch)?;
                    }
                    transform_record_batch_by_field_paths(&batch, &field_paths)
                })
            });
            Ok(Box::new(iter))
        } else {
            Ok(Box::new(iter.map(move |r| {
                r.and_then(|mut batch| {
                    if let Some(transformer) = &mut transformer {
                        batch = transformer.process_record_batch(batch)?
                    }
                    DataBlock::from_record_batch(&output_data_schema, &batch)
                })
            })))
        }
    }
}

/// ParquetFileReader is a wrapper around a Reader that impls parquet AsyncFileReader.
///
/// # TODO
///
/// [ParquetObjectReader](https://docs.rs/parquet/latest/src/parquet/arrow/async_reader/store.rs.html#64) contains the following hints to speed up metadata loading, we can consider adding them to this struct:
///
/// - `metadata_size_hint`: Provide a hint as to the size of the parquet file's footer.
/// - `preload_column_index`: Load the Column Index  as part of [`Self::get_metadata`].
/// - `preload_offset_index`: Load the Offset Index as part of [`Self::get_metadata`].
pub struct ParquetFileReader {
    r: Reader,
    size: u64,
}

impl ParquetFileReader {
    /// Create a new ParquetFileReader
    pub fn new(r: Reader, size: u64) -> Self {
        Self { r, size }
    }
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(
            self.r
                .read(range.start..range.end)
                .map_ok(|v| v.to_bytes())
                .map_err(|err| parquet::errors::ParquetError::External(Box::new(err))),
        )
    }

    fn get_metadata(
        &mut self,
        _options: Option<&'_ ArrowReaderOptions>,
    ) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            let size = self.size as usize;
            #[allow(deprecated)]
            let mut loader = MetadataLoader::load(self, size, None).await?;
            #[allow(deprecated)]
            loader.load_page_index(false, false).await?;
            Ok(Arc::new(loader.finish()))
        })
    }
}
