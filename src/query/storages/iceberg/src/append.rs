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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_storage::MutationStatus;
use databend_storages_common_cache::LoadParams;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::DataFileFormat;
use iceberg::spec::Literal;
use iceberg::spec::PartitionKey;
use iceberg::spec::Struct as IcebergStruct;
use iceberg::transaction::ApplyTransactionAction;
use iceberg::transaction::Transaction;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::DefaultFileNameGenerator;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use parquet::file::properties::WriterProperties;

use crate::IcebergTable;
use crate::cache;

/// Metadata for iceberg data files that flows through the pipeline.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct IcebergDataFileMeta {
    /// Serialized data files as JSON strings.
    /// We use JSON serialization because DataFile doesn't implement Clone/Serialize directly
    /// in a way that's compatible with BlockMetaInfo.
    pub data_files_json: Vec<String>,
}

#[typetag::serde(name = "iceberg_data_file_meta")]
impl BlockMetaInfo for IcebergDataFileMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        IcebergDataFileMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

/// Transform that writes data blocks to iceberg parquet files.
/// Accumulates all blocks and outputs data file metadata when finished.
pub struct IcebergDataFileWriter {
    ctx: Arc<dyn TableContext>,
    iceberg_table: iceberg::table::Table,
    data_files_json: Vec<String>,
}

impl IcebergDataFileWriter {
    pub fn create(ctx: Arc<dyn TableContext>, table: &IcebergTable) -> Self {
        Self {
            ctx,
            iceberg_table: table.iceberg_table().clone(),
            data_files_json: Vec::new(),
        }
    }

    async fn write_block(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let table = &self.iceberg_table;
        let metadata = table.metadata();
        let iceberg_schema = metadata.current_schema();
        let partition_spec = metadata.default_partition_spec();

        // Check if the table is partitioned
        let is_partitioned = !partition_spec.fields().is_empty();

        let location_generator = DefaultLocationGenerator::new(metadata.clone()).map_err(|e| {
            ErrorCode::Internal(format!("Failed to create location generator: {e:?}"))
        })?;

        // Use UUID suffix to ensure unique file names across different write operations
        let uuid = uuid::Uuid::new_v4().to_string();
        let file_name_generator =
            DefaultFileNameGenerator::new("data".to_string(), Some(uuid), DataFileFormat::Parquet);

        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema.clone());

        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        let arrow_schema = schema_to_arrow_schema(iceberg_schema.as_ref()).map_err(|e| {
            ErrorCode::Internal(format!("Failed to convert iceberg schema to arrow: {e:?}"))
        })?;

        let files = if is_partitioned {
            self.write_partitioned_block(
                block,
                Arc::new(arrow_schema),
                data_file_writer_builder,
                partition_spec.clone(),
                iceberg_schema.clone(),
            )
            .await?
        } else {
            self.write_unpartitioned_block(block, Arc::new(arrow_schema), data_file_writer_builder)
                .await?
        };

        // Track write progress
        let rows: usize = files.iter().map(|f| f.record_count() as usize).sum();
        let bytes: usize = files.iter().map(|f| f.file_size_in_bytes() as usize).sum();
        self.ctx
            .get_write_progress()
            .incr(&ProgressValues { rows, bytes });

        let partition_type = metadata.default_partition_type().clone();
        let format_version = metadata.format_version();
        for file in files {
            let json =
                iceberg::spec::serialize_data_file_to_json(file, &partition_type, format_version)
                    .map_err(|e| {
                    ErrorCode::Internal(format!("Failed to serialize data file: {e:?}"))
                })?;
            self.data_files_json.push(json);
        }

        Ok(())
    }

    async fn write_unpartitioned_block<B>(
        &self,
        block: DataBlock,
        arrow_schema: Arc<ArrowSchema>,
        data_file_writer_builder: B,
    ) -> Result<Vec<iceberg::spec::DataFile>>
    where
        B: IcebergWriterBuilder<arrow_array::RecordBatch, Vec<iceberg::spec::DataFile>>
            + Send
            + Sync,
        B::R: Send,
    {
        let mut data_file_writer = data_file_writer_builder.build(None).await.map_err(|e| {
            ErrorCode::Internal(format!("Failed to create data file writer: {e:?}"))
        })?;

        let record_batch = self.build_record_batch_with_schema(block, arrow_schema)?;

        data_file_writer
            .write(record_batch)
            .await
            .map_err(|e| ErrorCode::Internal(format!("Failed to write record batch: {e:?}")))?;

        data_file_writer
            .close()
            .await
            .map_err(|e| ErrorCode::Internal(format!("Failed to close data file writer: {e:?}")))
    }

    async fn write_partitioned_block<B>(
        &self,
        block: DataBlock,
        arrow_schema: Arc<ArrowSchema>,
        data_file_writer_builder: B,
        partition_spec: Arc<iceberg::spec::PartitionSpec>,
        iceberg_schema: Arc<iceberg::spec::Schema>,
    ) -> Result<Vec<iceberg::spec::DataFile>>
    where
        B: IcebergWriterBuilder<arrow_array::RecordBatch, Vec<iceberg::spec::DataFile>>
            + Send
            + Sync,
        B::R: Send,
    {
        // Get partition column indices and field info
        let partition_fields = partition_spec.fields();
        let mut partition_column_indices = Vec::with_capacity(partition_fields.len());

        for field in partition_fields {
            // Find the source column index in the schema
            let source_id = field.source_id;
            let schema_field = iceberg_schema.field_by_id(source_id).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Partition source field {source_id} not found in schema"
                ))
            })?;

            // Find column index by name in our block
            let field_name = &schema_field.name;
            let col_idx = arrow_schema
                .fields()
                .iter()
                .position(|f| f.name() == field_name)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Partition column {field_name} not found in arrow schema"
                    ))
                })?;

            partition_column_indices.push((col_idx, field.clone()));
        }

        // Group rows by partition values
        let num_rows = block.num_rows();
        let mut partition_groups: HashMap<IcebergStruct, Vec<u32>> = HashMap::new();

        for row_idx in 0..num_rows {
            // Build partition value for this row
            let partition_value =
                self.extract_partition_value(&block, row_idx, &partition_column_indices)?;
            partition_groups
                .entry(partition_value)
                .or_default()
                .push(row_idx as u32);
        }

        // Create FanoutWriter
        let mut fanout_writer: FanoutWriter<
            B,
            arrow_array::RecordBatch,
            Vec<iceberg::spec::DataFile>,
        > = FanoutWriter::new(data_file_writer_builder);

        // Write each partition group
        for (partition_value, row_indices) in partition_groups {
            // Create partition key
            let partition_key = PartitionKey::new(
                (*partition_spec).clone(),
                iceberg_schema.clone(),
                partition_value,
            );

            // Create a filtered block with only rows for this partition
            let filtered_block = block.take(&row_indices)?;
            let record_batch =
                self.build_record_batch_with_schema(filtered_block, arrow_schema.clone())?;

            fanout_writer
                .write(partition_key, record_batch)
                .await
                .map_err(|e| {
                    ErrorCode::Internal(format!("Failed to write partitioned data: {e:?}"))
                })?;
        }

        fanout_writer
            .close()
            .await
            .map_err(|e| ErrorCode::Internal(format!("Failed to close fanout writer: {e:?}")))
    }

    fn extract_partition_value(
        &self,
        block: &DataBlock,
        row_idx: usize,
        partition_column_indices: &[(usize, iceberg::spec::PartitionField)],
    ) -> Result<IcebergStruct> {
        let mut literals: Vec<Option<Literal>> = Vec::with_capacity(partition_column_indices.len());

        for (col_idx, _field) in partition_column_indices {
            let entry = &block.columns()[*col_idx];
            let column = entry.to_column();
            let value = column.index(row_idx);

            // Convert Databend ScalarRef to Iceberg Literal
            // Note: We only support Identity transform for now
            let literal = self.scalar_to_iceberg_literal(value)?;
            literals.push(literal);
        }

        Ok(IcebergStruct::from_iter(literals))
    }

    fn scalar_to_iceberg_literal(&self, scalar: Option<ScalarRef>) -> Result<Option<Literal>> {
        match scalar {
            None => Ok(None),
            Some(s) => {
                let lit = match s {
                    ScalarRef::Null => return Ok(None),
                    ScalarRef::Boolean(v) => Literal::bool(v),
                    ScalarRef::Number(n) => match n {
                        databend_common_expression::types::NumberScalar::Int8(v) => {
                            Literal::int(v as i32)
                        }
                        databend_common_expression::types::NumberScalar::Int16(v) => {
                            Literal::int(v as i32)
                        }
                        databend_common_expression::types::NumberScalar::Int32(v) => {
                            Literal::int(v)
                        }
                        databend_common_expression::types::NumberScalar::Int64(v) => {
                            Literal::long(v)
                        }
                        databend_common_expression::types::NumberScalar::UInt8(v) => {
                            Literal::int(v as i32)
                        }
                        databend_common_expression::types::NumberScalar::UInt16(v) => {
                            Literal::int(v as i32)
                        }
                        databend_common_expression::types::NumberScalar::UInt32(v) => {
                            Literal::long(v as i64)
                        }
                        databend_common_expression::types::NumberScalar::UInt64(v) => {
                            Literal::long(v as i64)
                        }
                        databend_common_expression::types::NumberScalar::Float32(v) => {
                            Literal::float(v.0)
                        }
                        databend_common_expression::types::NumberScalar::Float64(v) => {
                            Literal::double(v.0)
                        }
                    },
                    ScalarRef::String(v) => Literal::string(v),
                    ScalarRef::Date(v) => Literal::date(v),
                    ScalarRef::Timestamp(v) => Literal::timestamp(v),
                    _ => {
                        return Err(ErrorCode::Unimplemented(format!(
                            "Unsupported partition column type: {:?}",
                            s
                        )));
                    }
                };
                Ok(Some(lit))
            }
        }
    }

    fn build_record_batch_with_schema(
        &self,
        block: DataBlock,
        arrow_schema: Arc<ArrowSchema>,
    ) -> Result<arrow_array::RecordBatch> {
        use arrow_array::RecordBatch;
        use arrow_array::cast::AsArray;
        use arrow_schema::DataType as ArrowDataType;

        let num_rows = block.num_rows();
        if arrow_schema.fields().is_empty() {
            return Ok(RecordBatch::try_new_with_options(
                arrow_schema,
                vec![],
                &arrow_array::RecordBatchOptions::default().with_row_count(Some(num_rows)),
            )?);
        }

        let mut arrays = Vec::with_capacity(block.columns().len());
        for (entry, field) in block.columns().iter().zip(arrow_schema.fields()) {
            let array = entry.to_column().maybe_gc().into_arrow_rs();

            let array = match (array.data_type(), field.data_type()) {
                (ArrowDataType::Utf8View, ArrowDataType::Utf8) => {
                    let string_view = array.as_string_view();
                    Arc::new(arrow_array::StringArray::from_iter(string_view.iter()))
                        as Arc<dyn arrow_array::Array>
                }
                (ArrowDataType::BinaryView, ArrowDataType::Binary) => {
                    let binary_view = array.as_binary_view();
                    Arc::new(arrow_array::BinaryArray::from_iter(binary_view.iter()))
                        as Arc<dyn arrow_array::Array>
                }
                _ => array,
            };

            arrays.push(array);
        }

        Ok(RecordBatch::try_new(arrow_schema, arrays)?)
    }
}

#[async_trait]
impl AsyncAccumulatingTransform for IcebergDataFileWriter {
    const NAME: &'static str = "IcebergDataFileWriter";

    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        self.write_block(data).await?;
        // Don't output anything during transform - we accumulate and output on finish
        Ok(None)
    }

    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        if self.data_files_json.is_empty() {
            return Ok(None);
        }

        // Output the accumulated data files as metadata
        let meta = IcebergDataFileMeta {
            data_files_json: std::mem::take(&mut self.data_files_json),
        };
        Ok(Some(DataBlock::empty_with_meta(Box::new(meta))))
    }
}

/// Sink that commits iceberg data files via the Transaction API.
pub struct IcebergCommitSink {
    ctx: Arc<dyn TableContext>,
    iceberg_table: iceberg::table::Table,
    catalog: Arc<dyn iceberg::Catalog>,
    catalog_info: Arc<CatalogInfo>,
    db_name: String,
    table_name: String,
    /// Accumulated data files JSON from all input blocks
    data_files_json: Vec<String>,
}

impl IcebergCommitSink {
    pub fn try_create(
        input: Arc<InputPort>,
        ctx: Arc<dyn TableContext>,
        table: &IcebergTable,
    ) -> Result<ProcessorPtr> {
        let catalog = table.catalog().clone();
        let table_info = table.get_table_info();
        let catalog_info = table_info.catalog_info.clone();
        // Extract db_name and table_name from the table identifier
        let ident = table.iceberg_table().identifier();
        let db_name = ident
            .namespace()
            .as_ref()
            .first()
            .cloned()
            .unwrap_or_default();
        let table_name = ident.name().to_string();

        Ok(ProcessorPtr::create(AsyncSinker::create(input, Self {
            ctx,
            iceberg_table: table.iceberg_table().clone(),
            catalog,
            catalog_info,
            db_name,
            table_name,
            data_files_json: Vec::new(),
        })))
    }
}

#[async_trait]
impl AsyncSink for IcebergCommitSink {
    const NAME: &'static str = "IcebergCommitSink";

    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        // Extract data file metadata from the block
        if let Some(meta) = data_block.get_meta() {
            if let Some(iceberg_meta) = IcebergDataFileMeta::downcast_ref_from(meta) {
                self.data_files_json
                    .extend(iceberg_meta.data_files_json.iter().cloned());
            }
        }
        Ok(false)
    }

    async fn on_finish(&mut self) -> Result<()> {
        if self.data_files_json.is_empty() {
            // Nothing to commit
            return Ok(());
        }

        let table = &self.iceberg_table;
        let metadata = table.metadata();
        let spec_id = metadata.default_partition_spec_id();
        let partition_type = metadata.default_partition_type().clone();
        let current_schema = metadata.current_schema().clone();

        // Deserialize data files from JSON
        let mut data_files = Vec::with_capacity(self.data_files_json.len());
        for json in &self.data_files_json {
            let file = iceberg::spec::deserialize_data_file_from_json(
                json,
                spec_id,
                &partition_type,
                &current_schema,
            )
            .map_err(|e| ErrorCode::Internal(format!("Failed to deserialize data file: {e:?}")))?;
            data_files.push(file);
        }

        // Sum up the record counts for mutation status
        let insert_rows: u64 = data_files.iter().map(|f| f.record_count()).sum();

        // Create a transaction and commit the data files
        let tx = Transaction::new(table);
        let action = tx.fast_append().add_data_files(data_files);

        // Apply the action and commit the transaction
        action
            .apply(tx)
            .map_err(|e| ErrorCode::Internal(format!("Failed to apply transaction: {e:?}")))?
            .commit(self.catalog.as_ref())
            .await
            .map_err(|e| ErrorCode::Internal(format!("Failed to commit transaction: {e:?}")))?;

        // Report mutation status
        self.ctx.add_mutation_status(MutationStatus {
            insert_rows,
            update_rows: 0,
            deleted_rows: 0,
        });

        // Invalidate cache after successful commit so that subsequent reads see the new data
        let cache_key = format!("{}{}{}", self.db_name, cache::SEP_STR, self.table_name);
        let params = LoadParams {
            location: cache_key,
            len_hint: None,
            ver: 0,
            put_cache: false,
        };

        let reader =
            cache::iceberg_table_cache_reader(self.catalog.clone(), self.catalog_info.clone());
        reader.remove(&params);

        Ok(())
    }
}
