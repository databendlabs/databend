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
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NumberScalar;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_storage::MutationStatus;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::DataFileFormat;
use iceberg::spec::Datum;
use iceberg::spec::Literal;
use iceberg::spec::PartitionKey;
use iceberg::spec::PrimitiveLiteral;
use iceberg::spec::PrimitiveType;
use iceberg::spec::Struct as IcebergStruct;
use iceberg::transaction::ApplyTransactionAction;
use iceberg::transaction::Transaction;
use iceberg::transform::BoxedTransformFunction;
use iceberg::transform::TransformFunction;
use iceberg::transform::create_transform_function;
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
        let mut partition_sources = Vec::with_capacity(partition_fields.len());

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

            let source_type = match schema_field.field_type.as_ref() {
                iceberg::spec::Type::Primitive(primitive) => primitive.clone(),
                _ => {
                    return Err(ErrorCode::TableEngineNotSupported(format!(
                        "Iceberg partition source column '{field_name}' is not a primitive type"
                    )));
                }
            };

            // The stored partition value is the transformed source value, which is not
            // necessarily the raw column value (bucket, truncate, temporal transforms).
            let transform = create_transform_function(&field.transform).map_err(|err| {
                ErrorCode::TableEngineNotSupported(format!(
                    "Iceberg partition transform '{}' on column '{field_name}' is not supported: {err}",
                    field.transform
                ))
            })?;

            partition_sources.push(PartitionSource {
                column_index: col_idx,
                transform,
                source_type,
            });
        }

        // Group rows by partition values
        let num_rows = block.num_rows();
        let mut partition_groups: HashMap<IcebergStruct, MutableBitmap> = HashMap::new();

        for row_idx in 0..num_rows {
            // Build partition value for this row
            let partition_value =
                self.extract_partition_value(&block, row_idx, &partition_sources)?;
            let bitmap = partition_groups.entry(partition_value).or_insert_with(|| {
                let mut bitmap = MutableBitmap::new();
                bitmap.extend_constant(num_rows, false);
                bitmap
            });
            bitmap.set(row_idx, true);
        }

        // Create FanoutWriter
        let mut fanout_writer: FanoutWriter<
            B,
            arrow_array::RecordBatch,
            Vec<iceberg::spec::DataFile>,
        > = FanoutWriter::new(data_file_writer_builder);

        // Write each partition group
        for (partition_value, mutable_bitmap) in partition_groups {
            // Create partition key
            let partition_key = PartitionKey::new(
                (*partition_spec).clone(),
                iceberg_schema.clone(),
                partition_value,
            );

            // Convert MutableBitmap to Bitmap and filter the block
            let bitmap: Bitmap = mutable_bitmap.into();
            let filtered_block = block.clone().filter_with_bitmap(&bitmap)?;
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
        partition_sources: &[PartitionSource],
    ) -> Result<IcebergStruct> {
        let mut literals: Vec<Option<Literal>> = Vec::with_capacity(partition_sources.len());

        for source in partition_sources {
            let entry = &block.columns()[source.column_index];
            let column = entry.to_column();
            let value = column.index(row_idx);
            literals.push(transform_partition_literal(
                value,
                &source.source_type,
                source.transform.as_ref(),
            )?);
        }

        Ok(IcebergStruct::from_iter(literals))
    }

    fn build_record_batch_with_schema(
        &self,
        block: DataBlock,
        arrow_schema: Arc<ArrowSchema>,
    ) -> Result<arrow_array::RecordBatch> {
        block_to_record_batch(block, arrow_schema)
    }
}

/// Convert a databend block into a record batch of the iceberg table schema.
fn block_to_record_batch(
    block: DataBlock,
    arrow_schema: Arc<ArrowSchema>,
) -> Result<arrow_array::RecordBatch> {
    use arrow_array::RecordBatch;

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

        // Databend stores view types as well as 64-bit decimals and large lists, while
        // the iceberg writer expects the physical types of the table schema.
        let array = if array.data_type() == field.data_type() {
            array
        } else {
            // A safe cast would turn a value that does not fit the target type into NULL, so a
            // write that cannot represent its input would silently store NULLs instead of
            // failing.
            let casted = arrow_cast::cast_with_options(
                &array,
                field.data_type(),
                &arrow_cast::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )
            .map_err(|err| {
                ErrorCode::TableSchemaMismatch(format!(
                    "Cannot cast column '{}' from {:?} to {:?} for the iceberg table: {err}",
                    field.name(),
                    array.data_type(),
                    field.data_type(),
                ))
            })?;
            debug_assert_eq!(casted.data_type(), field.data_type());
            casted
        };

        arrays.push(array);
    }

    Ok(RecordBatch::try_new(arrow_schema, arrays)?)
}

/// One partition field of the table's partition spec, bound to the block column it reads.
struct PartitionSource {
    column_index: usize,
    transform: BoxedTransformFunction,
    source_type: PrimitiveType,
}

/// Serialize a primitive value with iceberg's binary single-value serialization, the format
/// [`Datum::try_from_bytes`] parses.
fn iceberg_value_bytes(ty: &PrimitiveType, value: &PrimitiveLiteral) -> Result<Vec<u8>> {
    let bytes = match (ty, value) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(v)) => vec![u8::from(*v)],
        (PrimitiveType::Int | PrimitiveType::Date, PrimitiveLiteral::Int(v)) => {
            v.to_le_bytes().to_vec()
        }
        (
            PrimitiveType::Long
            | PrimitiveType::Time
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::TimestampNs
            | PrimitiveType::TimestamptzNs,
            PrimitiveLiteral::Long(v),
        ) => v.to_le_bytes().to_vec(),
        (PrimitiveType::Float, PrimitiveLiteral::Float(v)) => v.0.to_le_bytes().to_vec(),
        (PrimitiveType::Double, PrimitiveLiteral::Double(v)) => v.0.to_le_bytes().to_vec(),
        (PrimitiveType::String, PrimitiveLiteral::String(v)) => v.clone().into_bytes(),
        (PrimitiveType::Binary | PrimitiveType::Fixed(_), PrimitiveLiteral::Binary(v)) => v.clone(),
        (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(v)) => v.to_be_bytes().to_vec(),
        (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(v)) => v.to_be_bytes().to_vec(),
        (ty, value) => {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "Iceberg cannot use partition source value {value:?} as type {ty}"
            )));
        }
    };
    Ok(bytes)
}

/// Apply a partition transform to one source value.
///
/// The partition value stored in the metadata and used to build the file path is the
/// *transformed* value, not the raw column value.
fn transform_partition_literal(
    scalar: Option<ScalarRef>,
    source_type: &PrimitiveType,
    transform: &dyn TransformFunction,
) -> Result<Option<Literal>> {
    let Some(literal) = scalar_to_iceberg_literal(scalar)? else {
        return Ok(None);
    };
    let primitive = literal.as_primitive_literal().ok_or_else(|| {
        ErrorCode::TableEngineNotSupported(
            "Iceberg partition source value must be a primitive value".to_string(),
        )
    })?;
    // Iceberg transforms operate on `Datum`, which can only be built from the spec's binary
    // single-value serialization (or from another `Datum`) outside of the iceberg crate.
    let bytes = iceberg_value_bytes(source_type, &primitive)?;
    let datum = Datum::try_from_bytes(&bytes, source_type.clone()).map_err(|err| {
        ErrorCode::Internal(format!("Iceberg partition source value is invalid: {err}"))
    })?;
    let transformed = transform
        .transform_literal(&datum)
        .map_err(|err| ErrorCode::Internal(format!("Iceberg partition transform failed: {err}")))?;
    Ok(transformed.map(Literal::from))
}

fn scalar_to_iceberg_literal(scalar: Option<ScalarRef>) -> Result<Option<Literal>> {
    match scalar {
        None => Ok(None),
        Some(s) => {
            let lit = match s {
                ScalarRef::Null => return Ok(None),
                ScalarRef::Boolean(v) => Literal::bool(v),
                ScalarRef::Number(n) => match n {
                    NumberScalar::Int8(v) => Literal::int(v as i32),
                    NumberScalar::Int16(v) => Literal::int(v as i32),
                    NumberScalar::Int32(v) => Literal::int(v),
                    NumberScalar::Int64(v) => Literal::long(v),
                    NumberScalar::UInt8(v) => Literal::int(v as i32),
                    NumberScalar::UInt16(v) => Literal::int(v as i32),
                    NumberScalar::UInt32(v) => Literal::long(v as i64),
                    NumberScalar::UInt64(v) => Literal::decimal(v as i128),
                    NumberScalar::Float32(v) => Literal::float(v.0),
                    NumberScalar::Float64(v) => Literal::double(v.0),
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
        self.ctx
            .mutation_state()
            .add_mutation_status(MutationStatus {
                insert_rows,
                update_rows: 0,
                deleted_rows: 0,
            });

        // Invalidate cache after successful commit so that subsequent reads see the new data
        let cache_key = cache::table_cache_key(
            self.catalog_info.catalog_name(),
            &self.db_name,
            &self.table_name,
        );
        let reader =
            cache::iceberg_table_cache_reader(self.catalog.clone(), self.catalog_info.clone());
        reader.remove(&cache_key);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::Datum;
    use iceberg::spec::Transform;
    use iceberg::transform::create_transform_function;

    use super::*;

    fn transform_value(
        value: ScalarRef,
        source_type: PrimitiveType,
        transform: Transform,
    ) -> Option<Literal> {
        let transform = create_transform_function(&transform).unwrap();
        transform_partition_literal(Some(value), &source_type, transform.as_ref()).unwrap()
    }

    #[test]
    fn test_identity_partition_value_is_the_source_value() {
        let value = ScalarRef::Number(NumberScalar::Int64(7));
        assert_eq!(
            transform_value(value, PrimitiveType::Long, Transform::Identity),
            Some(Literal::long(7))
        );
    }

    #[test]
    fn test_bucket_partition_value_is_hashed() {
        // Same value as the iceberg bucket transform test: bucket(10) of 34 is 9.
        let value = ScalarRef::Number(NumberScalar::Int32(34));
        assert_eq!(
            transform_value(value, PrimitiveType::Int, Transform::Bucket(10)),
            Some(Literal::int(9))
        );
    }

    #[test]
    fn test_truncate_partition_value_is_rounded_down() {
        let value = ScalarRef::Number(NumberScalar::Int32(-1));
        assert_eq!(
            transform_value(value, PrimitiveType::Int, Transform::Truncate(10)),
            Some(Literal::int(-10))
        );
    }

    #[test]
    fn test_partition_value_of_null_stays_null() {
        assert_eq!(
            transform_value(ScalarRef::Null, PrimitiveType::Int, Transform::Bucket(10)),
            None
        );
    }

    /// Build a databend block the way a query produces it: decimal(10, 2) as Decimal64 and
    /// arrays as LargeList, which is not what the iceberg table schema asks for.
    fn decimal_and_array_block() -> (DataBlock, Arc<ArrowSchema>) {
        use arrow_array::Decimal64Array;
        use arrow_array::LargeListArray;
        use arrow_array::RecordBatch;
        use arrow_schema::DataType as ArrowDataType;
        use arrow_schema::Field;
        use arrow_schema::Fields;
        use arrow_schema::Schema;
        use databend_common_expression::DataSchema;

        let source_schema = Schema::new(Fields::from(vec![
            Field::new("dec", ArrowDataType::Decimal64(10, 2), true),
            Field::new(
                "arr",
                ArrowDataType::LargeList(Arc::new(Field::new("item", ArrowDataType::Int64, true))),
                true,
            ),
        ]));
        let decimals = Decimal64Array::from(vec![Some(1234_i64), Some(-5_i64)])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let arrays =
            LargeListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>(vec![
                Some(vec![Some(1), Some(2)]),
                Some(vec![Some(3)]),
            ]);
        let batch = RecordBatch::try_new(Arc::new(source_schema.clone()), vec![
            Arc::new(decimals),
            Arc::new(arrays),
        ])
        .unwrap();
        let data_schema = DataSchema::try_from(&source_schema).unwrap();
        let block = DataBlock::from_record_batch(&data_schema, &batch).unwrap();

        // What `database::convert_table_schema` produces for the same columns.
        let iceberg_schema = Schema::new(Fields::from(vec![
            Field::new("dec", ArrowDataType::Decimal128(10, 2), true),
            Field::new(
                "arr",
                ArrowDataType::List(Arc::new(Field::new("element", ArrowDataType::Int64, true))),
                true,
            ),
        ]));
        (block, Arc::new(iceberg_schema))
    }

    #[test]
    fn test_decimal_and_array_columns_are_written_as_the_iceberg_physical_types() {
        use arrow_array::cast::AsArray;

        let (block, iceberg_schema) = decimal_and_array_block();
        let batch = block_to_record_batch(block, iceberg_schema.clone()).unwrap();

        assert_eq!(batch.schema().as_ref(), iceberg_schema.as_ref());
        let decimals = batch
            .column(0)
            .as_primitive::<arrow_array::types::Decimal128Type>();
        assert_eq!(decimals.value(0), 1234_i128);
        assert_eq!(decimals.value(1), -5_i128);
        assert_eq!(batch.column(1).as_list::<i32>().value(0).len(), 2);
    }

    #[test]
    fn test_value_that_does_not_fit_the_iceberg_type_is_an_error() {
        use arrow_array::BinaryArray;
        use arrow_array::RecordBatch;
        use arrow_schema::DataType as ArrowDataType;
        use arrow_schema::Field;
        use arrow_schema::Fields;
        use arrow_schema::Schema;
        use databend_common_expression::DataSchema;

        let source_schema = Schema::new(Fields::from(vec![Field::new(
            "b",
            ArrowDataType::Binary,
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::new(source_schema.clone()), vec![Arc::new(
            BinaryArray::from(vec![Some(&[1u8, 2, 3][..])]),
        )])
        .unwrap();
        let data_schema = DataSchema::try_from(&source_schema).unwrap();
        let block = DataBlock::from_record_batch(&data_schema, &batch).unwrap();

        // Three bytes do not fit fixed(16). A safe cast would write the value as NULL.
        let iceberg_schema = Schema::new(Fields::from(vec![Field::new(
            "b",
            ArrowDataType::FixedSizeBinary(16),
            true,
        )]));
        let err = block_to_record_batch(block, Arc::new(iceberg_schema)).unwrap_err();
        assert!(err.message().contains("Cannot cast column 'b'"), "{err}");
    }

    #[test]
    fn test_timestamp_outside_the_nanosecond_range_is_an_error() {
        use arrow_array::RecordBatch;
        use arrow_array::TimestampMicrosecondArray;
        use arrow_schema::DataType as ArrowDataType;
        use arrow_schema::Field;
        use arrow_schema::Fields;
        use arrow_schema::Schema;
        use arrow_schema::TimeUnit;
        use databend_common_expression::DataSchema;

        // Year 3000 in microseconds, which is outside what timestamp[ns] can represent.
        let year_3000 = 32_503_680_000_000_000_i64;
        let source_schema = Schema::new(Fields::from(vec![Field::new(
            "ts",
            ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::new(source_schema.clone()), vec![Arc::new(
            TimestampMicrosecondArray::from(vec![year_3000]),
        )])
        .unwrap();
        let data_schema = DataSchema::try_from(&source_schema).unwrap();
        let block = DataBlock::from_record_batch(&data_schema, &batch).unwrap();

        let iceberg_schema = Schema::new(Fields::from(vec![Field::new(
            "ts",
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let err = block_to_record_batch(block, Arc::new(iceberg_schema)).unwrap_err();
        assert!(err.message().contains("Cannot cast column 'ts'"), "{err}");
    }

    #[test]
    fn test_partition_value_serialization_matches_iceberg_datum() {
        // Round tripping through the binary single-value serialization has to reproduce the
        // exact datum, otherwise the transform would hash different bytes.
        for (literal, ty, datum) in [
            (Literal::int(34), PrimitiveType::Int, Datum::int(34)),
            (Literal::long(-7), PrimitiveType::Long, Datum::long(-7)),
            (
                Literal::date(17486),
                PrimitiveType::Date,
                Datum::date(17486),
            ),
            (
                Literal::string("iceberg"),
                PrimitiveType::String,
                Datum::string("iceberg"),
            ),
            (
                Literal::binary(vec![0, 1, 2]),
                PrimitiveType::Binary,
                Datum::binary(vec![0, 1, 2]),
            ),
        ] {
            let bytes = iceberg_value_bytes(&ty, &literal.as_primitive_literal().unwrap()).unwrap();
            assert_eq!(Datum::try_from_bytes(&bytes, ty).unwrap(), datum);
        }
    }
}
