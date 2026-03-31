// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! TaskWriter for DataFusion integration.
//!
//! This module provides a high-level writer that handles partitioning and routing
//! of RecordBatch data to Iceberg tables.

use datafusion::arrow::array::RecordBatch;
use iceberg::Result;
use iceberg::arrow::RecordBatchPartitionSplitter;
use iceberg::spec::{DataFile, PartitionSpecRef, SchemaRef};
use iceberg::writer::IcebergWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::clustered_writer::ClusteredWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::unpartitioned_writer::UnpartitionedWriter;

/// High-level writer for DataFusion that handles partitioning and routing of RecordBatch data.
///
/// TaskWriter coordinates writing data to Iceberg tables by:
/// - Selecting the appropriate partitioning strategy (unpartitioned, fanout, or clustered)
/// - Initializing the partition splitter in the constructor for partitioned tables
/// - Routing data to the underlying writer
/// - Collecting all written data files
///
/// # Type Parameters
///
/// * `B` - The IcebergWriterBuilder type used to create underlying writers
///
/// # Example
///
/// ```rust,ignore
/// use iceberg::spec::{PartitionSpec, Schema};
/// use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
/// use iceberg_datafusion::writer::task_writer::TaskWriter;
///
/// // Create a TaskWriter for an unpartitioned table
/// let task_writer = TaskWriter::new(
///     data_file_writer_builder,
///     false, // fanout_enabled
///     schema,
///     partition_spec,
/// );
///
/// // Write data
/// task_writer.write(record_batch).await?;
///
/// // Close and get data files
/// let data_files = task_writer.close().await?;
/// ```
pub(crate) struct TaskWriter<B: IcebergWriterBuilder> {
    /// The underlying writer (UnpartitionedWriter, FanoutWriter, or ClusteredWriter)
    writer: SupportedWriter<B>,
    /// Partition splitter for partitioned tables (initialized in constructor)
    partition_splitter: Option<RecordBatchPartitionSplitter>,
}

/// Internal enum to hold the different writer types.
///
/// This enum allows TaskWriter to work with different partitioning strategies
/// while maintaining a unified interface.
enum SupportedWriter<B: IcebergWriterBuilder> {
    /// Writer for unpartitioned tables
    Unpartitioned(UnpartitionedWriter<B>),
    /// Writer for partitioned tables with unsorted data (maintains multiple active writers)
    Fanout(FanoutWriter<B>),
    /// Writer for partitioned tables with sorted data (maintains single active writer)
    Clustered(ClusteredWriter<B>),
}

impl<B: IcebergWriterBuilder> TaskWriter<B> {
    /// Create a new TaskWriter.
    ///
    /// # Parameters
    ///
    /// * `writer_builder` - The IcebergWriterBuilder to use for creating underlying writers
    /// * `fanout_enabled` - If true, use FanoutWriter for partitioned tables; otherwise use ClusteredWriter
    /// * `schema` - The Iceberg schema reference
    /// * `partition_spec` - The partition specification reference
    ///
    /// # Returns
    ///
    /// Returns a new TaskWriter instance.
    ///
    /// # Writer Selection Logic
    ///
    /// - If partition_spec is unpartitioned: creates UnpartitionedWriter
    /// - If partition_spec is partitioned AND fanout_enabled is true: creates FanoutWriter
    /// - If partition_spec is partitioned AND fanout_enabled is false: creates ClusteredWriter
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg::spec::{PartitionSpec, Schema};
    /// use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    /// use iceberg_datafusion::writer::task_writer::TaskWriter;
    ///
    /// // Create a TaskWriter for an unpartitioned table
    /// let task_writer = TaskWriter::new(
    ///     data_file_writer_builder,
    ///     false, // fanout_enabled
    ///     schema,
    ///     partition_spec,
    /// );
    /// ```
    pub fn try_new(
        writer_builder: B,
        fanout_enabled: bool,
        schema: SchemaRef,
        partition_spec: PartitionSpecRef,
    ) -> Result<Self> {
        let writer = if partition_spec.is_unpartitioned() {
            SupportedWriter::Unpartitioned(UnpartitionedWriter::new(writer_builder))
        } else if fanout_enabled {
            SupportedWriter::Fanout(FanoutWriter::new(writer_builder))
        } else {
            SupportedWriter::Clustered(ClusteredWriter::new(writer_builder))
        };

        // Initialize partition splitter in constructor for partitioned tables
        let partition_splitter = if !partition_spec.is_unpartitioned() {
            Some(
                RecordBatchPartitionSplitter::try_new_with_precomputed_values(
                    schema.clone(),
                    partition_spec.clone(),
                )?,
            )
        } else {
            None
        };

        Ok(Self {
            writer,
            partition_splitter,
        })
    }

    /// Write a RecordBatch to the TaskWriter.
    ///
    /// For partitioned tables, uses the partition splitter to split
    /// the batch by partition key and route each partition to the underlying writer.
    /// For unpartitioned tables, data is written directly without splitting.
    ///
    /// # Parameters
    ///
    /// * `batch` - The RecordBatch to write
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the write fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - Splitting the batch by partition fails
    /// - Writing to the underlying writer fails
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use arrow_array::RecordBatch;
    /// use iceberg_datafusion::writer::task_writer::TaskWriter;
    ///
    /// // Write a RecordBatch
    /// task_writer.write(record_batch).await?;
    /// ```
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        match &mut self.writer {
            SupportedWriter::Unpartitioned(writer) => {
                // Unpartitioned: write directly without splitting
                writer.write(batch).await
            }
            SupportedWriter::Fanout(writer) => {
                Self::write_partitioned_batches(writer, &self.partition_splitter, &batch).await
            }
            SupportedWriter::Clustered(writer) => {
                Self::write_partitioned_batches(writer, &self.partition_splitter, &batch).await
            }
        }
    }

    /// Helper method to split and write partitioned data.
    ///
    /// This method handles the common logic for both FanoutWriter and ClusteredWriter:
    /// - Splits the batch by partition key using the partition splitter
    /// - Writes each partition to the underlying writer with its corresponding partition key
    ///
    /// # Parameters
    ///
    /// * `writer` - The underlying PartitioningWriter (FanoutWriter or ClusteredWriter)
    /// * `partition_splitter` - The partition splitter
    /// * `batch` - The RecordBatch to write
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if the operation fails.
    async fn write_partitioned_batches<W: PartitioningWriter>(
        writer: &mut W,
        partition_splitter: &Option<RecordBatchPartitionSplitter>,
        batch: &RecordBatch,
    ) -> Result<()> {
        // Split batch by partition
        let splitter = partition_splitter
            .as_ref()
            .expect("Partition splitter should be initialized");
        let partitioned_batches = splitter.split(batch)?;

        // Write each partition
        for (partition_key, partition_batch) in partitioned_batches {
            writer.write(partition_key, partition_batch).await?;
        }

        Ok(())
    }

    /// Close the TaskWriter and return all written data files.
    ///
    /// This method consumes the TaskWriter to prevent further use.
    ///
    /// # Returns
    ///
    /// Returns a `Vec<DataFile>` containing all written files, or an error if closing fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if:
    /// - Closing the underlying writer fails
    /// - Any I/O operation fails during the close process
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use iceberg_datafusion::writer::task_writer::TaskWriter;
    ///
    /// // Close the writer and get all data files
    /// let data_files = task_writer.close().await?;
    /// ```
    pub async fn close(self) -> Result<Vec<DataFile>> {
        match self.writer {
            SupportedWriter::Unpartitioned(writer) => writer.close().await,
            SupportedWriter::Fanout(writer) => writer.close().await,
            SupportedWriter::Clustered(writer) => writer.close().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use iceberg::arrow::PROJECTED_PARTITION_VALUE_COLUMN;
    use iceberg::io::FileIOBuilder;
    use iceberg::spec::{DataFileFormat, NestedField, PartitionSpec, PrimitiveType, Type};
    use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use iceberg::writer::file_writer::ParquetWriterBuilder;
    use iceberg::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;

    fn create_test_schema() -> Result<Arc<iceberg::spec::Schema>> {
        Ok(Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        ))
    }

    fn create_arrow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
        ]))
    }

    fn create_arrow_schema_with_partition() -> Arc<Schema> {
        let partition_field = Field::new("region", DataType::Utf8, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1000".to_string())]),
        );
        let partition_struct_field = Field::new(
            PROJECTED_PARTITION_VALUE_COLUMN,
            DataType::Struct(vec![partition_field].into()),
            false,
        );

        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "3".to_string(),
            )])),
            partition_struct_field,
        ]))
    }

    fn create_writer_builder(
        temp_dir: &TempDir,
        schema: Arc<iceberg::spec::Schema>,
    ) -> Result<
        DataFileWriterBuilder<
            ParquetWriterBuilder,
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        >,
    > {
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema);
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io,
            location_gen,
            file_name_gen,
        );
        Ok(DataFileWriterBuilder::new(rolling_writer_builder))
    }

    #[tokio::test]
    async fn test_task_writer_unpartitioned() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let arrow_schema = create_arrow_schema();

        // Create unpartitioned spec
        let partition_spec = Arc::new(PartitionSpec::builder(schema.clone()).build()?);

        let writer_builder = create_writer_builder(&temp_dir, schema.clone())?;
        let mut task_writer = TaskWriter::try_new(writer_builder, false, schema, partition_spec)?;

        // Write data
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(StringArray::from(vec!["US", "EU", "US"])),
        ])?;

        task_writer.write(batch).await?;
        let data_files = task_writer.close().await?;

        // Verify results
        assert!(!data_files.is_empty());
        assert_eq!(data_files[0].record_count(), 3);

        Ok(())
    }

    /// Helper to verify partition data files
    fn verify_partition_files(
        data_files: &[iceberg::spec::DataFile],
        expected_total: u64,
    ) -> HashMap<String, u64> {
        let total_records: u64 = data_files.iter().map(|f| f.record_count()).sum();
        assert_eq!(total_records, expected_total, "Total record count mismatch");

        let mut partition_counts = HashMap::new();
        for data_file in data_files {
            let partition_value = data_file.partition();
            let region_literal = partition_value.fields()[0]
                .as_ref()
                .expect("Partition value should not be null");
            let region = match region_literal
                .as_primitive_literal()
                .expect("Should be primitive literal")
            {
                iceberg::spec::PrimitiveLiteral::String(s) => s.clone(),
                _ => panic!("Expected string partition value"),
            };

            *partition_counts.entry(region.clone()).or_insert(0) += data_file.record_count();

            // Verify file path contains partition information
            assert!(
                data_file.file_path().contains("region="),
                "File path should contain partition info"
            );
        }
        partition_counts
    }

    #[tokio::test]
    async fn test_task_writer_partitioned_fanout() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let arrow_schema = create_arrow_schema_with_partition();

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("region", "region", iceberg::spec::Transform::Identity)?
                .build()?,
        );

        let writer_builder = create_writer_builder(&temp_dir, schema.clone())?;
        let mut task_writer = TaskWriter::try_new(writer_builder, true, schema, partition_spec)?;

        // Create partition column
        let partition_field = Field::new("region", DataType::Utf8, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1000".to_string())]),
        );
        let partition_values = StringArray::from(vec!["US", "EU", "US", "EU"]);
        let partition_struct = StructArray::from(vec![(
            Arc::new(partition_field),
            Arc::new(partition_values) as ArrayRef,
        )]);

        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["US", "EU", "US", "EU"])),
            Arc::new(partition_struct),
        ])?;

        task_writer.write(batch).await?;
        let data_files = task_writer.close().await?;

        let partition_counts = verify_partition_files(&data_files, 4);
        assert_eq!(partition_counts.get("US"), Some(&2));
        assert_eq!(partition_counts.get("EU"), Some(&2));

        Ok(())
    }

    #[tokio::test]
    async fn test_task_writer_partitioned_clustered() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let schema = create_test_schema()?;
        let arrow_schema = create_arrow_schema_with_partition();

        let partition_spec = Arc::new(
            PartitionSpec::builder(schema.clone())
                .with_spec_id(1)
                .add_partition_field("region", "region", iceberg::spec::Transform::Identity)?
                .build()?,
        );

        let writer_builder = create_writer_builder(&temp_dir, schema.clone())?;
        let mut task_writer = TaskWriter::try_new(writer_builder, false, schema, partition_spec)?;

        // Create partition column
        let partition_field = Field::new("region", DataType::Utf8, false).with_metadata(
            HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_string(), "1000".to_string())]),
        );
        let partition_values = StringArray::from(vec!["ASIA", "ASIA", "EU", "EU"]);
        let partition_struct = StructArray::from(vec![(
            Arc::new(partition_field),
            Arc::new(partition_values) as ArrayRef,
        )]);

        // ClusteredWriter expects data to be pre-sorted by partition
        let batch = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["ASIA", "ASIA", "EU", "EU"])),
            Arc::new(partition_struct),
        ])?;

        task_writer.write(batch).await?;
        let data_files = task_writer.close().await?;

        let partition_counts = verify_partition_files(&data_files, 4);
        assert_eq!(partition_counts.get("ASIA"), Some(&2));
        assert_eq!(partition_counts.get("EU"), Some(&2));

        Ok(())
    }
}
