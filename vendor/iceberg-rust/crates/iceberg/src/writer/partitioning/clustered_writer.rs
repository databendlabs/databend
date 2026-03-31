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

//! This module provides the `ClusteredWriter` implementation.

use std::collections::HashSet;
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::spec::{PartitionKey, Struct};
use crate::writer::partitioning::PartitioningWriter;
use crate::writer::{DefaultInput, DefaultOutput, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// A writer that writes data to a single partition at a time.
///
/// This writer expects input data to be sorted by partition key. It maintains only one
/// active writer at a time, making it memory efficient for sorted data.
///
/// # Type Parameters
///
/// * `B` - The inner writer builder type
/// * `I` - Input type (defaults to `RecordBatch`)
/// * `O` - Output collection type (defaults to `Vec<DataFile>`)
pub struct ClusteredWriter<B, I = DefaultInput, O = DefaultOutput>
where
    B: IcebergWriterBuilder<I, O>,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item>,
    <O as IntoIterator>::Item: Clone,
{
    inner_builder: B,
    current_writer: Option<B::R>,
    current_partition: Option<Struct>,
    closed_partitions: HashSet<Struct>,
    output: Vec<<O as IntoIterator>::Item>,
    _phantom: PhantomData<I>,
}

impl<B, I, O> ClusteredWriter<B, I, O>
where
    B: IcebergWriterBuilder<I, O>,
    I: Send + 'static,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item>,
    <O as IntoIterator>::Item: Send + Clone,
{
    /// Create a new `ClusteredWriter`.
    pub fn new(inner_builder: B) -> Self {
        Self {
            inner_builder,
            current_writer: None,
            current_partition: None,
            closed_partitions: HashSet::new(),
            output: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Closes the current writer if it exists, flushes the written data to output, and record closed partition.
    async fn close_current_writer(&mut self) -> Result<()> {
        if let Some(mut writer) = self.current_writer.take() {
            self.output.extend(writer.close().await?);

            // Add the current partition to the set of closed partitions
            if let Some(current_partition) = self.current_partition.take() {
                self.closed_partitions.insert(current_partition);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<B, I, O> PartitioningWriter<I, O> for ClusteredWriter<B, I, O>
where
    B: IcebergWriterBuilder<I, O>,
    I: Send + 'static,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item> + Send + 'static,
    <O as IntoIterator>::Item: Send + Clone,
{
    async fn write(&mut self, partition_key: PartitionKey, input: I) -> Result<()> {
        let partition_value = partition_key.data();

        // Check if this partition has been closed already
        if self.closed_partitions.contains(partition_value) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "The input is not sorted! Cannot write to partition that was previously closed: {partition_key:?}"
                ),
            ));
        }

        // Check if we need to switch to a new partition
        let need_new_writer = match &self.current_partition {
            Some(current) => current != partition_value,
            None => true,
        };

        if need_new_writer {
            self.close_current_writer().await?;

            // Create a new writer for the new partition
            self.current_writer = Some(
                self.inner_builder
                    .build(Some(partition_key.clone()))
                    .await?,
            );
            self.current_partition = Some(partition_value.clone());
        }

        // do write
        self.current_writer
            .as_mut()
            .expect("Writer should be initialized")
            .write(input)
            .await
    }

    async fn close(mut self) -> Result<O> {
        self.close_current_writer().await?;

        // Collect all output items into the output collection type
        Ok(O::from_iter(self.output))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Type};
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

    #[tokio::test]
    async fn test_clustered_writer_single_partition() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema with partition field
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        );

        // Create partition spec and key
        let partition_spec = crate::spec::PartitionSpec::builder(schema.clone()).build()?;
        let partition_value =
            crate::spec::Struct::from_iter([Some(crate::spec::Literal::string("US"))]);
        let partition_key =
            crate::spec::PartitionKey::new(partition_spec, schema.clone(), partition_value.clone());

        // Create writer builder
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        // Create rolling file writer builder
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create clustered writer
        let mut writer = ClusteredWriter::new(data_file_writer_builder);

        // Create test data with proper field ID metadata
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                3.to_string(),
            )])),
        ]);

        let batch1 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        let batch2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        // Write data to the same partition (this should work)
        writer.write(partition_key.clone(), batch1).await?;
        writer.write(partition_key.clone(), batch2).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify at least one file was created
        assert!(
            !data_files.is_empty(),
            "Expected at least one data file to be created"
        );

        // Verify that all data files have the correct partition value
        for data_file in &data_files {
            assert_eq!(data_file.partition, partition_value);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_clustered_writer_sorted_partitions() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema with partition field
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        );

        // Create partition spec
        let partition_spec = crate::spec::PartitionSpec::builder(schema.clone()).build()?;

        // Create partition keys for different regions (in sorted order)
        let partition_value_asia =
            crate::spec::Struct::from_iter([Some(crate::spec::Literal::string("ASIA"))]);
        let partition_key_asia = crate::spec::PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_asia.clone(),
        );

        let partition_value_eu =
            crate::spec::Struct::from_iter([Some(crate::spec::Literal::string("EU"))]);
        let partition_key_eu = crate::spec::PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_eu.clone(),
        );

        let partition_value_us =
            crate::spec::Struct::from_iter([Some(crate::spec::Literal::string("US"))]);
        let partition_key_us = crate::spec::PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_us.clone(),
        );

        // Create writer builder
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        // Create rolling file writer builder
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create clustered writer
        let mut writer = ClusteredWriter::new(data_file_writer_builder);

        // Create test data with proper field ID metadata
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                3.to_string(),
            )])),
        ]);

        // Create batches for different partitions (in sorted order)
        let batch_asia = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["ASIA", "ASIA"])),
        ])?;

        let batch_eu = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["EU", "EU"])),
        ])?;

        let batch_us = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![5, 6])),
            Arc::new(StringArray::from(vec!["Eve", "Frank"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        // Write data in sorted partition order (this should work)
        writer.write(partition_key_asia.clone(), batch_asia).await?;
        writer.write(partition_key_eu.clone(), batch_eu).await?;
        writer.write(partition_key_us.clone(), batch_us).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify files were created for all partitions
        assert!(
            data_files.len() >= 3,
            "Expected at least 3 data files (one per partition), got {}",
            data_files.len()
        );

        // Verify that we have files for each partition
        let mut partitions_found = std::collections::HashSet::new();
        for data_file in &data_files {
            partitions_found.insert(data_file.partition.clone());
        }

        assert!(
            partitions_found.contains(&partition_value_asia),
            "Missing ASIA partition"
        );
        assert!(
            partitions_found.contains(&partition_value_eu),
            "Missing EU partition"
        );
        assert!(
            partitions_found.contains(&partition_value_us),
            "Missing US partition"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_clustered_writer_unsorted_partitions_error() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema with partition field
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(3, "region", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])
                .build()?,
        );

        // Create partition spec
        let partition_spec = crate::spec::PartitionSpec::builder(schema.clone()).build()?;

        // Create partition keys for different regions
        let partition_value_us =
            crate::spec::Struct::from_iter([Some(crate::spec::Literal::string("US"))]);
        let partition_key_us = crate::spec::PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_us.clone(),
        );

        let partition_value_eu =
            crate::spec::Struct::from_iter([Some(crate::spec::Literal::string("EU"))]);
        let partition_key_eu = crate::spec::PartitionKey::new(
            partition_spec.clone(),
            schema.clone(),
            partition_value_eu.clone(),
        );

        // Create writer builder
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());

        // Create rolling file writer builder
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        // Create data file writer builder
        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create clustered writer
        let mut writer = ClusteredWriter::new(data_file_writer_builder);

        // Create test data with proper field ID metadata
        let arrow_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
            Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                3.to_string(),
            )])),
        ]);

        // Create batches for different partitions
        let batch_us = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            Arc::new(StringArray::from(vec!["US", "US"])),
        ])?;

        let batch_eu = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(StringArray::from(vec!["EU", "EU"])),
        ])?;

        let batch_us2 = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![5])),
            Arc::new(StringArray::from(vec!["Eve"])),
            Arc::new(StringArray::from(vec!["US"])),
        ])?;

        // Write data to US partition first
        writer.write(partition_key_us.clone(), batch_us).await?;

        // Write data to EU partition (this closes US partition)
        writer.write(partition_key_eu.clone(), batch_eu).await?;

        // Try to write to US partition again - this should fail because data is not sorted
        let result = writer.write(partition_key_us.clone(), batch_us2).await;

        assert!(result.is_err(), "Expected error when writing unsorted data");

        let error = result.unwrap_err();
        assert!(
            error.to_string().contains("The input is not sorted"),
            "Expected 'input is not sorted' error, got: {error}"
        );

        Ok(())
    }
}
