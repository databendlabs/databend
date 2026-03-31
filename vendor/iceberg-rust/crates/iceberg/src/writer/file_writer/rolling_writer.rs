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

use std::fmt::{Debug, Formatter};

use arrow_array::RecordBatch;

use crate::io::{FileIO, OutputFile};
use crate::spec::{DataFileBuilder, PartitionKey, TableProperties};
use crate::writer::CurrentFileStatus;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::{FileWriter, FileWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for [`RollingFileWriter`].
#[derive(Clone, Debug)]
pub struct RollingFileWriterBuilder<
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
> {
    inner_builder: B,
    target_file_size: usize,
    file_io: FileIO,
    location_generator: L,
    file_name_generator: F,
}

impl<B, L, F> RollingFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Creates a new `RollingFileWriterBuilder` with the specified target file size.
    ///
    /// # Parameters
    ///
    /// * `inner_builder` - The builder for the underlying file writer
    /// * `target_file_size` - The target file size in bytes that triggers rollover
    /// * `file_io` - The file IO interface for creating output files
    /// * `location_generator` - Generator for file locations
    /// * `file_name_generator` - Generator for file names
    ///
    /// # Returns
    ///
    /// A new `RollingFileWriterBuilder` instance
    pub fn new(
        inner_builder: B,
        target_file_size: usize,
        file_io: FileIO,
        location_generator: L,
        file_name_generator: F,
    ) -> Self {
        Self {
            inner_builder,
            target_file_size,
            file_io,
            location_generator,
            file_name_generator,
        }
    }

    /// Creates a new `RollingFileWriterBuilder` with the default target file size.
    ///
    /// # Parameters
    ///
    /// * `inner_builder` - The builder for the underlying file writer
    /// * `file_io` - The file IO interface for creating output files
    /// * `location_generator` - Generator for file locations
    /// * `file_name_generator` - Generator for file names
    ///
    /// # Returns
    ///
    /// A new `RollingFileWriterBuilder` instance with default target file size
    pub fn new_with_default_file_size(
        inner_builder: B,
        file_io: FileIO,
        location_generator: L,
        file_name_generator: F,
    ) -> Self {
        Self {
            inner_builder,
            target_file_size: TableProperties::PROPERTY_WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT,
            file_io,
            location_generator,
            file_name_generator,
        }
    }

    /// Build a new [`RollingFileWriter`].
    pub fn build(&self) -> RollingFileWriter<B, L, F> {
        RollingFileWriter {
            inner: None,
            inner_builder: self.inner_builder.clone(),
            target_file_size: self.target_file_size,
            data_file_builders: vec![],
            file_io: self.file_io.clone(),
            location_generator: self.location_generator.clone(),
            file_name_generator: self.file_name_generator.clone(),
        }
    }
}

/// A writer that automatically rolls over to a new file when the data size
/// exceeds a target threshold.
///
/// This writer wraps another file writer that tracks the amount of data written.
/// When the data size exceeds the target size, it closes the current file and
/// starts writing to a new one.
pub struct RollingFileWriter<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator> {
    inner: Option<B::R>,
    inner_builder: B,
    target_file_size: usize,
    data_file_builders: Vec<DataFileBuilder>,
    file_io: FileIO,
    location_generator: L,
    file_name_generator: F,
}

impl<B, L, F> Debug for RollingFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RollingFileWriter")
            .field("target_file_size", &self.target_file_size)
            .field("file_io", &self.file_io)
            .finish()
    }
}

impl<B, L, F> RollingFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Determines if the writer should roll over to a new file.
    ///
    /// # Returns
    ///
    /// `true` if a new file should be started, `false` otherwise
    fn should_roll(&self) -> bool {
        self.current_written_size() > self.target_file_size
    }

    fn new_output_file(&self, partition_key: &Option<PartitionKey>) -> Result<OutputFile> {
        self.file_io
            .new_output(self.location_generator.generate_location(
                partition_key.as_ref(),
                &self.file_name_generator.generate_file_name(),
            ))
    }

    /// Writes a record batch to the current file, rolling over to a new file if necessary.
    ///
    /// # Parameters
    ///
    /// * `partition_key` - Optional partition key for the data
    /// * `input` - The record batch to write
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure
    ///
    /// # Errors
    ///
    /// Returns an error if the writer is not initialized or if writing fails
    pub async fn write(
        &mut self,
        partition_key: &Option<PartitionKey>,
        input: &RecordBatch,
    ) -> Result<()> {
        if self.inner.is_none() {
            // initialize inner writer
            self.inner = Some(
                self.inner_builder
                    .build(self.new_output_file(partition_key)?)
                    .await?,
            );
        }

        if self.should_roll()
            && let Some(inner) = self.inner.take()
        {
            // close the current writer, roll to a new file
            self.data_file_builders.extend(inner.close().await?);

            // start a new writer
            self.inner = Some(
                self.inner_builder
                    .build(self.new_output_file(partition_key)?)
                    .await?,
            );
        }

        // write the input
        if let Some(writer) = self.inner.as_mut() {
            Ok(writer.write(input).await?)
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Writer is not initialized!",
            ))
        }
    }

    /// Closes the writer and returns all data file builders.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of `DataFileBuilder` instances representing
    /// all files that were written, including any that were created due to rollover
    pub async fn close(mut self) -> Result<Vec<DataFileBuilder>> {
        // close the current writer and merge the output
        if let Some(current_writer) = self.inner {
            self.data_file_builders
                .extend(current_writer.close().await?);
        }

        Ok(self.data_file_builders)
    }
}

impl<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator> CurrentFileStatus
    for RollingFileWriter<B, L, F>
{
    fn current_file_path(&self) -> String {
        self.inner.as_ref().unwrap().current_file_path()
    }

    fn current_row_num(&self) -> usize {
        self.inner.as_ref().unwrap().current_row_num()
    }

    fn current_written_size(&self) -> usize {
        self.inner.as_ref().unwrap().current_written_size()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;
    use rand::prelude::IteratorRandom;
    use tempfile::TempDir;

    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::tests::check_parquet_data_file;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder, RecordBatch};

    fn make_test_schema() -> Result<Schema> {
        Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
    }

    fn make_test_arrow_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                1.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                2.to_string(),
            )])),
        ])
    }

    #[tokio::test]
    async fn test_rolling_writer_basic() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = make_test_schema()?;

        // Create writer builders
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), Arc::new(schema));

        // Set a large target size so no rolling occurs
        let rolling_file_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            1024 * 1024,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);

        // Create writer
        let mut writer = data_file_writer_builder.build(None).await?;

        // Create test data
        let arrow_schema = make_test_arrow_schema();

        let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])?;

        // Write data
        writer.write(batch.clone()).await?;

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify only one file was created
        assert_eq!(
            data_files.len(),
            1,
            "Expected only one data file to be created"
        );

        // Verify file content
        check_parquet_data_file(&file_io, &data_files[0], &batch).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_rolling_writer_with_rolling() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        // Create schema
        let schema = make_test_schema()?;

        // Create writer builders
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), Arc::new(schema));

        // Set a very small target size to trigger rolling
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            1024,
            file_io,
            location_gen,
            file_name_gen,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create writer
        let mut writer = data_file_writer_builder.build(None).await?;

        // Create test data
        let arrow_schema = make_test_arrow_schema();
        let arrow_schema_ref = Arc::new(arrow_schema.clone());

        let names = vec![
            "Alice", "Bob", "Charlie", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy",
            "Kelly", "Larry", "Mallory", "Shawn",
        ];

        let mut rng = rand::thread_rng();
        let batch_num = 10;
        let batch_rows = 100;
        let expected_rows = batch_num * batch_rows;

        for i in 0..batch_num {
            let int_values: Vec<i32> = (0..batch_rows).map(|row| i * batch_rows + row).collect();
            let str_values: Vec<&str> = (0..batch_rows)
                .map(|_| *names.iter().choose(&mut rng).unwrap())
                .collect();

            let int_array = Arc::new(Int32Array::from(int_values)) as ArrayRef;
            let str_array = Arc::new(StringArray::from(str_values)) as ArrayRef;

            let batch =
                RecordBatch::try_new(Arc::clone(&arrow_schema_ref), vec![int_array, str_array])
                    .expect("Failed to create RecordBatch");

            writer.write(batch).await?;
        }

        // Close writer and get data files
        let data_files = writer.close().await?;

        // Verify multiple files were created (at least 4)
        assert!(
            data_files.len() > 4,
            "Expected at least 4 data files to be created, but got {}",
            data_files.len()
        );

        // Verify total record count across all files
        let total_records: u64 = data_files.iter().map(|file| file.record_count).sum();
        assert_eq!(
            total_records, expected_rows as u64,
            "Expected {expected_rows} total records across all files"
        );

        Ok(())
    }
}
