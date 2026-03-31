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

//! This module provides the `UnpartitionedWriter` implementation.

use std::marker::PhantomData;

use crate::Result;
use crate::writer::{DefaultInput, DefaultOutput, IcebergWriter, IcebergWriterBuilder};

/// A simple wrapper around `IcebergWriterBuilder` for unpartitioned tables.
///
/// This writer lazily creates the underlying writer on the first write operation
/// and writes all data to a single file (or set of files if rolling).
///
/// # Type Parameters
///
/// * `B` - The inner writer builder type
/// * `I` - Input type (defaults to `RecordBatch`)
/// * `O` - Output collection type (defaults to `Vec<DataFile>`)
pub struct UnpartitionedWriter<B, I = DefaultInput, O = DefaultOutput>
where
    B: IcebergWriterBuilder<I, O>,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item>,
    <O as IntoIterator>::Item: Clone,
{
    inner_builder: B,
    writer: Option<B::R>,
    output: Vec<<O as IntoIterator>::Item>,
    _phantom: PhantomData<I>,
}

impl<B, I, O> UnpartitionedWriter<B, I, O>
where
    B: IcebergWriterBuilder<I, O>,
    I: Send + 'static,
    O: IntoIterator + FromIterator<<O as IntoIterator>::Item>,
    <O as IntoIterator>::Item: Send + Clone,
{
    /// Create a new `UnpartitionedWriter`.
    pub fn new(inner_builder: B) -> Self {
        Self {
            inner_builder,
            writer: None,
            output: Vec::new(),
            _phantom: PhantomData,
        }
    }

    /// Write data to the writer.
    ///
    /// The underlying writer is lazily created on the first write operation.
    ///
    /// # Parameters
    ///
    /// * `input` - The input data to write
    ///
    /// # Returns
    ///
    /// `Ok(())` on success, or an error if the write operation fails.
    pub async fn write(&mut self, input: I) -> Result<()> {
        // Lazily create writer on first write
        if self.writer.is_none() {
            self.writer = Some(self.inner_builder.build(None).await?);
        }

        // Write directly to inner writer
        self.writer
            .as_mut()
            .expect("Writer should be initialized")
            .write(input)
            .await
    }

    /// Close the writer and return all written data files.
    ///
    /// This method consumes the writer to prevent further use.
    ///
    /// # Returns
    ///
    /// The accumulated output from all write operations, or an empty collection
    /// if no data was written.
    pub async fn close(mut self) -> Result<O> {
        if let Some(mut writer) = self.writer.take() {
            self.output.extend(writer.close().await?);
        }
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
    use crate::Result;
    use crate::io::FileIOBuilder;
    use crate::spec::{DataFileFormat, NestedField, PrimitiveType, Struct, Type};
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;

    #[tokio::test]
    async fn test_unpartitioned_writer() -> Result<()> {
        let temp_dir = TempDir::new()?;

        // Build Iceberg schema
        let schema = Arc::new(
            crate::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                ])
                .build()?,
        );

        // Build Arrow schema
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "1".to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                "2".to_string(),
            )])),
        ]));

        // Build writer
        let file_io = FileIOBuilder::new_fs_io().build()?;
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);
        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema.clone());
        let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io,
            location_gen,
            file_name_gen,
        );
        let writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        let mut writer = UnpartitionedWriter::new(writer_builder);

        // Write two batches
        let batch1 = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ])?;
        let batch2 = RecordBatch::try_new(arrow_schema, vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
        ])?;

        writer.write(batch1).await?;
        writer.write(batch2).await?;

        let data_files = writer.close().await?;

        // Verify files have empty partition and correct format
        assert!(!data_files.is_empty());
        for file in &data_files {
            assert_eq!(file.partition, Struct::empty());
            assert_eq!(file.file_format, DataFileFormat::Parquet);
            assert_eq!(file.record_count, 4);
        }

        Ok(())
    }
}
