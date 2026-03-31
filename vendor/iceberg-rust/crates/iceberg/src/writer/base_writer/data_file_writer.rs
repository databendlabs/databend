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

//! This module provide `DataFileWriter`.

use arrow_array::RecordBatch;

use crate::spec::{DataContentType, DataFile, PartitionKey};
use crate::writer::file_writer::FileWriterBuilder;
use crate::writer::file_writer::location_generator::{FileNameGenerator, LocationGenerator};
use crate::writer::file_writer::rolling_writer::{RollingFileWriter, RollingFileWriterBuilder};
use crate::writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder};
use crate::{Error, ErrorKind, Result};

/// Builder for `DataFileWriter`.
#[derive(Debug)]
pub struct DataFileWriterBuilder<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator> {
    inner: RollingFileWriterBuilder<B, L, F>,
}

impl<B, L, F> DataFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    /// Create a new `DataFileWriterBuilder` using a `RollingFileWriterBuilder`.
    pub fn new(inner: RollingFileWriterBuilder<B, L, F>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl<B, L, F> IcebergWriterBuilder for DataFileWriterBuilder<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    type R = DataFileWriter<B, L, F>;

    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
        Ok(DataFileWriter {
            inner: Some(self.inner.build()),
            partition_key,
        })
    }
}

/// A writer write data is within one spec/partition.
#[derive(Debug)]
pub struct DataFileWriter<B: FileWriterBuilder, L: LocationGenerator, F: FileNameGenerator> {
    inner: Option<RollingFileWriter<B, L, F>>,
    partition_key: Option<PartitionKey>,
}

#[async_trait::async_trait]
impl<B, L, F> IcebergWriter for DataFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
{
    async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(writer) = self.inner.as_mut() {
            writer.write(&self.partition_key, &batch).await
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Writer is not initialized!",
            ))
        }
    }

    async fn close(&mut self) -> Result<Vec<DataFile>> {
        if let Some(writer) = self.inner.take() {
            writer
                .close()
                .await?
                .into_iter()
                .map(|mut res| {
                    res.content(DataContentType::Data);
                    if let Some(pk) = self.partition_key.as_ref() {
                        res.partition(pk.data().clone());
                        res.partition_spec_id(pk.spec().spec_id());
                    }
                    res.build().map_err(|e| {
                        Error::new(
                            ErrorKind::DataInvalid,
                            format!("Failed to build data file: {e}"),
                        )
                    })
                })
                .collect()
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "Data file writer has been closed.",
            ))
        }
    }
}

impl<B, L, F> CurrentFileStatus for DataFileWriter<B, L, F>
where
    B: FileWriterBuilder,
    L: LocationGenerator,
    F: FileNameGenerator,
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
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use crate::Result;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        DataContentType, DataFileFormat, Literal, NestedField, PartitionKey, PartitionSpec,
        PrimitiveType, Schema, Struct, Type,
    };
    use crate::writer::base_writer::data_file_writer::DataFileWriterBuilder;
    use crate::writer::file_writer::ParquetWriterBuilder;
    use crate::writer::file_writer::location_generator::{
        DefaultFileNameGenerator, DefaultLocationGenerator,
    };
    use crate::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
    use crate::writer::{IcebergWriter, IcebergWriterBuilder, RecordBatch};

    #[tokio::test]
    async fn test_parquet_writer() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen =
            DefaultFileNameGenerator::new("test".to_string(), None, DataFileFormat::Parquet);

        let schema = Schema::builder()
            .with_schema_id(3)
            .with_fields(vec![
                NestedField::required(3, "foo", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(4, "bar", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;

        let pw = ParquetWriterBuilder::new(WriterProperties::builder().build(), Arc::new(schema));

        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            pw,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut data_file_writer = DataFileWriterBuilder::new(rolling_file_writer_builder)
            .build(None)
            .await
            .unwrap();

        let arrow_schema = arrow_schema::Schema::new(vec![
            Field::new("foo", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                3.to_string(),
            )])),
            Field::new("bar", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                4.to_string(),
            )])),
        ]);
        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])?;
        data_file_writer.write(batch).await?;

        let data_files = data_file_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);

        let data_file = &data_files[0];
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(data_file.content, DataContentType::Data);
        assert_eq!(data_file.partition, Struct::empty());

        let input_file = file_io.new_input(data_file.file_path.clone())?;
        let input_content = input_file.read().await?;

        let parquet_reader =
            ArrowReaderMetadata::load(&input_content, ArrowReaderOptions::default())
                .expect("Failed to load Parquet metadata");

        let field_ids: Vec<i32> = parquet_reader
            .parquet_schema()
            .columns()
            .iter()
            .map(|col| col.self_type().get_basic_info().id())
            .collect();

        assert_eq!(field_ids, vec![3, 4]);
        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_writer_with_partition() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let file_io = FileIOBuilder::new_fs_io().build().unwrap();
        let location_gen = DefaultLocationGenerator::with_data_location(
            temp_dir.path().to_str().unwrap().to_string(),
        );
        let file_name_gen = DefaultFileNameGenerator::new(
            "test_partitioned".to_string(),
            None,
            DataFileFormat::Parquet,
        );

        let schema = Schema::builder()
            .with_schema_id(5)
            .with_fields(vec![
                NestedField::required(5, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(6, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()?;
        let schema_ref = Arc::new(schema);

        let partition_value = Struct::from_iter([Some(Literal::int(1))]);
        let partition_key = PartitionKey::new(
            PartitionSpec::builder(schema_ref.clone()).build()?,
            schema_ref.clone(),
            partition_value.clone(),
        );

        let parquet_writer_builder =
            ParquetWriterBuilder::new(WriterProperties::builder().build(), schema_ref.clone());

        let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
            parquet_writer_builder,
            file_io.clone(),
            location_gen,
            file_name_gen,
        );

        let mut data_file_writer = DataFileWriterBuilder::new(rolling_file_writer_builder)
            .build(Some(partition_key))
            .await?;

        let arrow_schema = arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                5.to_string(),
            )])),
            Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                6.to_string(),
            )])),
        ]);
        let batch = RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])?;
        data_file_writer.write(batch).await?;

        let data_files = data_file_writer.close().await.unwrap();
        assert_eq!(data_files.len(), 1);

        let data_file = &data_files[0];
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);
        assert_eq!(data_file.content, DataContentType::Data);
        assert_eq!(data_file.partition, partition_value);

        let input_file = file_io.new_input(data_file.file_path.clone())?;
        let input_content = input_file.read().await?;

        let parquet_reader =
            ArrowReaderMetadata::load(&input_content, ArrowReaderOptions::default())?;

        let field_ids: Vec<i32> = parquet_reader
            .parquet_schema()
            .columns()
            .iter()
            .map(|col| col.self_type().get_basic_info().id())
            .collect();
        assert_eq!(field_ids, vec![5, 6]);

        let field_names: Vec<&str> = parquet_reader
            .parquet_schema()
            .columns()
            .iter()
            .map(|col| col.name())
            .collect();
        assert_eq!(field_names, vec!["id", "name"]);

        Ok(())
    }
}
