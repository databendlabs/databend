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

//! Iceberg writer module.
//!
//! This module contains the generic writer trait and specific writer implementation. We categorize the writer into two types:
//! 1. FileWriter: writer for physical file format (Such as parquet, orc).
//! 2. IcebergWriter: writer for logical format provided by iceberg table (Such as data file, equality delete file, position delete file)
//!    or other function (Such as partition writer, delta writer).
//!
//! The IcebergWriter will use the inner FileWriter to write physical files.
//!
//! The writer interface is designed to be extensible and flexible. Writers can be independently configured
//! and composed to support complex write logic. E.g. By combining `FanoutPartitionWriter`, `DataFileWriter`, and `ParquetWriter`,
//! you can build a writer that automatically partitions the data and writes it in the Parquet format.
//!
//! For this purpose, there are four trait corresponding to these writer:
//! - IcebergWriterBuilder
//! - IcebergWriter
//! - FileWriterBuilder
//! - FileWriter
//!
//! Users can create specific writer builders, combine them, and build the final writer.
//! They can also define custom writers by implementing the `Writer` trait,
//! allowing seamless integration with existing writers. (See the example below.)
//!
//! # Simple example for the data file writer used parquet physical format:
//! ```rust, no_run
//! use std::collections::HashMap;
//! use std::sync::Arc;
//!
//! use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
//! use async_trait::async_trait;
//! use iceberg::io::{FileIO, FileIOBuilder};
//! use iceberg::spec::DataFile;
//! use iceberg::transaction::Transaction;
//! use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//! use iceberg::writer::file_writer::ParquetWriterBuilder;
//! use iceberg::writer::file_writer::location_generator::{
//!     DefaultFileNameGenerator, DefaultLocationGenerator,
//! };
//! use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
//! use iceberg::{Catalog, CatalogBuilder, MemoryCatalog, Result, TableIdent};
//! use parquet::file::properties::WriterProperties;
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Connect to a catalog.
//!     use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
//!     use iceberg::writer::file_writer::rolling_writer::{
//!         RollingFileWriter, RollingFileWriterBuilder,
//!     };
//!     let catalog = MemoryCatalogBuilder::default()
//!         .load(
//!             "memory",
//!             HashMap::from([(
//!                 MEMORY_CATALOG_WAREHOUSE.to_string(),
//!                 "file:///path/to/warehouse".to_string(),
//!             )]),
//!         )
//!         .await?;
//!     // Add customized code to create a table first.
//!
//!     // Load table from catalog.
//!     let table = catalog
//!         .load_table(&TableIdent::from_strs(["hello", "world"])?)
//!         .await?;
//!     let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
//!     let file_name_generator = DefaultFileNameGenerator::new(
//!         "test".to_string(),
//!         None,
//!         iceberg::spec::DataFileFormat::Parquet,
//!     );
//!
//!     // Create a parquet file writer builder. The parameter can get from table.
//!     let parquet_writer_builder = ParquetWriterBuilder::new(
//!         WriterProperties::default(),
//!         table.metadata().current_schema().clone(),
//!     );
//!
//!     // Create a rolling file writer using parquet file writer builder.
//!     let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
//!         parquet_writer_builder,
//!         table.file_io().clone(),
//!         location_generator.clone(),
//!         file_name_generator.clone(),
//!     );
//!
//!     // Create a data file writer using parquet file writer builder.
//!     let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
//!     // Build the data file writer
//!     let mut data_file_writer = data_file_writer_builder.build(None).await?;
//!
//!     // Write the data using data_file_writer...
//!
//!     // Close the write and it will return data files back
//!     let data_files = data_file_writer.close().await.unwrap();
//!
//!     Ok(())
//! }
//! ```
//!
//! # Custom writer to record latency
//! ```rust, no_run
//! use std::collections::HashMap;
//! use std::time::Instant;
//!
//! use arrow_array::RecordBatch;
//! use iceberg::io::FileIOBuilder;
//! use iceberg::memory::MemoryCatalogBuilder;
//! use iceberg::spec::{DataFile, PartitionKey};
//! use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//! use iceberg::writer::file_writer::ParquetWriterBuilder;
//! use iceberg::writer::file_writer::location_generator::{
//!     DefaultFileNameGenerator, DefaultLocationGenerator,
//! };
//! use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
//! use iceberg::{Catalog, CatalogBuilder, MemoryCatalog, Result, TableIdent};
//! use parquet::file::properties::WriterProperties;
//!
//! #[derive(Clone)]
//! struct LatencyRecordWriterBuilder<B> {
//!     inner_writer_builder: B,
//! }
//!
//! impl<B: IcebergWriterBuilder> LatencyRecordWriterBuilder<B> {
//!     pub fn new(inner_writer_builder: B) -> Self {
//!         Self {
//!             inner_writer_builder,
//!         }
//!     }
//! }
//!
//! #[async_trait::async_trait]
//! impl<B: IcebergWriterBuilder> IcebergWriterBuilder for LatencyRecordWriterBuilder<B> {
//!     type R = LatencyRecordWriter<B::R>;
//!
//!     async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R> {
//!         Ok(LatencyRecordWriter {
//!             inner_writer: self.inner_writer_builder.build(partition_key).await?,
//!         })
//!     }
//! }
//! struct LatencyRecordWriter<W> {
//!     inner_writer: W,
//! }
//!
//! #[async_trait::async_trait]
//! impl<W: IcebergWriter> IcebergWriter for LatencyRecordWriter<W> {
//!     async fn write(&mut self, input: RecordBatch) -> Result<()> {
//!         let start = Instant::now();
//!         self.inner_writer.write(input).await?;
//!         let _latency = start.elapsed();
//!         // record latency...
//!         Ok(())
//!     }
//!
//!     async fn close(&mut self) -> Result<Vec<DataFile>> {
//!         let start = Instant::now();
//!         let res = self.inner_writer.close().await?;
//!         let _latency = start.elapsed();
//!         // record latency...
//!         Ok(res)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Connect to a catalog.
//!     use iceberg::memory::MEMORY_CATALOG_WAREHOUSE;
//!     use iceberg::spec::{Literal, PartitionKey, Struct};
//!     use iceberg::writer::file_writer::rolling_writer::{
//!         RollingFileWriter, RollingFileWriterBuilder,
//!     };
//!
//!     let catalog = MemoryCatalogBuilder::default()
//!         .load(
//!             "memory",
//!             HashMap::from([(
//!                 MEMORY_CATALOG_WAREHOUSE.to_string(),
//!                 "file:///path/to/warehouse".to_string(),
//!             )]),
//!         )
//!         .await?;
//!
//!     // Add customized code to create a table first.
//!
//!     // Load table from catalog.
//!     let table = catalog
//!         .load_table(&TableIdent::from_strs(["hello", "world"])?)
//!         .await?;
//!     let partition_key = PartitionKey::new(
//!         table.metadata().default_partition_spec().as_ref().clone(),
//!         table.metadata().current_schema().clone(),
//!         Struct::from_iter(vec![Some(Literal::string("Seattle"))]),
//!     );
//!     let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
//!     let file_name_generator = DefaultFileNameGenerator::new(
//!         "test".to_string(),
//!         None,
//!         iceberg::spec::DataFileFormat::Parquet,
//!     );
//!
//!     // Create a parquet file writer builder. The parameter can get from table.
//!     let parquet_writer_builder = ParquetWriterBuilder::new(
//!         WriterProperties::default(),
//!         table.metadata().current_schema().clone(),
//!     );
//!
//!     // Create a rolling file writer
//!     let rolling_file_writer_builder = RollingFileWriterBuilder::new(
//!         parquet_writer_builder,
//!         512 * 1024 * 1024,
//!         table.file_io().clone(),
//!         location_generator.clone(),
//!         file_name_generator.clone(),
//!     );
//!
//!     // Create a data file writer builder using rolling file writer.
//!     let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
//!     // Create latency record writer using data file writer builder.
//!     let latency_record_builder = LatencyRecordWriterBuilder::new(data_file_writer_builder);
//!     // Build the final writer
//!     let mut latency_record_data_file_writer = latency_record_builder
//!         .build(Some(partition_key))
//!         .await
//!         .unwrap();
//!
//!     Ok(())
//! }
//! ```
//!
//! # Adding Partitioning to Data File Writers
//!
//! You can wrap a `DataFileWriter` with partitioning writers to handle partitioned tables.
//! Iceberg provides two partitioning strategies:
//!
//! ## FanoutWriter - For Unsorted Data
//!
//! Wraps the data file writer to handle unsorted data by maintaining multiple active writers.
//! Use this when your data is not pre-sorted by partition key. Writes to different partitions
//! can happen in any order, even interleaved.
//!
//! ```rust, no_run
//! # // Same setup as the simple example above...
//! # use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
//! # use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
//! # use iceberg::{Catalog, CatalogBuilder, Result, TableIdent};
//! # use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//! # use iceberg::writer::file_writer::ParquetWriterBuilder;
//! # use iceberg::writer::file_writer::location_generator::{
//! #     DefaultFileNameGenerator, DefaultLocationGenerator,
//! # };
//! # use parquet::file::properties::WriterProperties;
//! # use std::collections::HashMap;
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! # let catalog = MemoryCatalogBuilder::default()
//! #     .load("memory", HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), "file:///path/to/warehouse".to_string())]))
//! #     .await?;
//! # let table = catalog.load_table(&TableIdent::from_strs(["hello", "world"])?).await?;
//! # let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
//! # let file_name_generator = DefaultFileNameGenerator::new("test".to_string(), None, iceberg::spec::DataFileFormat::Parquet);
//! # let parquet_writer_builder = ParquetWriterBuilder::new(WriterProperties::default(), table.metadata().current_schema().clone());
//! # let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
//! #     parquet_writer_builder, table.file_io().clone(), location_generator, file_name_generator);
//! # let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
//!
//! // Wrap the data file writer with FanoutWriter for partitioning
//! use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
//! use iceberg::writer::partitioning::PartitioningWriter;
//! use iceberg::spec::{Literal, PartitionKey, Struct};
//!
//! let mut fanout_writer = FanoutWriter::new(data_file_writer_builder);
//!
//! // Create partition keys for different regions
//! let schema = table.metadata().current_schema().clone();
//! let partition_spec = table.metadata().default_partition_spec().as_ref().clone();
//!
//! let partition_key_us = PartitionKey::new(
//!     partition_spec.clone(),
//!     schema.clone(),
//!     Struct::from_iter([Some(Literal::string("US"))]),
//! );
//!
//! let partition_key_eu = PartitionKey::new(
//!     partition_spec.clone(),
//!     schema.clone(),
//!     Struct::from_iter([Some(Literal::string("EU"))]),
//! );
//!
//! // Write to different partitions in any order - can interleave partition writes
//! // fanout_writer.write(partition_key_us.clone(), batch_us1).await?;
//! // fanout_writer.write(partition_key_eu.clone(), batch_eu1).await?;
//! // fanout_writer.write(partition_key_us.clone(), batch_us2).await?; // Back to US - OK!
//! // fanout_writer.write(partition_key_eu.clone(), batch_eu2).await?; // Back to EU - OK!
//!
//! let data_files = fanout_writer.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## ClusteredWriter - For Sorted Data
//!
//! Wraps the data file writer for pre-sorted data. More memory efficient as it maintains
//! only one active writer at a time, but requires input sorted by partition key.
//!
//! ```rust, no_run
//! # // Same setup as the simple example above...
//! # use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
//! # use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
//! # use iceberg::{Catalog, CatalogBuilder, Result, TableIdent};
//! # use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
//! # use iceberg::writer::file_writer::ParquetWriterBuilder;
//! # use iceberg::writer::file_writer::location_generator::{
//! #     DefaultFileNameGenerator, DefaultLocationGenerator,
//! # };
//! # use parquet::file::properties::WriterProperties;
//! # use std::collections::HashMap;
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! # let catalog = MemoryCatalogBuilder::default()
//! #     .load("memory", HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), "file:///path/to/warehouse".to_string())]))
//! #     .await?;
//! # let table = catalog.load_table(&TableIdent::from_strs(["hello", "world"])?).await?;
//! # let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
//! # let file_name_generator = DefaultFileNameGenerator::new("test".to_string(), None, iceberg::spec::DataFileFormat::Parquet);
//! # let parquet_writer_builder = ParquetWriterBuilder::new(WriterProperties::default(), table.metadata().current_schema().clone());
//! # let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
//! #     parquet_writer_builder, table.file_io().clone(), location_generator, file_name_generator);
//! # let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);
//!
//! // Wrap the data file writer with ClusteredWriter for sorted partitioning
//! use iceberg::writer::partitioning::clustered_writer::ClusteredWriter;
//! use iceberg::writer::partitioning::PartitioningWriter;
//! use iceberg::spec::{Literal, PartitionKey, Struct};
//!
//! let mut clustered_writer = ClusteredWriter::new(data_file_writer_builder);
//!
//! // Create partition keys (must write in sorted order)
//! let schema = table.metadata().current_schema().clone();
//! let partition_spec = table.metadata().default_partition_spec().as_ref().clone();
//!
//! let partition_key_asia = PartitionKey::new(
//!     partition_spec.clone(),
//!     schema.clone(),
//!     Struct::from_iter([Some(Literal::string("ASIA"))]),
//! );
//!
//! let partition_key_eu = PartitionKey::new(
//!     partition_spec.clone(),
//!     schema.clone(),
//!     Struct::from_iter([Some(Literal::string("EU"))]),
//! );
//!
//! let partition_key_us = PartitionKey::new(
//!     partition_spec.clone(),
//!     schema.clone(),
//!     Struct::from_iter([Some(Literal::string("US"))]),
//! );
//!
//! // Write to partitions in sorted order (ASIA -> EU -> US)
//! // clustered_writer.write(partition_key_asia, batch_asia).await?;
//! // clustered_writer.write(partition_key_eu, batch_eu).await?;
//! // clustered_writer.write(partition_key_us, batch_us).await?;
//! // Writing back to ASIA would fail since data must be sorted!
//!
//! let data_files = clustered_writer.close().await?;
//!
//!     Ok(())
//! }
//! ```

pub mod base_writer;
pub mod file_writer;
pub mod partitioning;

use arrow_array::RecordBatch;

use crate::Result;
use crate::spec::{DataFile, PartitionKey};

type DefaultInput = RecordBatch;
type DefaultOutput = Vec<DataFile>;

/// The builder for iceberg writer.
#[async_trait::async_trait]
pub trait IcebergWriterBuilder<I = DefaultInput, O = DefaultOutput>: Send + Sync + 'static {
    /// The associated writer type.
    type R: IcebergWriter<I, O>;
    /// Build the iceberg writer with an optional partition key.
    async fn build(&self, partition_key: Option<PartitionKey>) -> Result<Self::R>;
}

/// The iceberg writer used to write data to iceberg table.
#[async_trait::async_trait]
pub trait IcebergWriter<I = DefaultInput, O = DefaultOutput>: Send + 'static {
    /// Write data to iceberg table.
    async fn write(&mut self, input: I) -> Result<()>;
    /// Close the writer and return the written data files.
    /// If close failed, the data written before maybe be lost. User may need to recreate the writer and rewrite the data again.
    /// # NOTE
    /// After close, regardless of success or failure, the writer should never be used again, otherwise the writer will panic.
    async fn close(&mut self) -> Result<O>;
}

/// The current file status of the Iceberg writer.
/// This is implemented for writers that write a single file at a time.
pub trait CurrentFileStatus {
    /// Get the current file path.
    fn current_file_path(&self) -> String;
    /// Get the current file row number.
    fn current_row_num(&self) -> usize;
    /// Get the current file written size.
    fn current_written_size(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use arrow_select::concat::concat_batches;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::IcebergWriter;
    use crate::io::FileIO;
    use crate::spec::{DataFile, DataFileFormat};

    // This function is used to guarantee the trait can be used as an object safe trait.
    async fn _guarantee_object_safe(mut w: Box<dyn IcebergWriter>) {
        let _ = w
            .write(RecordBatch::new_empty(Schema::empty().into()))
            .await;
        let _ = w.close().await;
    }

    // This function check:
    // The data of the written parquet file is correct.
    // The metadata of the data file is consistent with the written parquet file.
    pub(crate) async fn check_parquet_data_file(
        file_io: &FileIO,
        data_file: &DataFile,
        batch: &RecordBatch,
    ) {
        assert_eq!(data_file.file_format, DataFileFormat::Parquet);

        let input_file = file_io.new_input(data_file.file_path.clone()).unwrap();
        // read the written file
        let input_content = input_file.read().await.unwrap();
        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(input_content.clone()).unwrap();

        // check data
        let reader = reader_builder.build().unwrap();
        let batches = reader.map(|batch| batch.unwrap()).collect::<Vec<_>>();
        let res = concat_batches(&batch.schema(), &batches).unwrap();
        assert_eq!(*batch, res);
    }
}
