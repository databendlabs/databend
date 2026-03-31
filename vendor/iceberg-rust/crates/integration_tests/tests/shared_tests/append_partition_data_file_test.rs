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

//! Integration test for partition data file

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::spec::{
    Literal, PartitionKey, PrimitiveLiteral, Struct, Transform, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableCreation};
use iceberg_catalog_rest::RestCatalogBuilder;
use parquet::file::properties::WriterProperties;

use crate::get_shared_containers;
use crate::shared_tests::{random_ns, test_schema};

#[tokio::test]
async fn test_append_partition_data_file() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;
    let schema = test_schema();

    let unbound_partition_spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "id", Transform::Identity)
        .expect("could not add partition field")
        .build();

    let partition_spec = unbound_partition_spec
        .bind(schema.clone())
        .expect("could not bind to schema");

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
        .partition_spec(partition_spec.clone())
        .build();

    let table = rest_catalog
        .create_table(ns.name(), table_creation)
        .await
        .unwrap();

    // Create the writer and write the data
    let schema: Arc<arrow_schema::Schema> = Arc::new(
        table
            .metadata()
            .current_schema()
            .as_ref()
            .try_into()
            .unwrap(),
    );

    let first_partition_id_value = 100;
    let partition_key = PartitionKey::new(
        partition_spec.clone(),
        table.metadata().current_schema().clone(),
        Struct::from_iter(vec![Some(Literal::int(first_partition_id_value))]),
    );

    let location_generator = DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(
        "test".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::default(),
        table.metadata().current_schema().clone(),
    );

    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder.clone(),
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );

    let mut data_file_writer_valid =
        DataFileWriterBuilder::new(rolling_file_writer_builder.clone())
            .build(Some(partition_key.clone()))
            .await
            .unwrap();

    let col1 = StringArray::from(vec![Some("foo1"), Some("foo2")]);
    let col2 = Int32Array::from(vec![
        Some(first_partition_id_value),
        Some(first_partition_id_value),
    ]);
    let col3 = BooleanArray::from(vec![Some(true), Some(false)]);
    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
    ])
    .unwrap();

    data_file_writer_valid.write(batch.clone()).await.unwrap();
    let data_file_valid = data_file_writer_valid.close().await.unwrap();

    // commit result
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file_valid.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // check result
    let batch_stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0], batch);

    let partition_key = partition_key.copy_with_data(Struct::from_iter([Some(
        Literal::Primitive(PrimitiveLiteral::Boolean(true)),
    )]));
    test_schema_incompatible_partition_type(
        rolling_file_writer_builder.clone(),
        batch.clone(),
        partition_key.clone(),
        table.clone(),
        &rest_catalog,
    )
    .await;

    let partition_key = partition_key.copy_with_data(Struct::from_iter([
        Some(Literal::Primitive(PrimitiveLiteral::Int(
            first_partition_id_value,
        ))),
        Some(Literal::Primitive(PrimitiveLiteral::Int(
            first_partition_id_value,
        ))),
    ]));
    test_schema_incompatible_partition_fields(
        rolling_file_writer_builder.clone(),
        batch,
        partition_key,
        table,
        &rest_catalog,
    )
    .await;
}

async fn test_schema_incompatible_partition_type(
    rolling_file_writer_builder: RollingFileWriterBuilder<
        ParquetWriterBuilder,
        DefaultLocationGenerator,
        DefaultFileNameGenerator,
    >,
    batch: RecordBatch,
    partition_key: PartitionKey,
    table: Table,
    catalog: &dyn Catalog,
) {
    // test writing different "type" of partition than mentioned in schema
    let mut data_file_writer_invalid = DataFileWriterBuilder::new(rolling_file_writer_builder)
        .build(Some(partition_key))
        .await
        .unwrap();

    data_file_writer_invalid.write(batch.clone()).await.unwrap();
    let data_file_invalid = data_file_writer_invalid.close().await.unwrap();

    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file_invalid.clone());
    let tx = append_action.apply(tx).unwrap();

    if tx.commit(catalog).await.is_ok() {
        panic!("diverging partition info should have returned error");
    }
}

async fn test_schema_incompatible_partition_fields(
    rolling_file_writer_builder: RollingFileWriterBuilder<
        ParquetWriterBuilder,
        DefaultLocationGenerator,
        DefaultFileNameGenerator,
    >,
    batch: RecordBatch,
    partition_key: PartitionKey,
    table: Table,
    catalog: &dyn Catalog,
) {
    // test writing different number of partition fields than mentioned in schema
    let mut data_file_writer_invalid = DataFileWriterBuilder::new(rolling_file_writer_builder)
        .build(Some(partition_key))
        .await
        .unwrap();

    data_file_writer_invalid.write(batch.clone()).await.unwrap();
    let data_file_invalid = data_file_writer_invalid.close().await.unwrap();

    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file_invalid.clone());
    let tx = append_action.apply(tx).unwrap();
    if tx.commit(catalog).await.is_ok() {
        panic!("passing different number of partition fields should have returned error");
    }
}
