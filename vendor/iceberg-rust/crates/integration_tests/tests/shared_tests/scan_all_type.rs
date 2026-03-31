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

//! Integration tests for rest catalog.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, MapArray, RecordBatch,
    StringArray, StructArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Fields};
use futures::TryStreamExt;
use iceberg::arrow::{DEFAULT_MAP_FIELD_NAME, UTC_TIME_ZONE};
use iceberg::spec::{
    LIST_FIELD_NAME, ListType, MAP_KEY_FIELD_NAME, MAP_VALUE_FIELD_NAME, MapType, NestedField,
    PrimitiveType, Schema, StructType, Type,
};
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
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::get_shared_containers;
use crate::shared_tests::random_ns;

#[tokio::test]
async fn test_scan_all_type() {
    let fixture = get_shared_containers();
    let rest_catalog = RestCatalogBuilder::default()
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();
    let ns = random_ns().await;

    let schema = Schema::builder()
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .with_fields(vec![
            // test all type
            NestedField::required(1, "int", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "long", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(3, "float", Type::Primitive(PrimitiveType::Float)).into(),
            NestedField::required(4, "double", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::required(
                5,
                "decimal",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 20,
                    scale: 5,
                }),
            )
            .into(),
            NestedField::required(6, "string", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(7, "boolean", Type::Primitive(PrimitiveType::Boolean)).into(),
            NestedField::required(8, "binary", Type::Primitive(PrimitiveType::Binary)).into(),
            NestedField::required(9, "date", Type::Primitive(PrimitiveType::Date)).into(),
            NestedField::required(10, "time", Type::Primitive(PrimitiveType::Time)).into(),
            NestedField::required(11, "timestamp", Type::Primitive(PrimitiveType::Timestamp))
                .into(),
            NestedField::required(12, "fixed", Type::Primitive(PrimitiveType::Fixed(10))).into(),
            NestedField::required(13, "uuid", Type::Primitive(PrimitiveType::Uuid)).into(),
            NestedField::required(
                14,
                "timestamptz",
                Type::Primitive(PrimitiveType::Timestamptz),
            )
            .into(),
            NestedField::required(
                15,
                "struct",
                Type::Struct(StructType::new(vec![
                    NestedField::required(18, "int", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::required(19, "string", Type::Primitive(PrimitiveType::String))
                        .into(),
                ])),
            )
            .into(),
            NestedField::required(
                16,
                "list",
                Type::List(ListType::new(
                    NestedField::list_element(20, Type::Primitive(PrimitiveType::Int), true).into(),
                )),
            )
            .into(),
            NestedField::required(
                17,
                "map",
                Type::Map(MapType::new(
                    NestedField::map_key_element(21, Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::map_value_element(
                        22,
                        Type::Primitive(PrimitiveType::String),
                        true,
                    )
                    .into(),
                )),
            )
            .into(),
        ])
        .build()
        .unwrap();

    let table_creation = TableCreation::builder()
        .name("t1".to_string())
        .schema(schema.clone())
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
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator.clone(),
        file_name_generator.clone(),
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await.unwrap();

    // Prepare data
    let col1 = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let col2 = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let col3 = Float32Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
    let col4 = Float64Array::from(vec![1.1, 2.2, 3.3, 4.4, 5.5]);
    let col5 = Decimal128Array::from(vec![
        Some(1.into()),
        Some(2.into()),
        Some(3.into()),
        Some(4.into()),
        Some(5.into()),
    ])
    .with_data_type(DataType::Decimal128(20, 5));
    let col6 = StringArray::from(vec!["a", "b", "c", "d", "e"]);
    let col7 = BooleanArray::from(vec![true, false, true, false, true]);
    let col8 = LargeBinaryArray::from_opt_vec(vec![
        Some(b"a"),
        Some(b"b"),
        Some(b"c"),
        Some(b"d"),
        Some(b"e"),
    ]);
    let col9 = Date32Array::from(vec![1, 2, 3, 4, 5]);
    let col10 = Time64MicrosecondArray::from(vec![1, 2, 3, 4, 5]);
    let col11 = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]);
    let col12 = FixedSizeBinaryArray::try_from_iter(
        vec![
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(col12.data_type(), &DataType::FixedSizeBinary(10));
    let col13 = FixedSizeBinaryArray::try_from_iter(
        vec![
            Uuid::new_v4().as_bytes().to_vec(),
            Uuid::new_v4().as_bytes().to_vec(),
            Uuid::new_v4().as_bytes().to_vec(),
            Uuid::new_v4().as_bytes().to_vec(),
            Uuid::new_v4().as_bytes().to_vec(),
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(col13.data_type(), &DataType::FixedSizeBinary(16));
    let col14 = TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5]).with_timezone(UTC_TIME_ZONE);
    let col15 = StructArray::from(vec![
        (
            Arc::new(
                Field::new("int", DataType::Int32, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    18.to_string(),
                )])),
            ),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
        ),
        (
            Arc::new(
                Field::new("string", DataType::Utf8, false).with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    19.to_string(),
                )])),
            ),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
        ),
    ]);
    let col16 = {
        let mut builder = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(
            Field::new(LIST_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                20.to_string(),
            )])),
        ));
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.append_value([Some(1), Some(2), Some(3), Some(4), Some(5)]);
        builder.finish()
    };
    let col17 = {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(4);
        let mut builder = MapBuilder::new(None, int_builder, string_builder);
        builder.keys().append_value(1);
        builder.values().append_value("a");
        builder.append(true).unwrap();
        builder.keys().append_value(2);
        builder.values().append_value("b");
        builder.append(true).unwrap();
        builder.keys().append_value(3);
        builder.values().append_value("c");
        builder.append(true).unwrap();
        builder.keys().append_value(4);
        builder.values().append_value("d");
        builder.append(true).unwrap();
        builder.keys().append_value(5);
        builder.values().append_value("e");
        builder.append(true).unwrap();
        let array = builder.finish();
        let (_field, offsets, entries, nulls, ordered) = array.into_parts();
        let new_struct_fields = Fields::from(vec![
            Field::new(MAP_KEY_FIELD_NAME, DataType::Int32, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), 21.to_string()),
            ])),
            Field::new(MAP_VALUE_FIELD_NAME, DataType::Utf8, false).with_metadata(HashMap::from([
                (PARQUET_FIELD_ID_META_KEY.to_string(), 22.to_string()),
            ])),
        ]);
        let entries = {
            let (_, arrays, nulls) = entries.into_parts();
            StructArray::new(new_struct_fields.clone(), arrays, nulls)
        };
        let field = Arc::new(Field::new(
            DEFAULT_MAP_FIELD_NAME,
            DataType::Struct(new_struct_fields),
            false,
        ));
        MapArray::new(field, offsets, entries, nulls, ordered)
    };

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(col1) as ArrayRef,
        Arc::new(col2) as ArrayRef,
        Arc::new(col3) as ArrayRef,
        Arc::new(col4) as ArrayRef,
        Arc::new(col5) as ArrayRef,
        Arc::new(col6) as ArrayRef,
        Arc::new(col7) as ArrayRef,
        Arc::new(col8) as ArrayRef,
        Arc::new(col9) as ArrayRef,
        Arc::new(col10) as ArrayRef,
        Arc::new(col11) as ArrayRef,
        Arc::new(col12) as ArrayRef,
        Arc::new(col13) as ArrayRef,
        Arc::new(col14) as ArrayRef,
        Arc::new(col15) as ArrayRef,
        Arc::new(col16) as ArrayRef,
        Arc::new(col17) as ArrayRef,
    ])
    .unwrap();
    data_file_writer.write(batch.clone()).await.unwrap();
    let data_file = data_file_writer.close().await.unwrap();

    // commit result
    let tx = Transaction::new(&table);
    let append_action = tx.fast_append().add_data_files(data_file.clone());
    let tx = append_action.apply(tx).unwrap();
    let table = tx.commit(&rest_catalog).await.unwrap();

    // check result
    let batch_stream = table
        .scan()
        .select(vec![
            "int",
            "long",
            "float",
            "double",
            "decimal",
            "string",
            "boolean",
            "binary",
            "date",
            "time",
            "timestamp",
            "fixed",
            "uuid",
            "timestamptz",
            "struct",
            "list",
            "map",
        ])
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0], batch);

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
}
