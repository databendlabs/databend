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

use std::collections::HashMap;

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};

static REST_URI: &str = "http://localhost:8181";
static NAMESPACE: &str = "default";
static TABLE_NAME: &str = "t1";

/// This is a simple example that demonstrates how to use [`RestCatalog`] to create tables.
///
/// The demo creates a table creates a table and then later retrieves the same table.
///
/// A running instance of the iceberg-rest catalog on port 8181 is required. You can find how to run
/// the iceberg-rest catalog with `docker compose` in the official
/// [quickstart documentation](https://iceberg.apache.org/spark-quickstart/).
#[tokio::main]
async fn main() {
    // Create the REST iceberg catalog.
    let catalog = RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), REST_URI.to_string())]),
        )
        .await
        .unwrap();

    // ANCHOR: create_table
    // Create the table identifier.
    let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), TABLE_NAME.to_string());

    // You can also use the `from_strs` method on `TableIdent` to create the table identifier.
    // let table_ident = TableIdent::from_strs([NAMESPACE, TABLE_NAME]).unwrap();

    // Drop the table if it already exists.
    if catalog.table_exists(&table_ident).await.unwrap() {
        println!("Table {TABLE_NAME} already exists, dropping now.");
        catalog.drop_table(&table_ident).await.unwrap();
    }

    // Build the table schema.
    let table_schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .build()
        .unwrap();

    // Build the table creation parameters.
    let table_creation = TableCreation::builder()
        .name(table_ident.name.clone())
        .schema(table_schema.clone())
        .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
        .build();

    // Create the table.
    let _created_table = catalog
        .create_table(&table_ident.namespace, table_creation)
        .await
        .unwrap();
    println!("Table {TABLE_NAME} created!");
    // ANCHOR_END: create_table

    // ANCHOR: load_table
    // Ensure that the table is under the correct namespace.
    assert!(
        catalog
            .list_tables(&namespace_ident)
            .await
            .unwrap()
            .contains(&table_ident)
    );

    // Load the table back from the catalog. It should be identical to the created table.
    let loaded_table = catalog.load_table(&table_ident).await.unwrap();
    println!("Table {TABLE_NAME} loaded!\n\nTable: {loaded_table:?}");
    // ANCHOR_END: load_table
}
