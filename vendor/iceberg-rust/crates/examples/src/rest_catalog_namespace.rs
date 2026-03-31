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

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};

static REST_URI: &str = "http://localhost:8181";

/// This is a simple example that demonstrates how to use [`RestCatalog`] to create namespaces.
///
/// The demo creates a namespace and prints it out.
///
/// A running instance of the iceberg-rest catalog on port 8181 is required. You can find how to run
/// the iceberg-rest catalog with `docker compose` in the official
/// [quickstart documentation](https://iceberg.apache.org/spark-quickstart/).
#[tokio::main]
async fn main() {
    // ANCHOR: create_catalog
    // Create the REST iceberg catalog.
    let catalog = RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([(REST_CATALOG_PROP_URI.to_string(), REST_URI.to_string())]),
        )
        .await
        .unwrap();
    // ANCHOR_END: create_catalog

    // ANCHOR: list_all_namespace
    // List all namespaces already in the catalog.
    let existing_namespaces = catalog.list_namespaces(None).await.unwrap();
    println!("Namespaces alreading in the existing catalog: {existing_namespaces:?}");
    // ANCHOR_END: list_all_namespace

    // ANCHOR: create_namespace
    // Create a new namespace identifier.
    let namespace_ident =
        NamespaceIdent::from_vec(vec!["ns1".to_string(), "ns11".to_string()]).unwrap();

    // Drop the namespace if it already exists.
    if catalog.namespace_exists(&namespace_ident).await.unwrap() {
        println!("Namespace already exists, dropping now.",);
        catalog.drop_namespace(&namespace_ident).await.unwrap();
    }

    // Create the new namespace in the catalog.
    let _created_namespace = catalog
        .create_namespace(
            &namespace_ident,
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        )
        .await
        .unwrap();
    println!("Namespace {namespace_ident:?} created!");

    let loaded_namespace = catalog.get_namespace(&namespace_ident).await.unwrap();
    println!("Namespace loaded!\n\nNamespace: {loaded_namespace:#?}",);
    // ANCHOR_END: create_namespace
}
