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

use futures::stream::StreamExt;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalogBuilder};

// Configure these values according to your environment

static REST_URI: &str = "http://127.0.0.1:8181";
static NAMESPACE: &str = "default";
static TABLE_NAME: &str = "t1";
static OSS_ENDPOINT: &str = "https://oss-cn-hangzhou.aliyuncs.com/bucket";
static OSS_ACCESS_KEY_ID: &str = "LTAI5t999999999999999";
static OSS_ACCESS_KEY_SECRET: &str = "99999999999999999999999999999999";

/// This is a simple example that demonstrates how to use [`RestCatalog`] to read data from OSS.
///
/// The demo reads data from an existing table in OSS storage.
///
/// A running instance of the iceberg-rest catalog on port 8181 is required. You can find how to run
/// the iceberg-rest catalog with `docker compose` in the official
/// [quickstart documentation](https://iceberg.apache.org/spark-quickstart/).
///
/// The example also requires valid OSS credentials and endpoint to be configured.
#[tokio::main]
async fn main() {
    // Create the REST iceberg catalog.
    let catalog = RestCatalogBuilder::default()
        .load(
            "rest",
            HashMap::from([
                (REST_CATALOG_PROP_URI.to_string(), REST_URI.to_string()),
                (
                    iceberg::io::OSS_ENDPOINT.to_string(),
                    OSS_ENDPOINT.to_string(),
                ),
                (
                    iceberg::io::OSS_ACCESS_KEY_ID.to_string(),
                    OSS_ACCESS_KEY_ID.to_string(),
                ),
                (
                    iceberg::io::OSS_ACCESS_KEY_SECRET.to_string(),
                    OSS_ACCESS_KEY_SECRET.to_string(),
                ),
            ]),
        )
        .await
        .unwrap();

    // Create the table identifier.
    let namespace_ident = NamespaceIdent::from_vec(vec![NAMESPACE.to_string()]).unwrap();
    let table_ident = TableIdent::new(namespace_ident.clone(), TABLE_NAME.to_string());

    // Check if the table exists.
    if !catalog.table_exists(&table_ident).await.unwrap() {
        println!("Table {TABLE_NAME} must exists.");
        return;
    }

    let table = catalog.load_table(&table_ident).await.unwrap();
    println!("Table {TABLE_NAME} loaded!");

    let scan = table.scan().select_all().build().unwrap();
    let reader = scan.to_arrow().await.unwrap();
    let buf = reader.collect::<Vec<_>>().await;

    println!("Table {TABLE_NAME} has {} batches.", buf.len());

    assert!(!buf.is_empty());
    assert!(buf.iter().all(|x| x.is_ok()));
}
