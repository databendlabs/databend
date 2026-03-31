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

//! Integration tests for hms catalog.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;

use ctor::{ctor, dtor};
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_hms::{
    HMS_CATALOG_PROP_THRIFT_TRANSPORT, HMS_CATALOG_PROP_URI, HMS_CATALOG_PROP_WAREHOUSE,
    HmsCatalog, HmsCatalogBuilder, THRIFT_TRANSPORT_BUFFERED,
};
use iceberg_test_utils::docker::DockerCompose;
use iceberg_test_utils::{normalize_test_name, set_up};
use port_scanner::scan_port_addr;
use tokio::time::sleep;
use tracing::info;

const HMS_CATALOG_PORT: u16 = 9083;
const MINIO_PORT: u16 = 9000;
static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);
type Result<T> = std::result::Result<T, iceberg::Error>;

#[ctor]
fn before_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    let docker_compose = DockerCompose::new(
        normalize_test_name(module_path!()),
        format!("{}/testdata/hms_catalog", env!("CARGO_MANIFEST_DIR")),
    );
    docker_compose.up();
    guard.replace(docker_compose);
}

#[dtor]
fn after_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    guard.take();
}

async fn get_catalog() -> HmsCatalog {
    set_up();

    let (hms_catalog_ip, minio_ip) = {
        let guard = DOCKER_COMPOSE_ENV.read().unwrap();
        let docker_compose = guard.as_ref().unwrap();
        (
            docker_compose.get_container_ip("hive-metastore"),
            docker_compose.get_container_ip("minio"),
        )
    };
    let hms_socket_addr = SocketAddr::new(hms_catalog_ip, HMS_CATALOG_PORT);
    let minio_socket_addr = SocketAddr::new(minio_ip, MINIO_PORT);
    while !scan_port_addr(hms_socket_addr) {
        info!("scan hms_socket_addr {} check", hms_socket_addr);
        info!("Waiting for 1s hms catalog to ready...");
        sleep(std::time::Duration::from_millis(1000)).await;
    }

    while !scan_port_addr(minio_socket_addr) {
        info!("Waiting for 1s minio to ready...");
        sleep(std::time::Duration::from_millis(1000)).await;
    }

    let props = HashMap::from([
        (
            HMS_CATALOG_PROP_URI.to_string(),
            hms_socket_addr.to_string(),
        ),
        (
            HMS_CATALOG_PROP_THRIFT_TRANSPORT.to_string(),
            THRIFT_TRANSPORT_BUFFERED.to_string(),
        ),
        (
            HMS_CATALOG_PROP_WAREHOUSE.to_string(),
            "s3a://warehouse/hive".to_string(),
        ),
        (
            S3_ENDPOINT.to_string(),
            format!("http://{minio_socket_addr}"),
        ),
        (S3_ACCESS_KEY_ID.to_string(), "admin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "password".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
    ]);

    // Wait for bucket to actually exist
    let file_io = iceberg::io::FileIO::from_path("s3a://")
        .unwrap()
        .with_props(props.clone())
        .build()
        .unwrap();

    let mut retries = 0;
    while retries < 30 {
        if file_io.exists("s3a://warehouse/").await.unwrap_or(false) {
            info!("S3 bucket 'warehouse' is ready");
            break;
        }
        info!("Waiting for bucket creation... (attempt {})", retries + 1);
        sleep(std::time::Duration::from_millis(1000)).await;
        retries += 1;
    }

    HmsCatalogBuilder::default()
        .load("hms", props)
        .await
        .unwrap()
}

async fn set_test_namespace(catalog: &HmsCatalog, namespace: &NamespaceIdent) -> Result<()> {
    let properties = HashMap::new();

    catalog.create_namespace(namespace, properties).await?;

    Ok(())
}

fn set_table_creation(location: Option<String>, name: impl ToString) -> Result<TableCreation> {
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "foo", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "bar", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let builder = TableCreation::builder()
        .name(name.to_string())
        .properties(HashMap::new())
        .location_opt(location)
        .schema(schema);

    Ok(builder.build())
}

#[tokio::test]
async fn test_rename_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation: TableCreation = set_table_creation(None, "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_rename_table".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let table: iceberg::table::Table = catalog.create_table(namespace.name(), creation).await?;

    let dest = TableIdent::new(namespace.name().clone(), "my_table_rename".to_string());

    catalog.rename_table(table.identifier(), &dest).await?;

    let result = catalog.table_exists(&dest).await?;

    assert!(result);

    Ok(())
}

#[tokio::test]
async fn test_table_exists() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation(None, "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_table_exists".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let table = catalog.create_table(namespace.name(), creation).await?;

    let result = catalog.table_exists(table.identifier()).await?;

    assert!(result);

    Ok(())
}

#[tokio::test]
async fn test_drop_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation(None, "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_drop_table".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let table = catalog.create_table(namespace.name(), creation).await?;

    catalog.drop_table(table.identifier()).await?;

    let result = catalog.table_exists(table.identifier()).await?;

    assert!(!result);

    Ok(())
}

#[tokio::test]
async fn test_load_table() -> Result<()> {
    let catalog = get_catalog().await;
    let creation = set_table_creation(None, "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_load_table".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let expected = catalog.create_table(namespace.name(), creation).await?;

    let result = catalog
        .load_table(&TableIdent::new(
            namespace.name().clone(),
            "my_table".to_string(),
        ))
        .await?;

    assert_eq!(result.identifier(), expected.identifier());
    assert_eq!(result.metadata_location(), expected.metadata_location());
    assert_eq!(result.metadata(), expected.metadata());

    Ok(())
}

#[tokio::test]
async fn test_create_table() -> Result<()> {
    let catalog = get_catalog().await;
    // inject custom location, ignore the namespace prefix
    let creation = set_table_creation(Some("s3a://warehouse/hive".into()), "my_table")?;
    let namespace = Namespace::new(NamespaceIdent::new("test_create_table".into()));
    set_test_namespace(&catalog, namespace.name()).await?;

    let result = catalog.create_table(namespace.name(), creation).await?;

    assert_eq!(result.identifier().name(), "my_table");
    assert!(
        result
            .metadata_location()
            .is_some_and(|location| location.starts_with("s3a://warehouse/hive/metadata/00000-"))
    );
    assert!(
        catalog
            .file_io()
            .exists("s3a://warehouse/hive/metadata/")
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn test_list_tables() -> Result<()> {
    let catalog = get_catalog().await;
    let ns = Namespace::new(NamespaceIdent::new("test_list_tables".into()));
    let result = catalog.list_tables(ns.name()).await?;
    set_test_namespace(&catalog, ns.name()).await?;

    assert_eq!(result, vec![]);

    let creation = set_table_creation(None, "my_table")?;
    catalog.create_table(ns.name(), creation).await?;
    let result = catalog.list_tables(ns.name()).await?;

    assert_eq!(result, vec![TableIdent::new(
        ns.name().clone(),
        "my_table".to_string()
    )]);

    Ok(())
}

#[tokio::test]
async fn test_list_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let result_no_parent = catalog.list_namespaces(None).await?;

    let result_with_parent = catalog
        .list_namespaces(Some(&NamespaceIdent::new("parent".into())))
        .await?;

    assert!(result_no_parent.contains(&NamespaceIdent::new("default".into())));
    assert!(result_with_parent.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_create_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let properties = HashMap::from([
        ("comment".to_string(), "my_description".to_string()),
        ("location".to_string(), "my_location".to_string()),
        (
            "hive.metastore.database.owner".to_string(),
            "apache".to_string(),
        ),
        (
            "hive.metastore.database.owner-type".to_string(),
            "user".to_string(),
        ),
        ("key1".to_string(), "value1".to_string()),
    ]);

    let ns = Namespace::with_properties(
        NamespaceIdent::new("test_create_namespace".into()),
        properties.clone(),
    );

    let result = catalog.create_namespace(ns.name(), properties).await?;

    assert_eq!(result, ns);

    Ok(())
}

#[tokio::test]
async fn test_get_default_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let ns = Namespace::new(NamespaceIdent::new("default".into()));
    let properties = HashMap::from([
        ("location".to_string(), "s3a://warehouse/hive".to_string()),
        (
            "hive.metastore.database.owner-type".to_string(),
            "Role".to_string(),
        ),
        ("comment".to_string(), "Default Hive database".to_string()),
        (
            "hive.metastore.database.owner".to_string(),
            "public".to_string(),
        ),
    ]);

    let expected = Namespace::with_properties(NamespaceIdent::new("default".into()), properties);

    let result = catalog.get_namespace(ns.name()).await?;

    assert_eq!(expected, result);

    Ok(())
}

#[tokio::test]
async fn test_namespace_exists() -> Result<()> {
    let catalog = get_catalog().await;

    let ns_exists = Namespace::new(NamespaceIdent::new("default".into()));
    let ns_not_exists = Namespace::new(NamespaceIdent::new("test_namespace_exists".into()));

    let result_exists = catalog.namespace_exists(ns_exists.name()).await?;
    let result_not_exists = catalog.namespace_exists(ns_not_exists.name()).await?;

    assert!(result_exists);
    assert!(!result_not_exists);

    Ok(())
}

#[tokio::test]
async fn test_update_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let ns = NamespaceIdent::new("test_update_namespace".into());
    set_test_namespace(&catalog, &ns).await?;
    let properties = HashMap::from([("comment".to_string(), "my_update".to_string())]);

    catalog.update_namespace(&ns, properties).await?;

    let db = catalog.get_namespace(&ns).await?;

    assert_eq!(
        db.properties().get("comment"),
        Some(&"my_update".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn test_drop_namespace() -> Result<()> {
    let catalog = get_catalog().await;

    let ns = Namespace::new(NamespaceIdent::new("delete_me".into()));

    catalog.create_namespace(ns.name(), HashMap::new()).await?;

    let result = catalog.namespace_exists(ns.name()).await?;
    assert!(result);

    catalog.drop_namespace(ns.name()).await?;

    let result = catalog.namespace_exists(ns.name()).await?;
    assert!(!result);

    Ok(())
}
