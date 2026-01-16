// Copyright 2025 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tests for the refactored DatabaseCatalog error handling methods
//!
//! These tests verify that the refactored methods using ErrorCodeResultExt
//! maintain the same behavior as before while providing cleaner code.

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_query::catalogs::Catalog;

use crate::tests::create_catalog;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_database_error_handling() -> Result<()> {
    let tenant = Tenant::new_literal("test");
    let catalog = create_catalog().await?;

    // Test successful database retrieval
    let db = catalog.get_database(&tenant, "system").await?;
    assert_eq!(db.name(), "system");

    // Test error propagation for non-existent database
    let result = catalog.get_database(&tenant, "nonexistent_db").await;
    assert!(result.is_err());

    if let Err(err) = result {
        assert_eq!(err.code(), ErrorCode::UNKNOWN_DATABASE);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_table_error_handling() -> Result<()> {
    let tenant = Tenant::new_literal("test");
    let catalog = create_catalog().await?;

    // Create a test database and table
    let req = CreateDatabaseReq {
        create_option: CreateOption::Create,
        catalog_name: None,
        name_ident: DatabaseNameIdent::new(&tenant, "test_db"),
        meta: DatabaseMeta::default(),
    };
    catalog.create_database(req).await?;

    let schema = Arc::new(TableSchema::new(vec![TableField::new(
        "id",
        TableDataType::Number(NumberDataType::UInt64),
    )]));

    let table_req = CreateTableReq {
        create_option: CreateOption::Create,
        catalog_name: None,
        name_ident: TableNameIdent {
            tenant: tenant.clone(),
            db_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
        },
        table_meta: TableMeta {
            schema,
            engine: "MEMORY".to_string(),
            ..TableMeta::default()
        },
        as_dropped: false,
        table_properties: None,
        table_partition: None,
    };
    catalog.create_table(table_req).await?;

    // Test successful table retrieval
    let table = catalog.get_table(&tenant, "test_db", "test_table").await?;
    assert_eq!(table.name(), "test_table");

    // Test error propagation for non-existent database
    let result = catalog
        .get_table(&tenant, "nonexistent_db", "test_table")
        .await;
    assert!(result.is_err());
    if let Err(err) = result {
        assert_eq!(err.code(), ErrorCode::UNKNOWN_DATABASE);
    }

    // Test error propagation for non-existent table in existing database
    let result = catalog
        .get_table(&tenant, "test_db", "nonexistent_table")
        .await;
    assert!(result.is_err());
    if let Err(err) = result {
        assert_eq!(err.code(), ErrorCode::UNKNOWN_TABLE);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_table_history_error_handling() -> Result<()> {
    let tenant = Tenant::new_literal("test");
    let catalog = create_catalog().await?;

    // Create a test database
    let req = CreateDatabaseReq {
        create_option: CreateOption::Create,
        catalog_name: None,
        name_ident: DatabaseNameIdent::new(&tenant, "history_test_db"),
        meta: DatabaseMeta::default(),
    };
    catalog.create_database(req).await?;

    // Test error propagation for non-existent database
    let result = catalog
        .get_table_history(&tenant, "nonexistent_db", "test_table")
        .await;
    assert!(result.is_err());
    if let Err(err) = result {
        assert_eq!(err.code(), ErrorCode::UNKNOWN_DATABASE);
    }

    // Test that non-existent table in existing database returns empty history
    let result = catalog
        .get_table_history(&tenant, "history_test_db", "nonexistent_table")
        .await;
    assert!(result.is_ok());
    let tables = result.unwrap();
    assert!(
        tables.is_empty(),
        "History for non-existent table should be empty"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_list_tables_error_handling() -> Result<()> {
    let tenant = Tenant::new_literal("test");
    let catalog = create_catalog().await?;

    // Test successful table listing for existing database
    let tables = catalog.list_tables(&tenant, "system").await?;
    assert!(!tables.is_empty()); // system database should have tables

    // Test error propagation for non-existent database
    let result = catalog.list_tables(&tenant, "nonexistent_db").await;
    assert!(result.is_err());
    if let Err(err) = result {
        assert_eq!(err.code(), ErrorCode::UNKNOWN_DATABASE);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_list_tables_history_error_handling() -> Result<()> {
    let tenant = Tenant::new_literal("test");
    let catalog = create_catalog().await?;

    // Test error propagation for non-existent database
    let result = catalog.list_tables_history(&tenant, "nonexistent_db").await;
    assert!(result.is_err());
    if let Err(err) = result {
        assert_eq!(err.code(), ErrorCode::UNKNOWN_DATABASE);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_table_by_info_error_handling() -> Result<()> {
    let tenant = Tenant::new_literal("test");
    let catalog = create_catalog().await?;

    // Create a test database and table
    let req = CreateDatabaseReq {
        create_option: CreateOption::Create,
        catalog_name: None,
        name_ident: DatabaseNameIdent::new(&tenant, "info_test_db"),
        meta: DatabaseMeta::default(),
    };
    catalog.create_database(req).await?;

    let schema = Arc::new(TableSchema::new(vec![TableField::new(
        "id",
        TableDataType::Number(NumberDataType::UInt64),
    )]));

    let table_req = CreateTableReq {
        create_option: CreateOption::Create,
        catalog_name: None,
        name_ident: TableNameIdent {
            tenant: tenant.clone(),
            db_name: "info_test_db".to_string(),
            table_name: "info_test_table".to_string(),
        },
        table_meta: TableMeta {
            schema,
            engine: "MEMORY".to_string(),
            ..TableMeta::default()
        },
        as_dropped: false,
        table_properties: None,
        table_partition: None,
    };
    catalog.create_table(table_req).await?;

    // Test successful table retrieval by info
    let table = catalog
        .get_table(&tenant, "info_test_db", "info_test_table")
        .await?;
    let table_info = table.get_table_info();
    let table_by_info = catalog.get_table_by_info(table_info)?;
    assert_eq!(table_by_info.name(), "info_test_table");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_delegation_between_catalogs() -> Result<()> {
    let tenant = Tenant::new_literal("test");
    let catalog = create_catalog().await?;

    // Test that system databases are handled by immutable catalog
    let system_db = catalog.get_database(&tenant, "system").await?;
    assert_eq!(system_db.name(), "system");

    // Test that system tables are handled by immutable catalog
    let system_tables = catalog.list_tables(&tenant, "system").await?;
    assert!(!system_tables.is_empty());

    // Test that user-created databases are handled by mutable catalog
    let req = CreateDatabaseReq {
        create_option: CreateOption::Create,
        catalog_name: None,
        name_ident: DatabaseNameIdent::new(&tenant, "user_db"),
        meta: DatabaseMeta::default(),
    };
    catalog.create_database(req).await?;

    let user_db = catalog.get_database(&tenant, "user_db").await?;
    assert_eq!(user_db.name(), "user_db");

    // Test that tables in user databases are handled correctly
    let user_tables = catalog.list_tables(&tenant, "user_db").await?;
    assert!(user_tables.is_empty()); // Should be empty initially

    Ok(())
}
