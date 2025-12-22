// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use databend_common_ast::ast::UriLocation;
use databend_common_base::runtime::CaptureLogSettings;
use databend_common_base::runtime::ThreadTracker;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::Planner;
use databend_common_sql::binder::parse_uri_location;
use databend_common_sql::plans::Plan;
use log::info;

use crate::history_tables::external::ExternalStorageConnection;
use crate::sessions::QueryContext;

pub async fn get_schemas(
    ctx: Arc<QueryContext>,
    new_create_sql: &str,
    table_name: &str,
) -> Result<(TableSchemaRef, TableSchemaRef)> {
    let old_table_schema = ThreadTracker::tracking_future(ctx.get_table(
        CATALOG_DEFAULT,
        "system_history",
        table_name,
    ))
    .await?
    .schema();
    let mut planner = Planner::new(ctx.clone());
    let (create_plan, _) = ThreadTracker::tracking_future(planner.plan_sql(new_create_sql)).await?;
    let new_table_schema = match create_plan {
        Plan::CreateTable(plan) => plan.schema,
        _ => {
            unreachable!("logic error: expected CreateTable plan")
        }
    };
    Ok((old_table_schema, new_table_schema))
}

pub async fn get_alter_table_sql(
    ctx: Arc<QueryContext>,
    new_create_sql: &str,
    table_name: &str,
) -> Result<Vec<String>> {
    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.capture_log_settings = Some(CaptureLogSettings::capture_off());
    let _guard = ThreadTracker::tracking(tracking_payload);
    let (old_table_schema, new_table_schema) =
        ThreadTracker::tracking_future(get_schemas(ctx, new_create_sql, table_name)).await?;
    // The table schema change follow "open-closed principle", only accept adding new fields.
    // If the new table schema has less or equal fields than the old one, means older version
    // node restarted, we should not alter the table.
    if new_table_schema.fields.len() <= old_table_schema.fields.len() {
        return Ok(vec![]);
    }

    // Find the new fields in the new table schema
    let new_fields: Vec<_> = new_table_schema
        .fields
        .iter()
        .filter(|f| {
            !old_table_schema
                .fields
                .iter()
                .any(|old_f| old_f.name == f.name)
        })
        .collect();

    let alter_sqls: Vec<String> = new_fields
        .iter()
        .map(|f| {
            format!(
                "ALTER TABLE system_history.{} ADD COLUMN {} {}",
                table_name,
                f.name,
                f.data_type().sql_name()
            )
        })
        .collect();

    Ok(alter_sqls)
}

/// Determines whether the history table should be reset based on its existence and new config storage parameters.
///
/// Reset Condition:
/// 1. Internal -> External: If the internal table exist, and config a new connection, reset the table.
/// 2. External -> Internal: If the external table exist and new config connection is None, means old config node restarted, do not reset the table.
/// 3. External1 -> External2: If the external table exist and new config connection is different from the current one, need manually drop the table and stage first.
///
/// Note: We only support converting from internal to external.
pub async fn should_reset(
    context: Arc<QueryContext>,
    connection: &Option<ExternalStorageConnection>,
) -> Result<bool> {
    let table = get_log_table(context.clone()).await?;

    if table.is_none() {
        return Ok(false);
    }

    let table = table.unwrap();

    let table_info = table.get_table_info();

    let current_storage_params = table_info.meta.storage_params.as_ref();

    // Internal -> External
    if current_storage_params.is_none() && connection.is_some() {
        info!(
            "Converting internal table to external table, current None vs new {:?}",
            connection
        );
        return Ok(true);
    }

    // External -> Internal
    if current_storage_params.is_some() && connection.is_none() {
        info!(
            "Converting external table to internal table, current {:?} vs new None",
            current_storage_params
        );
        return Ok(false);
    }

    if let Some(c) = connection {
        let uri = format!("{}{}/", c.uri, "log_history");
        let mut uri_location = UriLocation::from_uri(uri, c.params.clone())?;
        let mut payload = ThreadTracker::new_tracking_payload();
        payload.capture_log_settings = Some(CaptureLogSettings::capture_off());
        let _guard = ThreadTracker::tracking(payload);
        let (new_storage_params, _) = ThreadTracker::tracking_future(parse_uri_location(
            &mut uri_location,
            Some(context.as_ref()),
        ))
        .await?;

        // External1 -> External2
        // return error to prevent cyclic conversion
        if current_storage_params != Some(&new_storage_params) {
            info!(
                "Storage parameters have changed, current {:?} vs new {:?}",
                current_storage_params, new_storage_params
            );
            return Err(ErrorCode::InvalidConfig(
                "Cannot change storage parameters of external history table, please drop the tables and stage first.",
            ));
        }
    }
    Ok(false)
}

pub async fn get_log_table(context: Arc<QueryContext>) -> Result<Option<Arc<dyn Table>>> {
    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.capture_log_settings = Some(CaptureLogSettings::capture_off());
    let _guard = ThreadTracker::tracking(tracking_payload);
    let table = ThreadTracker::tracking_future(context.get_table(
        CATALOG_DEFAULT,
        "system_history",
        "log_history",
    ))
    .await;

    // table is not exist
    if table.is_err() {
        return Ok(None);
    }

    Ok(Some(table?))
}

#[cfg(test)]
mod tests {
    mod alter_table_tests {
        use databend_common_exception::ErrorCode;

        use crate::history_tables::alter_table::get_alter_table_sql;
        use crate::test_kits::TestFixture;

        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn test_get_alter_table_sql() -> Result<(), ErrorCode> {
            let test_fixture = TestFixture::setup_with_history_log().await?;
            let ctx = test_fixture.new_query_ctx().await?;
            let create_db = "CREATE DATABASE system_history";
            let create_table = "CREATE TABLE system_history.test_table_alter (id INT, name String)";
            let _ = test_fixture.execute_query(create_db).await?;
            let _ = test_fixture.execute_query(create_table).await?;

            // not change the table schema, should return empty alter sqls
            let no_change_create_sql =
                "CREATE TABLE system_history.test_table_alter (id INT, name String)";
            let table_name = "test_table_alter";
            let alter_sqls =
                get_alter_table_sql(ctx.clone(), no_change_create_sql, table_name).await?;
            assert_eq!(alter_sqls.len(), 0);

            // add a new field `age` to the existing table
            let new_create_sql =
                "CREATE TABLE system_history.test_table_alter (id INT, name String, age INT)";
            let alter_sqls = get_alter_table_sql(ctx.clone(), new_create_sql, table_name).await?;
            assert_eq!(alter_sqls.len(), 1);
            assert_eq!(
                alter_sqls[0],
                "ALTER TABLE system_history.test_table_alter ADD COLUMN age INT NULL"
            );

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn test_get_alter_table_sql_old_version() -> Result<(), ErrorCode> {
            // This test simulates the following scenario:
            // - A cluster with two nodes, where one node has a newer version of the table schema.
            // - The node with the newer schema version has already altered the table.
            // - The node with the older schema version is restarted.
            // - Upon restart, the older node should not attempt to alter the table again.

            let test_fixture = TestFixture::setup_with_history_log().await?;
            let ctx = test_fixture.new_query_ctx().await?;
            let create_db = "CREATE DATABASE system_history";
            let create_table =
                "CREATE TABLE system_history.test_table_alter (id INT, name String, age INT)";
            let _ = test_fixture.execute_query(create_db).await?;
            let _ = test_fixture.execute_query(create_table).await?;

            // add a new field `age` to the existing table
            let new_create_sql =
                "CREATE TABLE system_history.test_table_alter (id INT, name String)";
            let table_name = "test_table_alter";

            let alter_sqls = get_alter_table_sql(ctx, new_create_sql, table_name).await?;
            assert_eq!(alter_sqls.len(), 0);

            Ok(())
        }
    }

    mod should_reset_tests {
        use std::collections::BTreeMap;

        use databend_common_exception::ErrorCode;

        use crate::history_tables::alter_table::should_reset;
        use crate::history_tables::external::ExternalStorageConnection;
        use crate::test_kits::TestFixture;

        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn test_should_reset_table_not_exists() -> Result<(), ErrorCode> {
            // Test: When log_history table doesn't exist, should_reset should return false
            let test_fixture = TestFixture::setup_with_history_log().await?;
            let ctx = test_fixture.new_query_ctx().await?;

            // Don't create the table, it should not exist
            let connection = None;
            let result = should_reset(ctx, &connection).await?;
            assert!(!result, "Should not reset when table doesn't exist");

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn test_should_reset_both_inner() -> Result<(), ErrorCode> {
            // Test: When log_history table is internal, config also internal, should_reset should return false
            let test_fixture = TestFixture::setup_with_history_log().await?;
            let ctx = test_fixture.new_query_ctx().await?;

            // Create system_history database and internal log_history table
            let create_db = "CREATE DATABASE system_history";
            let create_table = "CREATE TABLE system_history.log_history (timestamp TIMESTAMP, level STRING, message STRING)";
            let _ = test_fixture.execute_query(create_db).await?;
            let _ = test_fixture.execute_query(create_table).await?;

            let connection = None;
            let result = should_reset(ctx, &connection).await?;
            assert!(!result, "Should not reset when table doesn't exist");

            Ok(())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
        async fn test_should_reset_inner_to_external() -> Result<(), ErrorCode> {
            // Test Condition: Internal -> External conversion should trigger reset
            let test_fixture = TestFixture::setup_with_history_log().await?;
            let ctx = test_fixture.new_query_ctx().await?;

            // Create system_history database and internal log_history table
            let create_db = "CREATE DATABASE system_history";
            let create_table = "CREATE TABLE system_history.log_history (timestamp TIMESTAMP, level STRING, message STRING)";
            let _ = test_fixture.execute_query(create_db).await?;
            let _ = test_fixture.execute_query(create_table).await?;

            // Create external connection
            let mut external_connection = ExternalStorageConnection::new(BTreeMap::new());
            external_connection.set_uri(
                "s3".to_string(),
                "test-bucket".to_string(),
                "test-root".to_string(),
            );
            external_connection.set_value("access_key_id".to_string(), "test_key".to_string());

            let connection = Some(external_connection);
            let result = should_reset(ctx, &connection).await?;
            assert!(
                result,
                "Should reset when converting internal table to external"
            );

            Ok(())
        }
    }
}
