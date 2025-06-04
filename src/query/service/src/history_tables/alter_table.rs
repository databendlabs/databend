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

use databend_common_base::runtime::CaptureLogSettings;
use databend_common_base::runtime::ThreadTracker;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::plans::Plan;
use databend_common_sql::Planner;

use crate::sessions::QueryContext;

pub async fn get_alter_table_sql(
    ctx: Arc<QueryContext>,
    new_create_sql: &str,
    table_name: &str,
) -> Result<Vec<String>> {
    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.capture_log_settings = Some(CaptureLogSettings::capture_off());
    let _guard = ThreadTracker::tracking(tracking_payload);
    let old_table_schema = ThreadTracker::tracking_future(ctx.get_table(
        CATALOG_DEFAULT,
        "system_history",
        table_name,
    ))
    .await?
    .schema();
    let (create_plan, _) = Planner::new(ctx.clone()).plan_sql(new_create_sql).await?;
    let new_table_schema = match create_plan {
        Plan::CreateTable(plan) => plan.schema,
        _ => {
            unreachable!("logic error: expected CreateTable plan")
        }
    };
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

#[cfg(test)]
mod tests {
    use databend_common_exception::ErrorCode;

    use crate::history_tables::alter_table::get_alter_table_sql;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_alter_table_sql() -> Result<(), ErrorCode> {
        let test_fixture = TestFixture::setup().await?;
        let ctx = test_fixture.new_query_ctx().await?;
        let create_db = "CREATE DATABASE system_history";
        let create_table = "CREATE TABLE system_history.test_table_alter (id INT, name String)";
        let _ = test_fixture.execute_query(create_db).await?;
        let _ = test_fixture.execute_query(create_table).await?;

        // not change the table schema, should return empty alter sqls
        let no_change_create_sql =
            "CREATE TABLE system_history.test_table_alter (id INT, name String)";
        let table_name = "test_table_alter";
        let alter_sqls = get_alter_table_sql(ctx.clone(), no_change_create_sql, table_name).await?;
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

        let test_fixture = TestFixture::setup().await?;
        let ctx = test_fixture.new_query_ctx().await?;
        let create_db = "CREATE DATABASE system_history";
        let create_table =
            "CREATE TABLE system_history.test_table_alter (id INT, name String, age INT)";
        let _ = test_fixture.execute_query(create_db).await?;
        let _ = test_fixture.execute_query(create_table).await?;

        // add a new field `age` to the existing table
        let new_create_sql = "CREATE TABLE system_history.test_table_alter (id INT, name String)";
        let table_name = "test_table_alter";

        let alter_sqls = get_alter_table_sql(ctx, new_create_sql, table_name).await?;
        assert_eq!(alter_sqls.len(), 0);

        Ok(())
    }
}
