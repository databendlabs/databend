// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use chrono::Duration;
use chrono::Utc;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::generate_snapshots;

#[tokio::test(flavor = "multi_thread")]
async fn test_stream_binds_source_database_id() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture.execute_command("CREATE DATABASE source_db").await?;
    fixture
        .execute_command("CREATE TABLE source_db.t(i INT) CHANGE_TRACKING = true")
        .await?;
    fixture
        .execute_command("CREATE STREAM default.s ON TABLE source_db.t")
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let db = ctx
        .get_default_catalog()?
        .get_database(&fixture.default_tenant(), "source_db")
        .await?;
    ctx.get_table("default", "default", "s").await?;
    // The full source table is now in the ordinary table cache, before any direct lookup.
    assert_eq!(
        ctx.get_resolved_database_id("default", "source_db"),
        Some(db.get_db_info().database_id.db_id)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stream_create() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture.create_default_database().await?;
    fixture.create_normal_table().await?;

    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    {
        let qry = format!(
            "alter table {}.{} set options(change_tracking=true)",
            db, tbl
        );
        let r = fixture.execute_command(&qry).await;
        assert!(r.is_ok());
    }

    generate_snapshots(&fixture).await?;
    let now = Utc::now();
    {
        let time_point = now - Duration::hours(14);
        let qry = format!(
            "create stream s on table {}.{} at(timestamp => '{:?}'::TIMESTAMP)",
            db, tbl, time_point
        );
        let r = fixture.execute_command(&qry).await;
        assert!(r.is_err());
        let expect = "TableHistoricalDataNotFound. Code: 2013, Text = No historical data found at given point.";
        assert_eq!(expect, format!("{}", r.unwrap_err()));
    }

    {
        let time_point = now - Duration::hours(12);
        let qry = format!(
            "create stream s on table {}.{} at(timestamp => '{:?}'::TIMESTAMP)",
            db, tbl, time_point
        );
        let r = fixture.execute_command(&qry).await;
        assert!(r.is_err());
        let expect = "IllegalStream. Code: 2733, Text = The stream navigation at point has not table version.";
        assert_eq!(expect, format!("{}", r.unwrap_err()));
    }

    {
        let qry = format!(
            "create stream s on table {}.{} append_only = false",
            db, tbl
        );
        let r = fixture.execute_command(&qry).await;
        assert!(r.is_ok());
    }

    Ok(())
}
