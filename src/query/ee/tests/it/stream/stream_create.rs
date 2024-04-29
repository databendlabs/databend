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
use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::generate_snapshots;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_stream_create() -> Result<()> {
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
