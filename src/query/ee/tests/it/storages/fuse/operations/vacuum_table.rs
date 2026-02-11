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

use databend_common_storages_fuse::FuseTable;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::*;

use super::vacuum2::snapshot_segment_and_block_locations;

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum_table_keeps_branch_refs() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command("set enable_experimental_table_ref=1")
        .await?;

    append_sample_data(1, &fixture).await?;

    let db = fixture.default_db_name();
    let table = fixture.default_table_name();
    let table_obj = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table_obj.as_ref())?;
    let snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let (branch_segments, branch_blocks) =
        snapshot_segment_and_block_locations(fuse_table, snapshot.as_ref()).await?;
    assert!(!branch_segments.is_empty());
    assert!(!branch_blocks.is_empty());

    fixture
        .execute_command(format!("alter table {db}.{table} create branch old_branch").as_str())
        .await?;
    fixture
        .execute_command(format!("truncate table {db}.{table}").as_str())
        .await?;
    append_sample_data(1, &fixture).await?;

    fixture
        .execute_command(format!("vacuum table {db}.{table}").as_str())
        .await?;

    let latest_table = fixture.latest_default_table().await?;
    let latest_fuse_table = FuseTable::try_from_table(latest_table.as_ref())?;
    let operator = latest_fuse_table.get_operator_ref();
    for location in &branch_segments {
        assert!(
            operator.exists(location).await?,
            "segment referenced by branch was incorrectly removed: {location}"
        );
    }
    for location in &branch_blocks {
        assert!(
            operator.exists(location).await?,
            "block referenced by branch was incorrectly removed: {location}"
        );
    }

    Ok(())
}
