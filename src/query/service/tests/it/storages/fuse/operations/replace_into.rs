// Copyright 2023 Datafuse Labs.
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

use databend_common_storages_fuse::FuseTable;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::latest_default_block_meta;
use itertools::Itertools;

#[test]
fn test_partition() -> anyhow::Result<()> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    for _ in 0..100 {
        let number_segment: usize = rng.gen_range(1..100);

        // do not matter, arbitrarily picked
        let format_version = 2;

        let segments = (0..number_segment)
            .map(|idx| (format!("{idx}"), format_version))
            .collect::<Vec<_>>();
        let segments: Vec<_> = segments.into_iter().enumerate().collect();

        for _ in 0..100 {
            let num_partition: usize = if number_segment == 1 {
                1
            } else {
                rng.gen_range(1..number_segment)
            };

            let partitions = FuseTable::partition_segments(&segments, num_partition);
            // check number of partitions are as expected
            assert_eq!(partitions.len(), num_partition);

            // check segments
            let origin = &segments;
            let segment_of_chunks = partitions
                .iter()
                .flatten()
                .sorted_by(|a, b| a.0.cmp(&b.0))
                .collect::<Vec<_>>();

            for (origin_idx, origin_location) in origin {
                let (seg_idx, seg_location) = segment_of_chunks[*origin_idx];
                assert_eq!(origin_idx, seg_idx);
                assert_eq!(origin_location, seg_location);
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replace_bloom_prunes_partial_update_block() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, a int, b int) engine=fuse \
             cluster by(id) bloom_index_columns='id,a,b' enable_partial_update=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 10, 100), (100, 30, 200)"
        ))
        .await?;
    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set a = a + 1 where id = 1"
        ))
        .await?;
    let block_meta = latest_default_block_meta(&fixture).await?;
    assert_eq!(block_meta.bloom_index_files.len(), 2);

    // Make the active data files unreadable while leaving both Bloom files intact. A key that
    // might conflict must still try to read the block, proving that the missing data is visible.
    // The absent key below can succeed only if REPLACE reads and combines the split Bloom files.
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse_table.get_operator();
    for group in block_meta.physical_column_groups().iter() {
        operator.delete(&group.location.0).await?;
    }
    assert!(
        fixture
            .execute_command(&format!(
                "replace into {db}.{table_name} on(id, a) values (1, 11, 999)"
            ))
            .await
            .is_err()
    );

    fixture
        .execute_command(&format!(
            "replace into {db}.{table_name} on(id, a) values (50, 20, 999)"
        ))
        .await?;
    let latest_table = fixture.latest_default_table().await?;
    let latest_fuse = FuseTable::try_from_table(latest_table.as_ref())?;
    assert_eq!(
        latest_fuse
            .read_table_snapshot()
            .await?
            .unwrap()
            .summary
            .row_count,
        3
    );

    Ok(())
}
