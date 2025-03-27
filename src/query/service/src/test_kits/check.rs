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

use std::str;

use databend_common_catalog::table::Table;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_expression::block_debug::assert_blocks_sorted_eq_with_name;
use databend_common_expression::DataBlock;
use databend_common_expression::SendableDataBlockStream;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storages_fuse::operations::load_last_snapshot_hint;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::FUSE_TBL_BLOCK_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SEGMENT_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SNAPSHOT_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_XOR_BLOOM_INDEX_PREFIX;
use futures::TryStreamExt;
use walkdir::WalkDir;

use crate::test_kits::TestFixture;

pub fn expects_err<T>(case_name: &str, err_code: u16, res: Result<T>) {
    if let Err(err) = res {
        assert_eq!(
            err.code(),
            err_code,
            "case name {}, unexpected error: {}",
            case_name,
            err
        );
    } else {
        panic!(
            "case name {}, expecting err code {}, but got ok",
            case_name, err_code,
        );
    }
}

pub async fn expects_ok(
    case_name: impl AsRef<str>,
    res: Result<SendableDataBlockStream>,
    expected: Vec<&str>,
) -> Result<()> {
    match res {
        Ok(stream) => {
            let blocks: Vec<DataBlock> = stream.try_collect().await?;
            assert_blocks_sorted_eq_with_name(case_name.as_ref(), expected, &blocks)
        }
        Err(err) => {
            panic!(
                "case name {}, expecting  Ok, but got err {}",
                case_name.as_ref(),
                err,
            )
        }
    };
    Ok(())
}

pub async fn check_data_dir(
    fixture: &TestFixture,
    case_name: &str,
    snapshot_count: u32,
    table_statistic_count: u32,
    segment_count: u32,
    block_count: u32,
    index_count: u32,
    check_last_snapshot: Option<()>,
    check_table_statistic_file: Option<()>,
) -> Result<()> {
    let data_path = match &GlobalConfig::instance().storage.params {
        StorageParams::Fs(v) => v.root.clone(),
        _ => panic!("storage type is not fs"),
    };
    let root = data_path.as_str();
    let mut ss_count = 0;
    let mut ts_count = 0;
    let mut sg_count = 0;
    let mut b_count = 0;
    let mut i_count = 0;
    let mut table_statistic_files = vec![];
    let prefix_snapshot = FUSE_TBL_SNAPSHOT_PREFIX;
    let prefix_snapshot_statistics = FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX;
    let prefix_segment = FUSE_TBL_SEGMENT_PREFIX;
    let prefix_block = FUSE_TBL_BLOCK_PREFIX;
    let prefix_index = FUSE_TBL_XOR_BLOOM_INDEX_PREFIX;
    for entry in WalkDir::new(root) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() {
            let (_, entry_path) = entry.path().to_str().unwrap().split_at(root.len());
            // trim the leading prefix, e.g. "/db_id/table_id/"
            let path = entry_path.split('/').skip(3).collect::<Vec<_>>();
            let path = path[0];
            if path.starts_with(prefix_snapshot) {
                ss_count += 1;
            } else if path.starts_with(prefix_segment) {
                sg_count += 1;
            } else if path.starts_with(prefix_block) {
                b_count += 1;
            } else if path.starts_with(prefix_index) {
                i_count += 1;
            } else if path.starts_with(prefix_snapshot_statistics) {
                ts_count += 1;
                table_statistic_files.push(entry_path.to_string());
            }
        }
    }

    assert_eq!(
        ss_count, snapshot_count,
        "case [{}], check snapshot count",
        case_name
    );
    assert_eq!(
        ts_count, table_statistic_count,
        "case [{}], check snapshot statistics count",
        case_name
    );
    assert_eq!(
        sg_count, segment_count,
        "case [{}], check segment count",
        case_name
    );

    assert_eq!(
        b_count, block_count,
        "case [{}], check block count",
        case_name
    );

    assert_eq!(
        i_count, index_count,
        "case [{}], check index count",
        case_name
    );

    if check_last_snapshot.is_some() {
        let table = fixture.latest_default_table().await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot_loc = fuse_table.snapshot_loc().unwrap();

        let table_info = fuse_table.get_table_info();
        let storage_prefix = FuseTable::parse_storage_prefix_from_table_info(table_info)?;
        let hint =
            load_last_snapshot_hint(storage_prefix.as_str(), fuse_table.get_operator_ref()).await?;

        if let Some(hint) = hint {
            let operator_meta_data = fuse_table.get_operator_ref().info();
            let storage_prefix = operator_meta_data.root();
            let snapshot_full_path = format!("{}{}", storage_prefix, snapshot_loc);
            assert_eq!(snapshot_full_path, hint.snapshot_full_path);
        } else {
            panic!("hint of the last snapshot is not there!");
        }
    }

    if check_table_statistic_file.is_some() {
        let table = fixture.latest_default_table().await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot_opt = fuse_table.read_table_snapshot().await?;
        assert!(snapshot_opt.is_some());
        let snapshot = snapshot_opt.unwrap();
        let ts_location_opt = snapshot.table_statistics_location.clone();
        assert!(ts_location_opt.is_some());
        let ts_location = ts_location_opt.unwrap();
        println!(
            "ts_location_opt: {:?}, table_statistic_files: {:?}",
            ts_location, table_statistic_files
        );
        assert!(table_statistic_files
            .iter()
            .any(|e| e.contains(&ts_location)));
    }

    Ok(())
}

pub async fn history_should_have_item(
    fixture: &TestFixture,
    case_name: &str,
    item_cnt: u32,
) -> Result<()> {
    // check history
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let count_str = format!("| {}        |", item_cnt);
    let expected = vec![
        "+----------+",
        "| Column 0 |",
        "+----------+",
        count_str.as_str(),
        "+----------+",
    ];

    let qry = format!(
        "select count(*) as count from fuse_snapshot('{}', '{}')",
        db, tbl
    );

    expects_ok(
        format!("{}: count_of_history_item_should_be_1", case_name),
        fixture.execute_query(qry.as_str()).await,
        expected,
    )
    .await
}
