//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
use common_base::tokio;
use common_exception::Result;
use walkdir::WalkDir;

use crate::storages::fuse::table_test_fixture::append_sample_data;
use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::expects_ok;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_truncate_purge_stmt() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    // ingests some test data
    append_sample_data(10, &fixture).await?;
    let data_path = ctx.get_config().storage.disk.data_path;
    let root = data_path.as_str();

    // there should be some data there
    {
        let mut got_some_files = false;
        for entry in WalkDir::new(root) {
            let entry = entry.unwrap();
            if entry.file_type().is_file() {
                got_some_files = true;
                break;
            }
        }
        assert!(got_some_files, "there should be some files");
    }

    // let's truncate
    {
        let qry = format!("truncate table '{}'.'{}' purge", db, tbl);
        execute_command(qry.as_str(), ctx.clone()).await?;

        let expected = vec![
            "+-------+",
            "| count |",
            "+-------+",
            "| 1     |",
            "+-------+",
        ];
        let qry = format!(
            "select count(*) as count from fuse_history('{}', '{}')",
            db, tbl
        );

        expects_ok(
            "after_truncate_there_should_be_one_history_item_left",
            execute_query(qry.as_str(), ctx.clone()).await,
            expected,
        )
        .await?;

        let mut got_some_files = false;
        for entry in WalkDir::new(root) {
            let entry = entry.unwrap();
            if entry.file_type().is_file() {
                got_some_files = true;
                break;
            }
        }
        assert!(got_some_files, "there should be some files");
    }
    Ok(())
}
