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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::test_kits::*;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_computed_column() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    fixture.create_default_database().await?;
    fixture.create_computed_table().await?;

    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    for i in 0..2 {
        let table = fixture.latest_default_table().await?;
        let num_blocks = 1;
        let stream = TestFixture::gen_computed_sample_blocks_stream(num_blocks, i);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;
    }

    // insert
    {
        let query = format!("select * from {}.{} order by id", db, tbl);
        let expected = vec![
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 | Column 6 |",
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| 0        | '0-0-s'  | 'S-0-0'  | 2        | 3        | 's-0-0'  | 0        |",
            "| 1        | '1-0-s'  | 'S-0-1'  | 3        | 6        | 's-0-1'  | 1        |",
            "| 2        | '2-0-s'  | 'S-0-2'  | 4        | 9        | 's-0-2'  | 2        |",
            "| 3        | '0-1-s'  | 'S-1-0'  | 12       | 33       | 's-1-0'  | 10       |",
            "| 4        | '1-1-s'  | 'S-1-1'  | 13       | 36       | 's-1-1'  | 11       |",
            "| 5        | '2-1-s'  | 'S-1-2'  | 14       | 39       | 's-1-2'  | 12       |",
            "+----------+----------+----------+----------+----------+----------+----------+",
        ];
        expects_ok(
            "check insert computed columns",
            fixture.execute_query(query.as_str()).await,
            expected,
        )
        .await?;
    }

    // update
    {
        let update = format!("update {}.{} set c = 'abc', d = 100 where id = 0", db, tbl);
        fixture.execute_command(&update).await?;

        let query = format!("select * from {}.{} order by id", db, tbl);
        let expected = vec![
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 | Column 6 |",
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| 0        | 'cba'    | 'ABC'    | 102      | 303      | 'abc'    | 100      |",
            "| 1        | '1-0-s'  | 'S-0-1'  | 3        | 6        | 's-0-1'  | 1        |",
            "| 2        | '2-0-s'  | 'S-0-2'  | 4        | 9        | 's-0-2'  | 2        |",
            "| 3        | '0-1-s'  | 'S-1-0'  | 12       | 33       | 's-1-0'  | 10       |",
            "| 4        | '1-1-s'  | 'S-1-1'  | 13       | 36       | 's-1-1'  | 11       |",
            "| 5        | '2-1-s'  | 'S-1-2'  | 14       | 39       | 's-1-2'  | 12       |",
            "+----------+----------+----------+----------+----------+----------+----------+",
        ];
        expects_ok(
            "check update computed columns",
            fixture.execute_query(query.as_str()).await,
            expected,
        )
        .await?;

        let update = format!("update {}.{} set c = 'xyz', d = 30 where b1 = 12", db, tbl);
        fixture.execute_command(&update).await?;

        let query = format!("select * from {}.{} order by id", db, tbl);
        let expected = vec![
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 | Column 6 |",
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| 0        | 'cba'    | 'ABC'    | 102      | 303      | 'abc'    | 100      |",
            "| 1        | '1-0-s'  | 'S-0-1'  | 3        | 6        | 's-0-1'  | 1        |",
            "| 2        | '2-0-s'  | 'S-0-2'  | 4        | 9        | 's-0-2'  | 2        |",
            "| 3        | 'zyx'    | 'XYZ'    | 32       | 93       | 'xyz'    | 30       |",
            "| 4        | '1-1-s'  | 'S-1-1'  | 13       | 36       | 's-1-1'  | 11       |",
            "| 5        | '2-1-s'  | 'S-1-2'  | 14       | 39       | 's-1-2'  | 12       |",
            "+----------+----------+----------+----------+----------+----------+----------+",
        ];
        expects_ok(
            "check update computed columns",
            fixture.execute_query(query.as_str()).await,
            expected,
        )
        .await?;
    }

    // delete
    {
        let delete = format!("delete from {}.{} where id >= 4", db, tbl);
        fixture.execute_command(&delete).await?;

        let query = format!("select * from {}.{} order by id", db, tbl);
        let expected = vec![
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 | Column 6 |",
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| 0        | 'cba'    | 'ABC'    | 102      | 303      | 'abc'    | 100      |",
            "| 1        | '1-0-s'  | 'S-0-1'  | 3        | 6        | 's-0-1'  | 1        |",
            "| 2        | '2-0-s'  | 'S-0-2'  | 4        | 9        | 's-0-2'  | 2        |",
            "| 3        | 'zyx'    | 'XYZ'    | 32       | 93       | 'xyz'    | 30       |",
            "+----------+----------+----------+----------+----------+----------+----------+",
        ];
        expects_ok(
            "check delete computed columns",
            fixture.execute_query(query.as_str()).await,
            expected,
        )
        .await?;

        let delete = format!("delete from {}.{} where b1 = 3 or b2 = 9", db, tbl);
        fixture.execute_command(&delete).await?;

        let query = format!("select * from {}.{} order by id", db, tbl);
        let expected = vec![
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 | Column 6 |",
            "+----------+----------+----------+----------+----------+----------+----------+",
            "| 0        | 'cba'    | 'ABC'    | 102      | 303      | 'abc'    | 100      |",
            "| 3        | 'zyx'    | 'XYZ'    | 32       | 93       | 'xyz'    | 30       |",
            "+----------+----------+----------+----------+----------+----------+----------+",
        ];
        expects_ok(
            "check delete computed columns",
            fixture.execute_query(query.as_str()).await,
            expected,
        )
        .await?;
    }

    Ok(())
}
