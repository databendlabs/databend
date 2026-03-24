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

use databend_common_exception::Result;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::expects_ok;

#[tokio::test(flavor = "multi_thread")]
async fn test_window_grouping_over_rollup() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();

    fixture
        .execute_command(&format!("create database {db}"))
        .await?;
    fixture
        .execute_command(&format!(
            "create table {db}.empsalary (depname string, empno bigint, salary int, enroll_date date)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.empsalary values \
            ('develop', 10, 5200, '2007-08-01'), \
            ('sales', 1, 5000, '2006-10-01'), \
            ('personnel', 5, 3500, '2007-12-10'), \
            ('sales', 4, 4800, '2007-08-08'), \
            ('personnel', 2, 3900, '2006-12-23'), \
            ('develop', 7, 4200, '2008-01-01'), \
            ('develop', 9, 4500, '2008-01-01'), \
            ('sales', 3, 4800, '2007-08-01'), \
            ('develop', 8, 6000, '2006-10-01'), \
            ('develop', 11, 5200, '2007-08-15')"
        ))
        .await?;

    expects_ok(
        "window grouping over rollup",
        fixture
            .execute_query(&format!(
                "select grouping(salary), grouping(depname), \
                sum(grouping(salary)) over (partition by grouping(salary) + grouping(depname) \
                order by grouping(depname) desc) \
                from {db}.empsalary group by rollup (depname, salary) order by 1,2,3"
            ))
            .await,
        vec![
            "+----------+----------+----------+",
            "| Column 0 | Column 1 | Column 2 |",
            "+----------+----------+----------+",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 0        | 0        | 0        |",
            "| 1        | 0        | 3        |",
            "| 1        | 0        | 3        |",
            "| 1        | 0        | 3        |",
            "| 1        | 1        | 1        |",
            "+----------+----------+----------+",
        ],
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unnest_aggregate_argument() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    expects_ok(
        "unnest aggregate argument",
        fixture.execute_query("select unnest(max([11,12]))").await,
        vec![
            "+----------+",
            "| Column 0 |",
            "+----------+",
            "| 11       |",
            "| 12       |",
            "+----------+",
        ],
    )
    .await?;

    Ok(())
}
