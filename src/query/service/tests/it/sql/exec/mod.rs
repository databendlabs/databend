// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::plans::Plan;
use common_sql::Planner;
use common_storages_fuse::FuseTable;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::OptimizeTableInterpreter;
use databend_query::test_kits::table_test_fixture::do_insert;
use databend_query::test_kits::TestFixture;

#[test]
pub fn test_format_field_name() {
    use databend_query::sql::executor::decode_field_name;
    use databend_query::sql::executor::format_field_name;
    let display_name = "column_name123名字".to_string();
    let index = 12321;
    let field_name = format_field_name(display_name.as_str(), index);
    let (decoded_name, decoded_index) = decode_field_name(field_name.as_str()).unwrap();
    assert!(decoded_name == display_name && decoded_index == index);
}

#[tokio::test(flavor = "multi_thread")]
pub async fn test_snapshot_consistency() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let db2 = db.clone();
    let tbl2 = tbl.clone();
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    // 1. insert into tbl
    let insert_sql = format!("insert into {}.{} values(1,(1,2))", db, tbl);
    let mut planner = Planner::new(ctx.clone());
    let mut planner2 = Planner::new(ctx.clone());
    // generate 3 segments
    for _ in 0..3 {
        let (insert_plan, _) = planner.plan_sql(&insert_sql).await?;
        if let Plan::Insert(insert) = insert_plan {
            do_insert(ctx.clone(), *insert).await?;
        }
    }

    let query_task = async move {
        // 2. test compact and select concurrency
        let query = format!(
            "select * from {}.{} join (select id,t from {}.{} as t2 where id > 1 and id < 100000)",
            db, tbl, db, tbl
        );

        // a. thread 1: read table
        println!("read plan");

        let (query_plan, _) = planner.plan_sql(&query).await?;
        if let Plan::Query {
            s_expr: _s_expr,
            metadata,
            bind_context: _bind_context,
            rewrite_kind: _rewrite_kind,
            formatted_ast: _formatted_ast,
            ignore_result: _ignore_result,
        } = query_plan
        {
            let meta = metadata.read();
            let tbl_entries = meta.tables();
            let mut tables = Vec::with_capacity(2);
            for entry in tbl_entries {
                if entry.name() == &tbl {
                    tables.push(entry.table());
                }
            }
            assert_eq!(tables.len(), 2);
            let table0 = tables[0].clone();
            let table1 = tables[1].clone();

            let fuse_table0 = table0
                .as_any()
                .downcast_ref::<FuseTable>()
                .ok_or(ErrorCode::Unimplemented(format!(
                    "table {}, engine type {}, does not support",
                    table0.name(),
                    table0.get_table_info().engine(),
                )))
                .unwrap();
            let snapshot0 = fuse_table0.read_table_snapshot().await?;

            let fuse_table1 = table1
                .as_any()
                .downcast_ref::<FuseTable>()
                .ok_or(ErrorCode::Unimplemented(format!(
                    "table {}, engine type {}, does not support",
                    table1.name(),
                    table1.get_table_info().engine(),
                )))
                .unwrap();
            let snapshot1 = fuse_table1.read_table_snapshot().await?;

            let res = match (snapshot0, snapshot1) {
                (None, None) => true,
                (None, Some(_)) => false,
                (Some(_), None) => false,
                (Some(a), Some(b)) => {
                    if a.segments == b.segments {
                        true
                    } else {
                        false
                    }
                }
            };
            if !res {
                return Err(ErrorCode::BadArguments("snapshot consistency failed"));
            }
        } else {
            return Err(ErrorCode::BadArguments("query bad plan"));
        }
        Ok::<(), ErrorCode>(())
    };

    query_task.await?;

    let compact_task = async move {
        let compact_sql = format!("optimize table {}.{} compact", db2, tbl2);
        let (compact_plan, _) = planner2.plan_sql(&compact_sql).await?;
        if let Plan::OptimizeTable(plan) = compact_plan {
            let optimize_interpreter =
                OptimizeTableInterpreter::try_create(ctx.clone(), *plan.clone())?;
            optimize_interpreter.execute(ctx).await?;
        }
        Ok::<(), ErrorCode>(())
    };

    // b. thread2: optmize table
    tokio::spawn(compact_task);

    Ok(())
}
