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

use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;

#[test]
fn test_explain_fragments_insert_multi_table_does_not_record_txn_timestamp() -> anyhow::Result<()> {
    std::thread::Builder::new()
        .name("multi-table-insert-test".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| -> anyhow::Result<()> {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(32 * 1024 * 1024)
                .enable_all()
                .build()?;

            runtime.block_on(async {
                let fixture = TestFixture::setup().await?;
                let db = fixture.default_db_name();

                fixture
                    .execute_command(&format!("create database {db}"))
                    .await?;
                fixture.execute_command(&format!("use {db}")).await?;
                fixture
                    .execute_command("create table mti_src(a int, b int)")
                    .await?;
                fixture
                    .execute_command("insert into mti_src values (1, 1), (2, 0)")
                    .await?;
                fixture
                    .execute_command("create table mti_all(a int, b int)")
                    .await?;
                fixture
                    .execute_command("create table mti_even(a int)")
                    .await?;

                let session = fixture.default_session();
                let begin_ctx = fixture.new_query_ctx().await?;
                execute_command(begin_ctx, "begin transaction").await?;

                let explain_ctx = fixture.new_query_ctx().await?;
                execute_command(
                    explain_ctx,
                    "EXPLAIN FRAGMENTS INSERT ALL
            WHEN true THEN INTO mti_all
            WHEN b = 0 THEN INTO mti_even VALUES(a)
         SELECT * FROM mti_src",
                )
                .await?;

                let inspect_ctx = fixture.new_query_ctx().await?;
                let all_table = inspect_ctx.get_table("default", &db, "mti_all").await?;
                let even_table = inspect_ctx.get_table("default", &db, "mti_even").await?;
                {
                    let txn_mgr = session.txn_mgr();
                    let txn_mgr = txn_mgr.lock();
                    assert!(txn_mgr.is_active());
                    assert!(
                        txn_mgr
                            .get_table_txn_begin_timestamp(all_table.get_id())
                            .is_none()
                    );
                    assert!(
                        txn_mgr
                            .get_table_txn_begin_timestamp(even_table.get_id())
                            .is_none()
                    );
                }

                let insert_ctx = fixture.new_query_ctx().await?;
                execute_command(
                    insert_ctx,
                    "INSERT ALL
            WHEN true THEN INTO mti_all
            WHEN b = 0 THEN INTO mti_even VALUES(a)
         SELECT * FROM mti_src",
                )
                .await?;
                {
                    let txn_mgr = session.txn_mgr();
                    let txn_mgr = txn_mgr.lock();
                    assert!(
                        txn_mgr
                            .get_table_txn_begin_timestamp(all_table.get_id())
                            .is_some()
                    );
                    assert!(
                        txn_mgr
                            .get_table_txn_begin_timestamp(even_table.get_id())
                            .is_some()
                    );
                }

                let rollback_ctx = fixture.new_query_ctx().await?;
                execute_command(rollback_ctx, "rollback").await?;

                Ok(())
            })
        })?
        .join()
        .map_err(|_| anyhow::anyhow!("multi-table insert test thread panicked"))?
}
