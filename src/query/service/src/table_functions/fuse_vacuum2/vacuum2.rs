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

use std::sync::Arc;

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use log::info;
use log::warn;

use crate::sessions::TableContext;

pub(crate) async fn vacuum_table(
    ctx: &Arc<dyn TableContext>,
    catalog: &dyn Catalog,
    database_name: &str,
    table_name: &str,
    respect_flash_back: bool,
) -> Result<()> {
    let table = catalog
        .get_table(&ctx.get_tenant(), database_name, table_name)
        .await?;
    let table = FuseTable::try_from_table(table.as_ref()).map_err(|_| {
        ErrorCode::StorageOther("Invalid table engine, only fuse table is supported")
    })?;

    table.check_mutable()?;
    get_vacuum_handler()
        .do_vacuum2(table, ctx.clone(), respect_flash_back)
        .await
}

pub(crate) async fn vacuum_tables(
    ctx: &Arc<dyn TableContext>,
    catalog: &dyn Catalog,
    database_name: Option<&str>,
) -> Result<()> {
    if let Some(database_name) = database_name {
        vacuum_database(ctx, catalog, database_name).await?;
        return Ok(());
    }

    let tenant = ctx.get_tenant();
    let databases = catalog.list_databases(&tenant).await?;
    let num_databases = databases.len();

    for (index, database) in databases.iter().enumerate() {
        if database.engine().eq_ignore_ascii_case("SYSTEM") {
            info!("Bypass system database [{}]", database.name());
            continue;
        }

        info!(
            "Processing db {}, progress: {}/{}",
            database.name(),
            index + 1,
            num_databases
        );
        vacuum_database(ctx, catalog, database.name()).await?;
    }

    Ok(())
}

async fn vacuum_database(
    ctx: &Arc<dyn TableContext>,
    catalog: &dyn Catalog,
    database_name: &str,
) -> Result<()> {
    let tenant = ctx.get_tenant();
    let tables = catalog.list_tables(&tenant, database_name).await?;
    info!("Found {} tables in db {}", tables.len(), database_name);

    let num_tables = tables.len();
    let handler = get_vacuum_handler();
    for (index, table) in tables.iter().enumerate() {
        let table_name = &table.get_table_info().name;
        info!(
            "Processing table {}.{}, db level progress: {}/{}",
            database_name,
            table_name,
            index + 1,
            num_tables
        );

        let Ok(table) = FuseTable::try_from_table(table.as_ref()) else {
            info!("Bypass non-fuse table {}.{}", database_name, table_name);
            continue;
        };

        if table.is_read_only() {
            info!("Bypass read only table {}.{}", database_name, table_name);
            continue;
        }

        let result = async {
            // Earlier tables may take days to vacuum. Refresh by ID so the
            // snapshot captured by list_tables does not hold back LVT and GC.
            let table = table.refresh(ctx.as_ref()).await?;
            handler.do_vacuum2(table.as_ref(), ctx.clone(), false).await
        }
        .await;
        if let Err(error) = result {
            if error.code() == ErrorCode::ABORTED_QUERY {
                return Err(error);
            }
            warn!(
                "vacuum2 table {}.{} failed: {}",
                database_name, table_name, error
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use databend_common_base::base::GlobalInstance;
    use databend_common_catalog::table_context::AbortChecker;
    use databend_enterprise_vacuum_handler::VacuumHandler;
    use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;
    use databend_enterprise_vacuum_handler::vacuum_handler::VacuumDropTablesResult;
    use databend_enterprise_vacuum_handler::vacuum_handler::VacuumTempOptions;
    use tokio::sync::Notify;
    use tokio::sync::mpsc;

    use super::*;
    use crate::test_kits::TestFixture;

    struct SnapshotRecordingVacuumHandler {
        first_table: mpsc::Sender<u64>,
        resume: Arc<Notify>,
        snapshots: Arc<Mutex<Vec<(u64, String)>>>,
    }

    #[async_trait::async_trait]
    impl VacuumHandler for SnapshotRecordingVacuumHandler {
        async fn do_vacuum2(
            &self,
            table: &dyn Table,
            _ctx: Arc<dyn TableContext>,
            respect_flash_back: bool,
        ) -> Result<()> {
            assert!(!respect_flash_back);
            let snapshot = FuseTable::try_from_table(table)?.snapshot_loc().unwrap();
            let is_first = {
                let mut snapshots = self.snapshots.lock().unwrap();
                snapshots.push((table.get_id(), snapshot));
                snapshots.len() == 1
            };
            if is_first {
                self.first_table.send(table.get_id()).await.unwrap();
                self.resume.notified().await;
            }
            Ok(())
        }

        async fn do_vacuum_drop_tables(
            &self,
            _threads_nums: usize,
            _tables: Vec<Arc<dyn Table>>,
            _dry_run_limit: Option<usize>,
        ) -> VacuumDropTablesResult {
            unreachable!("batch vacuum must not vacuum dropped tables")
        }

        async fn do_vacuum_temporary_files(
            &self,
            _abort_checker: AbortChecker,
            _temporary_dir: String,
            _options: &VacuumTempOptions,
            _vacuum_limit: usize,
        ) -> Result<usize> {
            unreachable!("batch vacuum must not vacuum temporary files")
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vacuum_tables_refreshes_snapshot_by_id() -> anyhow::Result<()> {
        let fixture = TestFixture::setup().await?;
        let database = "vacuum_refresh_db";
        fixture
            .execute_command(&format!("create database {database}"))
            .await?;
        for table in ["t1", "t2"] {
            fixture
                .execute_command(&format!(
                    "create table {database}.{table} (c int) as select 1"
                ))
                .await?;
        }

        let ctx: Arc<dyn TableContext> = fixture.new_query_ctx().await?;
        let catalog = ctx.get_default_catalog()?;
        let tables = catalog.list_tables(&ctx.get_tenant(), database).await?;
        assert_eq!(tables.len(), 2);

        let (first_table_tx, mut first_table_rx) = mpsc::channel(1);
        let resume = Arc::new(Notify::new());
        let snapshots = Arc::new(Mutex::new(Vec::new()));
        GlobalInstance::set(Arc::new(VacuumHandlerWrapper::new(Box::new(
            SnapshotRecordingVacuumHandler {
                first_table: first_table_tx,
                resume: resume.clone(),
                snapshots: snapshots.clone(),
            },
        ))));

        let update_waiting_table = async {
            // The first handler call proves that vacuum has already listed the
            // tables. Choose the other table without relying on catalog order.
            let first_table_id = first_table_rx.recv().await.unwrap();
            let waiting_table = tables
                .iter()
                .find(|table| table.get_id() != first_table_id)
                .unwrap();
            let old_snapshot = FuseTable::try_from_table(waiting_table.as_ref())?
                .snapshot_loc()
                .unwrap();
            let name = &waiting_table.get_table_info().name;
            fixture
                .execute_command(&format!("truncate table {database}.{name}"))
                .await?;
            // Refresh must use the table ID even if the listed name is stale.
            fixture
                .execute_command(&format!("alter table {database}.{name} rename to renamed"))
                .await?;
            let updated_table = catalog
                .get_table(&ctx.get_tenant(), database, "renamed")
                .await?;
            assert_eq!(updated_table.get_id(), waiting_table.get_id());
            let new_snapshot = FuseTable::try_from_table(updated_table.as_ref())?
                .snapshot_loc()
                .unwrap();
            assert_ne!(new_snapshot, old_snapshot);
            resume.notify_one();
            Ok::<_, ErrorCode>((updated_table.get_id(), new_snapshot))
        };

        let (_, expected_snapshot) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::try_join!(
                vacuum_tables(&ctx, catalog.as_ref(), Some(database)),
                update_waiting_table,
            )
        })
        .await??;

        let snapshots = snapshots.lock().unwrap();
        assert_eq!(snapshots.len(), 2, "both listed tables must be vacuumed");
        assert_eq!(
            snapshots[1], expected_snapshot,
            "vacuum must use the snapshot committed while the table was waiting"
        );
        Ok(())
    }
}
