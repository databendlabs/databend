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
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::utils::FromData;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::ListHistoryTableBranchesReq;
use databend_common_meta_app::schema::TableBranchMeta;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use futures::StreamExt;
use futures::stream;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::collect_visible_tables;
use crate::util::extract_push_down_string_filters;
use crate::util::generate_default_catalog_meta;

pub struct BranchesTable<const WITH_HISTORY: bool> {
    table_info: TableInfo,
}

pub type BranchesTableWithHistory = BranchesTable<true>;
pub type BranchesTableWithoutHistory = BranchesTable<false>;

#[async_trait::async_trait]
pub trait BranchesHistoryAware {
    const TABLE_NAME: &'static str;
    async fn list_branches(
        catalog: &Arc<dyn Catalog>,
        table_id: u64,
        with_history: bool,
    ) -> Result<Vec<TableBranchMeta>>;
}

macro_rules! impl_branches_history_aware {
    ($with_history:expr, $table_name:expr) => {
        #[async_trait::async_trait]
        impl BranchesHistoryAware for BranchesTable<$with_history> {
            const TABLE_NAME: &'static str = $table_name;

            #[async_backtrace::framed]
            async fn list_branches(
                catalog: &Arc<dyn Catalog>,
                table_id: u64,
                with_history: bool,
            ) -> Result<Vec<TableBranchMeta>> {
                if with_history {
                    catalog
                        .list_history_table_branches(ListHistoryTableBranchesReq {
                            table_id,
                            retention_boundary: None,
                        })
                        .await
                } else {
                    catalog.list_table_branches(table_id).await
                }
            }
        }
    };
}

impl_branches_history_aware!(true, "branches_with_history");
impl_branches_history_aware!(false, "branches");

#[async_trait::async_trait]
impl<const WITH_HISTORY: bool> AsyncSystemTable for BranchesTable<WITH_HISTORY>
where BranchesTable<WITH_HISTORY>: BranchesHistoryAware
{
    const NAME: &'static str = Self::TABLE_NAME;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        // system.branches is a metadata inspection table. Keep it available without the
        // table-ref feature gate; SQL syntax such as SHOW BRANCHES performs that check.
        let tenant = ctx.get_tenant();
        let catalog_mgr = CatalogManager::instance();
        let catalog = catalog_mgr
            .get_catalog(
                tenant.tenant_name(),
                self.get_table_info().catalog(),
                ctx.session_state()?,
            )
            .await?
            .disable_table_info_refresh()?;

        let func_ctx = ctx.get_function_context()?;
        let mut filters = extract_push_down_string_filters(
            &push_downs,
            &["database", "table", "name"],
            &func_ctx,
        )?;
        let filtered_db_names = filters.remove(0);
        let filtered_table_names = filters.remove(0);
        let filtered_branch_names = filters.remove(0);
        let db_with_tables =
            collect_visible_tables(&ctx, &catalog, &filtered_db_names, &filtered_table_names)
                .await?;

        let catalog_name = catalog.name().to_string();
        let mut table_refs = Vec::new();
        for db in db_with_tables {
            let db_name = db.name;
            for table in db.tables {
                table_refs.push((db_name.clone(), table));
            }
        }

        let branch_concurrency = ctx
            .get_settings()
            .get_system_tables_count_db_concurrency()? as usize;
        let mut branch_futures = Vec::with_capacity(table_refs.len());
        for (db_name, table) in table_refs {
            branch_futures.push(Self::build_branch_rows(
                catalog.clone(),
                catalog_name.clone(),
                db_name,
                table,
                filtered_branch_names.clone(),
            ));
        }

        let mut rows = Vec::new();
        // Reuse the system.tables metadata fan-out setting here: branch listing also
        // issues one bounded meta request per visible base table.
        let mut branch_stream = stream::iter(branch_futures).buffer_unordered(branch_concurrency);
        while let Some(branch_rows) = branch_stream.next().await {
            rows.extend(branch_rows?);
        }

        rows.sort_by(|lhs, rhs| {
            (
                &lhs.catalog,
                &lhs.database,
                &lhs.table,
                &lhs.name,
                lhs.branch_id,
            )
                .cmp(&(
                    &rhs.catalog,
                    &rhs.database,
                    &rhs.table,
                    &rhs.name,
                    rhs.branch_id,
                ))
        });

        Ok(rows_to_datablock(rows))
    }
}

impl<const WITH_HISTORY: bool> BranchesTable<WITH_HISTORY>
where BranchesTable<WITH_HISTORY>: BranchesHistoryAware
{
    async fn build_branch_rows(
        catalog: Arc<dyn Catalog>,
        catalog_name: String,
        db_name: String,
        table: Arc<dyn Table>,
        filtered_branch_names: Vec<String>,
    ) -> Result<Vec<BranchRow>> {
        let branches = Self::list_branches(&catalog, table.get_id(), WITH_HISTORY).await?;
        let mut rows = Vec::with_capacity(branches.len());

        for branch in branches {
            if !filtered_branch_names.is_empty()
                && !filtered_branch_names
                    .iter()
                    .any(|name| name == &branch.branch_name)
            {
                continue;
            }
            rows.push(BranchRow::new(
                &catalog_name,
                &db_name,
                table.as_ref(),
                branch,
            ));
        }

        Ok(rows)
    }

    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("catalog", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new("table", TableDataType::String),
            TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("name", TableDataType::String),
            TableField::new("branch_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("snapshot_location", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new(
                "dropped_on",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new("updated_on", TableDataType::Timestamp),
            TableField::new(
                "expire_at",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
        ])
    }

    pub fn create(table_id: u64, ctl_name: &str) -> Arc<dyn Table> {
        let name = Self::TABLE_NAME;
        let table_info = TableInfo {
            desc: format!("'system'.'{name}'"),
            name: Self::NAME.to_owned(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: BranchesTable::<WITH_HISTORY>::schema(),
                engine: "SystemBranches".to_string(),
                ..Default::default()
            },
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), ctl_name).into(),
                meta: generate_default_catalog_meta(),
                ..Default::default()
            }),
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(BranchesTable::<WITH_HISTORY> { table_info })
    }
}

struct BranchRow {
    catalog: String,
    database: String,
    table: String,
    table_id: u64,
    name: String,
    branch_id: u64,
    snapshot_location: String,
    created_on: i64,
    dropped_on: Option<i64>,
    updated_on: i64,
    expire_at: Option<i64>,
}

impl BranchRow {
    fn new(catalog: &str, database: &str, table: &dyn Table, branch: TableBranchMeta) -> Self {
        let branch_meta = branch.branch_meta.data;
        let snapshot_location = branch_meta
            .options
            .get(OPT_KEY_SNAPSHOT_LOCATION)
            .cloned()
            .unwrap_or_default();

        Self {
            catalog: catalog.to_string(),
            database: database.to_string(),
            table: table.name().to_string(),
            table_id: table.get_id(),
            name: branch.branch_name,
            branch_id: branch.branch_id.table_id,
            snapshot_location,
            created_on: branch_meta.created_on.timestamp_micros(),
            dropped_on: branch_meta.drop_on.map(|t| t.timestamp_micros()),
            updated_on: branch_meta.updated_on.timestamp_micros(),
            expire_at: branch.expire_at.map(|t| t.timestamp_micros()),
        }
    }
}

fn rows_to_datablock(rows: Vec<BranchRow>) -> DataBlock {
    DataBlock::new_from_columns(vec![
        StringType::from_data(
            rows.iter()
                .map(|row| row.catalog.clone())
                .collect::<Vec<_>>(),
        ),
        StringType::from_data(
            rows.iter()
                .map(|row| row.database.clone())
                .collect::<Vec<_>>(),
        ),
        StringType::from_data(rows.iter().map(|row| row.table.clone()).collect::<Vec<_>>()),
        UInt64Type::from_data(rows.iter().map(|row| row.table_id).collect::<Vec<_>>()),
        StringType::from_data(rows.iter().map(|row| row.name.clone()).collect::<Vec<_>>()),
        UInt64Type::from_data(rows.iter().map(|row| row.branch_id).collect::<Vec<_>>()),
        StringType::from_data(
            rows.iter()
                .map(|row| row.snapshot_location.clone())
                .collect::<Vec<_>>(),
        ),
        TimestampType::from_data(rows.iter().map(|row| row.created_on).collect::<Vec<_>>()),
        TimestampType::from_opt_data(rows.iter().map(|row| row.dropped_on).collect::<Vec<_>>()),
        TimestampType::from_data(rows.iter().map(|row| row.updated_on).collect::<Vec<_>>()),
        TimestampType::from_opt_data(rows.iter().map(|row| row.expire_at).collect::<Vec<_>>()),
    ])
}
