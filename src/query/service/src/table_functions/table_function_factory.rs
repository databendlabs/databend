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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table_args::TableArgs;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_fuse::table_functions::ClusteringStatisticsFunc;
use databend_common_storages_fuse::table_functions::FuseAmendTable;
use databend_common_storages_fuse::table_functions::FuseBlockFunc;
use databend_common_storages_fuse::table_functions::FuseColumnFunc;
use databend_common_storages_fuse::table_functions::FuseDumpSnapshotsFunc;
use databend_common_storages_fuse::table_functions::FuseEncodingFunc;
use databend_common_storages_fuse::table_functions::FusePageFunc;
use databend_common_storages_fuse::table_functions::FuseStatisticsFunc;
use databend_common_storages_fuse::table_functions::FuseTimeTravelSizeFunc;
use databend_common_storages_fuse::table_functions::FuseVacuumDropAggregatingIndex;
use databend_common_storages_fuse::table_functions::FuseVacuumDropInvertedIndex;
use databend_common_storages_fuse::table_functions::FuseVacuumTemporaryTable;
use databend_common_storages_fuse::table_functions::FuseVirtualColumnFunc;
use databend_common_storages_fuse::table_functions::SetCacheCapacity;
use databend_common_storages_fuse::table_functions::TableFunctionTemplate;
use databend_common_storages_iceberg::IcebergInspectTable;
use databend_common_storages_stream::stream_status_table_func::StreamStatusTable;
use databend_meta_types::MetaId;
use databend_storages_common_table_meta::table_id_ranges::SYS_TBL_FUC_ID_END;
use databend_storages_common_table_meta::table_id_ranges::SYS_TBL_FUNC_ID_BEGIN;
use itertools::Itertools;
use parking_lot::RwLock;

use super::LicenseInfoTable;
use super::TenantQuotaTable;
use super::others::UdfEchoTable;
use crate::storages::fuse::table_functions::ClusteringInformationFunc;
use crate::storages::fuse::table_functions::FuseSegmentFunc;
use crate::storages::fuse::table_functions::FuseSnapshotFunc;
use crate::table_functions::TableFunction;
use crate::table_functions::async_crash_me::AsyncCrashMeTable;
use crate::table_functions::cloud::TaskDependentsEnableTable;
use crate::table_functions::cloud::TaskDependentsTable;
use crate::table_functions::cloud::TaskHistoryTable;
use crate::table_functions::copy_history::CopyHistoryTable;
use crate::table_functions::fuse_vacuum2::FuseVacuum2Table;
use crate::table_functions::infer_schema::InferSchemaTable;
use crate::table_functions::inspect_parquet::InspectParquetTable;
use crate::table_functions::list_stage::ListStageTable;
use crate::table_functions::numbers::NumbersTable;
use crate::table_functions::policy_references::PolicyReferencesTable;
use crate::table_functions::show_grants::ShowGrants;
use crate::table_functions::show_roles::ShowRoles;
use crate::table_functions::show_sequences::ShowSequences;
use crate::table_functions::show_variables::ShowVariables;
use crate::table_functions::srf::RangeTable;
use crate::table_functions::sync_crash_me::SyncCrashMeTable;
use crate::table_functions::system::TableStatisticsFunc;
use crate::table_functions::tag_references::TagReferencesTable;
type TableFunctionCreators = RwLock<HashMap<String, (MetaId, Arc<dyn TableFunctionCreator>)>>;

pub trait TableFunctionCreator: Send + Sync {
    fn try_create(
        &self,
        db_name: &str,
        tbl_func_name: &str,
        tbl_id: MetaId,
        arg: TableArgs,
    ) -> Result<Arc<dyn TableFunction>>;
}

impl<T> TableFunctionCreator for T
where
    T: Fn(&str, &str, MetaId, TableArgs) -> Result<Arc<dyn TableFunction>>,
    T: Send + Sync,
{
    fn try_create(
        &self,
        db_name: &str,
        tbl_func_name: &str,
        tbl_id: MetaId,
        arg: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        self(db_name, tbl_func_name, tbl_id, arg)
    }
}

#[derive(Default)]
pub struct TableFunctionFactory {
    creators: TableFunctionCreators,
}

impl TableFunctionFactory {
    pub fn create(config: &InnerConfig) -> Self {
        let mut id = SYS_TBL_FUNC_ID_BEGIN;
        let mut next_id = || -> MetaId {
            if id >= SYS_TBL_FUC_ID_END {
                panic!("function table id used up")
            } else {
                let r = id;
                id += 1;
                r
            }
        };

        let mut creators: HashMap<String, (MetaId, Arc<dyn TableFunctionCreator>)> =
            Default::default();

        let number_table_func_creator: Arc<dyn TableFunctionCreator> =
            Arc::new(NumbersTable::create);

        creators.insert(
            "numbers".to_string(),
            (next_id(), number_table_func_creator.clone()),
        );
        creators.insert(
            "numbers_mt".to_string(),
            (next_id(), number_table_func_creator.clone()),
        );
        creators.insert(
            "numbers_local".to_string(),
            (next_id(), number_table_func_creator),
        );

        creators.insert(
            "fuse_snapshot".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseSnapshotFunc>::create),
            ),
        );

        creators.insert(
            "fuse_dump_snapshots".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseDumpSnapshotsFunc>::create),
            ),
        );

        creators.insert(
            "fuse_amend".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseAmendTable>::create),
            ),
        );

        creators.insert(
            "set_cache_capacity".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<SetCacheCapacity>::create),
            ),
        );

        creators.insert(
            "fuse_segment".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseSegmentFunc>::create),
            ),
        );

        creators.insert(
            "fuse_vacuum2".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseVacuum2Table>::create),
            ),
        );

        creators.insert(
            "fuse_block".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseBlockFunc>::create),
            ),
        );

        creators.insert(
            "fuse_page".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FusePageFunc>::create),
            ),
        );

        creators.insert(
            "fuse_column".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseColumnFunc>::create),
            ),
        );

        creators.insert(
            "fuse_virtual_column".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseVirtualColumnFunc>::create),
            ),
        );

        creators.insert(
            "fuse_statistic".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseStatisticsFunc>::create),
            ),
        );

        creators.insert(
            "clustering_information".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<ClusteringInformationFunc>::create),
            ),
        );

        creators.insert(
            "clustering_statistics".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<ClusteringStatisticsFunc>::create),
            ),
        );

        creators.insert(
            "fuse_vacuum_temporary_table".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseVacuumTemporaryTable>::create),
            ),
        );

        creators.insert(
            "stream_status".to_string(),
            (next_id(), Arc::new(StreamStatusTable::create)),
        );

        creators.insert(
            "sync_crash_me".to_string(),
            (next_id(), Arc::new(SyncCrashMeTable::create)),
        );

        creators.insert(
            "async_crash_me".to_string(),
            (next_id(), Arc::new(AsyncCrashMeTable::create)),
        );

        creators.insert(
            "infer_schema".to_string(),
            (next_id(), Arc::new(InferSchemaTable::create)),
        );
        creators.insert(
            "inspect_parquet".to_string(),
            (next_id(), Arc::new(InspectParquetTable::create)),
        );

        creators.insert(
            "list_stage".to_string(),
            (next_id(), Arc::new(ListStageTable::create)),
        );

        creators.insert(
            "generate_series".to_string(),
            (next_id(), Arc::new(RangeTable::create)),
        );

        creators.insert(
            "range".to_string(),
            (next_id(), Arc::new(RangeTable::create)),
        );

        creators.insert(
            "license_info".to_string(),
            (next_id(), Arc::new(LicenseInfoTable::create)),
        );

        creators.insert(
            "tenant_quota".to_string(),
            (next_id(), Arc::new(TenantQuotaTable::create)),
        );

        creators.insert(
            "fuse_encoding".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseEncodingFunc>::create),
            ),
        );

        if !config.task.on {
            creators.insert(
                "task_dependents".to_string(),
                (next_id(), Arc::new(TaskDependentsTable::create)),
            );

            creators.insert(
                "task_dependents_enable".to_string(),
                (next_id(), Arc::new(TaskDependentsEnableTable::create)),
            );

            creators.insert(
                "task_history".to_string(),
                (next_id(), Arc::new(TaskHistoryTable::create)),
            );
        }

        creators.insert(
            "show_grants".to_string(),
            (next_id(), Arc::new(ShowGrants::create)),
        );

        creators.insert(
            "show_variables".to_string(),
            (next_id(), Arc::new(ShowVariables::create)),
        );

        creators.insert(
            "policy_references".to_string(),
            (next_id(), Arc::new(PolicyReferencesTable::create)),
        );

        creators.insert(
            "tag_references".to_string(),
            (next_id(), Arc::new(TagReferencesTable::create)),
        );

        creators.insert(
            "udf_echo".to_string(),
            (next_id(), Arc::new(UdfEchoTable::create)),
        );

        creators.insert(
            "fuse_time_travel_size".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseTimeTravelSizeFunc>::create),
            ),
        );

        creators.insert(
            "fuse_vacuum_drop_aggregating_index".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseVacuumDropAggregatingIndex>::create),
            ),
        );

        creators.insert(
            "fuse_vacuum_drop_inverted_index".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<FuseVacuumDropInvertedIndex>::create),
            ),
        );

        creators.insert(
            "show_roles".to_string(),
            (next_id(), Arc::new(ShowRoles::create)),
        );
        creators.insert(
            "table_statistics".to_string(),
            (
                next_id(),
                Arc::new(TableFunctionTemplate::<TableStatisticsFunc>::create),
            ),
        );
        creators.insert(
            "iceberg_snapshot".to_string(),
            (next_id(), Arc::new(IcebergInspectTable::create)),
        );

        creators.insert(
            "iceberg_manifest".to_string(),
            (next_id(), Arc::new(IcebergInspectTable::create)),
        );

        creators.insert(
            "show_sequences".to_string(),
            (next_id(), Arc::new(ShowSequences::create)),
        );

        creators.insert(
            "copy_history".to_string(),
            (next_id(), Arc::new(CopyHistoryTable::create)),
        );

        TableFunctionFactory {
            creators: RwLock::new(creators),
        }
    }

    pub fn get(&self, func_name: &str, tbl_args: TableArgs) -> Result<Arc<dyn TableFunction>> {
        let lock = self.creators.read();
        let func_name = func_name.to_lowercase();
        let (id, factory) = lock.get(&func_name).ok_or_else(|| {
            ErrorCode::UnknownTable(format!("Unknown table function {}", func_name))
        })?;
        let func = factory.try_create("", &func_name, *id, tbl_args)?;
        Ok(func)
    }

    pub fn exists(&self, func_name: &str) -> bool {
        let lock = self.creators.read();
        let func_name = func_name.to_lowercase();
        lock.contains_key(&func_name)
    }

    pub fn list(&self) -> Vec<String> {
        self.creators
            .read()
            .iter()
            .map(|(name, (id, _))| (name, id))
            .sorted_by(|a, b| Ord::cmp(a.1, b.1))
            .map(|(name, _)| name.clone())
            .collect()
    }
}
