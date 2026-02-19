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

use std::assert_ne;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::vec;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_api::CatalogApi;
use databend_common_meta_api::DEFAULT_MGET_SIZE;
use databend_common_meta_api::DatabaseApi;
use databend_common_meta_api::DatamaskApi;
use databend_common_meta_api::DictionaryApi;
use databend_common_meta_api::GarbageCollectionApi;
use databend_common_meta_api::IndexApi;
use databend_common_meta_api::LockApi;
use databend_common_meta_api::RowAccessPolicyApi;
use databend_common_meta_api::SecurityApi;
use databend_common_meta_api::SequenceApi;
use databend_common_meta_api::TableApi;
use databend_common_meta_api::deserialize_struct;
use databend_common_meta_api::kv_app_error::KVAppError;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_api::serialize_struct;
use databend_common_meta_api::util::IdempotentKVTxnSender;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::CreateRowAccessPolicyReq;
use databend_common_meta_app::row_access_policy::RowAccessPolicyMeta;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateCatalogReq;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateDictionaryReq;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdHistoryIdent;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DbIdList;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::IcebergCatalogOption;
use databend_common_meta_app::schema::IcebergRestCatalogOption;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::IndexNameIdent;
use databend_common_meta_app::schema::IndexNameIdentRaw;
use databend_common_meta_app::schema::IndexType;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::ListCatalogReq;
use databend_common_meta_app::schema::ListDatabaseReq;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListTableReq;
use databend_common_meta_app::schema::LockKey;
use databend_common_meta_app::schema::MarkedDeletedIndexType;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameDictionaryReq;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SecurityPolicyColumnMap;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::SetSecurityPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReq;
use databend_common_meta_app::schema::SwapTableReq;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_meta_app::schema::TableIdList;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdentRaw;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::schema::index_id_ident::IndexId;
use databend_common_meta_app::schema::index_id_ident::IndexIdIdent;
use databend_common_meta_app::schema::index_id_to_name_ident::IndexIdToNameIdent;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_meta_app::schema::sequence_storage::SequenceStorageIdent;
use databend_common_meta_app::schema::vacuum_watermark_ident::VacuumWatermarkIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::ToTenant;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use fastrace::func_name;
use log::debug;
use log::info;

use crate::db_table_harness::DbTableHarness;
use crate::support::DroponInfo;
use crate::support::assert_meta_eq_without_updated;
use crate::support::calc_and_compare_drop_on_db_result;
use crate::support::calc_and_compare_drop_on_table_result;
use crate::support::delete_test_data;
use crate::support::upsert_test_data;
use crate::testing::get_kv_data;
use crate::testing::get_kv_u64_data;

/// Test suite of `SchemaApi`.
///
/// It is not used by this crate, but is used by other crate that impl `SchemaApi`,
/// to ensure an impl works as expected,
/// such as `meta/embedded` and `metasrv`.
#[derive(Copy, Clone)]
pub struct SchemaApiTestSuite {}

impl SchemaApiTestSuite {
    /// Test SchemaAPI on a single node
    pub async fn test_single_node<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError>
            + DatabaseApi
            + TableApi
            + CatalogApi
            + DictionaryApi
            + GarbageCollectionApi
            + IndexApi
            + LockApi
            + SecurityApi
            + DatamaskApi
            + SequenceApi
            + RowAccessPolicyApi
            + 'static,
    {
        let suite = SchemaApiTestSuite {};

        suite.run_database_tests(&b).await?;
        suite.run_table_core_tests(&b).await?;
        suite.run_datamask_api_tests(&b).await?;
        suite.run_row_access_policy_api_tests(&b).await?;
        suite.run_table_post_core_tests(&b).await?;
        suite.run_table_history_gc_tests(&b).await?;
        suite.run_sequence_api_tests(&b).await?;
        suite.run_dictionary_api_tests(&b).await?;
        suite.run_miscellaneous_tests(&b).await?;

        Ok(())
    }

    async fn run_database_tests<B, MT>(&self, b: &B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + 'static,
    {
        self.database_and_table_rename(&b.build().await).await?;
        self.database_create_get_drop(&b.build().await).await?;
        self.database_create_get_drop_in_diff_tenant(&b.build().await)
            .await?;
        self.database_list(&b.build().await).await?;
        self.database_list_in_diff_tenant(&b.build().await).await?;
        self.database_rename(&b.build().await).await?;
        self.get_tenant_history_databases(&b.build().await).await?;
        self.database_drop_undrop_list_history(&b.build().await)
            .await?;

        Ok(())
    }

    async fn run_table_core_tests<B, MT>(&self, b: &B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError>
            + DatabaseApi
            + TableApi
            + GarbageCollectionApi
            + 'static,
    {
        self.table_commit_table_meta(&b.build().await).await?;
        self.concurrent_commit_table_meta(b.clone()).await?;
        self.database_drop_out_of_retention_time_history(&b.build().await)
            .await?;
        self.table_create_get_drop(&b.build().await).await?;
        self.table_drop_without_db_id_to_name(&b.build().await)
            .await?;
        self.list_db_without_db_id_list(&b.build().await).await?;
        self.drop_table_without_table_id_list(&b.build().await)
            .await?;
        self.table_rename(&b.build().await).await?;
        self.table_swap(&b.build().await).await?;
        self.table_update_meta(&b.build().await).await?;

        Ok(())
    }

    async fn run_datamask_api_tests<B, MT>(&self, b: &B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError>
            + DatabaseApi
            + TableApi
            + SecurityApi
            + DatamaskApi
            + 'static,
    {
        self.table_update_mask_policy(&b.build().await).await?;
        self.mask_policy_drop_with_table_lifecycle(&b.build().await)
            .await?;

        Ok(())
    }

    async fn run_row_access_policy_api_tests<B, MT>(&self, b: &B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError>
            + DatabaseApi
            + TableApi
            + SecurityApi
            + RowAccessPolicyApi
            + 'static,
    {
        self.row_access_policy_drop_with_table_lifecycle(&b.build().await)
            .await?;
        self.table_update_row_access_policy(&b.build().await)
            .await?;

        Ok(())
    }

    async fn run_table_post_core_tests<B, MT>(&self, b: &B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + 'static,
    {
        self.table_upsert_option(&b.build().await).await?;
        self.table_list(&b.build().await).await?;
        self.table_list_many(&b.build().await).await?;

        Ok(())
    }

    async fn run_table_history_gc_tests<B, MT>(&self, b: &B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError>
            + CatalogApi
            + DatabaseApi
            + GarbageCollectionApi
            + IndexApi
            + LockApi
            + TableApi
            + 'static,
    {
        self.table_drop_undrop_list_history(&b.build().await)
            .await?;
        self.database_gc_out_of_retention_time(&b.build().await)
            .await?;
        self.table_gc_out_of_retention_time(&b.build().await)
            .await?;
        self.db_table_gc_out_of_retention_time(&b.build().await)
            .await?;
        self.table_drop_out_of_retention_time_history(&b.build().await)
            .await?;
        self.table_history_filter(&b.build().await).await?;
        self.table_history_filter_with_limit(&b.build().await)
            .await?;
        self.get_table_by_id(&b.build().await).await?;
        self.get_table_copied_file(&b.build().await).await?;
        self.truncate_table(&b.build().await).await?;
        self.update_table_with_copied_files(&b.build().await)
            .await?;
        self.table_index_create_drop(&b.build().await).await?;
        self.index_create_list_drop(&b.build().await).await?;
        self.table_lock_revision(&b.build().await).await?;
        self.gc_dropped_db_after_undrop(&b.build().await).await?;
        self.catalog_create_get_list_drop(&b.build().await).await?;
        self.table_least_visible_time(&b.build().await).await?;
        self.vacuum_retention_timestamp(&b.build().await).await?;
        self.drop_table_without_tableid_to_name(&b.build().await)
            .await?;

        Ok(())
    }

    async fn run_sequence_api_tests<B, MT>(&self, b: &B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError> + SequenceApi + 'static,
    {
        self.test_sequence_0(&b.build().await).await?;
        self.test_sequence_1(&b.build().await).await?;

        Ok(())
    }

    async fn run_dictionary_api_tests<B, MT>(&self, b: &B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + DictionaryApi + 'static,
    {
        self.dictionary_create_list_drop(&b.build().await).await?;

        Ok(())
    }

    async fn run_miscellaneous_tests<B, MT>(&self, b: &B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + 'static,
    {
        self.get_table_name_by_id(&b.build().await).await?;
        self.get_db_name_by_id(&b.build().await).await?;

        Ok(())
    }

    /// Test SchemaAPI on cluster
    pub async fn test_cluster<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    {
        let suite = SchemaApiTestSuite {};

        // leader-follower test
        {
            let cluster = b.build_cluster().await;
            suite
                .database_get_diff_nodes(&cluster[0], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_database_diff_nodes(&cluster[0], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_table_diff_nodes(&cluster[0], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite.table_get_diff_nodes(&cluster[0], &cluster[1]).await?;
        }

        // follower-follower test
        {
            let cluster = b.build_cluster().await;
            suite
                .database_get_diff_nodes(&cluster[2], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_database_diff_nodes(&cluster[2], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_table_diff_nodes(&cluster[2], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite.table_get_diff_nodes(&cluster[2], &cluster[1]).await?;
        }

        Ok(())
    }

    async fn database_and_table_rename<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let util = DbTableHarness::new(mt, "tenant1", "db1", "table", "JSON");
        let tenant = util.tenant();

        let db_name = "db1";
        let db2_name = "db2";
        let db3_name = "db3";
        let table_name = "table";
        let table2_name = "table2";
        let table3_name = "table3";
        let db_id;
        let db3_id;
        let table_id;

        let db_name_ident = DatabaseNameIdent::new(&tenant, db_name);
        let db2_name_ident = DatabaseNameIdent::new(&tenant, db2_name);

        {
            info!("--- prepare db1,db3 and table");
            // prepare db1
            let mut util1 = DbTableHarness::new(mt, "tenant1", "db1", "", "eng1");
            util1.create_db().await?;
            assert_eq!(1, *util1.db_id());
            db_id = util1.db_id();

            let mut util3 = DbTableHarness::new(mt, "tenant1", "db3", "", "eng1");
            util3.create_db().await?;
            db3_id = util3.db_id();

            let mut table_util = DbTableHarness::new(mt, "tenant1", "db1", "table", "JSON");
            let res = table_util.create_table().await?;
            table_id = res.0;

            let db_id_name_key = DatabaseIdToName { db_id: *db_id };
            let ret_db_name_ident: DatabaseNameIdentRaw = get_kv_data(mt, &db_id_name_key).await?;
            assert_eq!(
                ret_db_name_ident,
                DatabaseNameIdentRaw::from(&db_name_ident)
            );

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName = get_kv_data(mt, &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id: *db_id,
                table_name: table_name.to_string(),
            });
        }

        {
            info!("--- rename exists db db1 to not exists mutable db2");
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: db_name_ident.clone(),
                new_db_name: db2_name.to_string(),
            };
            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_ok());

            let db_id_2_name_key = DatabaseIdToName { db_id: *db_id };
            let ret_db_name_ident: DatabaseNameIdentRaw =
                get_kv_data(mt, &db_id_2_name_key).await?;
            assert_eq!(
                ret_db_name_ident,
                DatabaseNameIdentRaw::from(&db2_name_ident)
            );

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName = get_kv_data(mt, &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id: *db_id,
                table_name: table_name.to_string(),
            });
        }

        {
            info!("--- rename exists table1 to not exists mutable table2");
            let got = mt
                .rename_table(RenameTableReq {
                    if_exists: true,
                    name_ident: TableNameIdent {
                        tenant: tenant.clone(),
                        db_name: db2_name.to_string(),
                        table_name: table_name.to_string(),
                    },
                    new_db_name: db2_name.to_string(),
                    new_table_name: table2_name.to_string(),
                })
                .await;
            debug!("--- rename table on unknown database got: {:?}", got);

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName = get_kv_data(mt, &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id: *db_id,
                table_name: table2_name.to_string(),
            });
        }

        {
            info!("--- rename exists table1 to not exists mutable db3.table3");
            let got = mt
                .rename_table(RenameTableReq {
                    if_exists: true,
                    name_ident: TableNameIdent {
                        tenant: tenant.clone(),
                        db_name: db2_name.to_string(),
                        table_name: table2_name.to_string(),
                    },
                    new_db_name: db3_name.to_string(),
                    new_table_name: table3_name.to_string(),
                })
                .await;
            debug!("--- rename table on unknown database got: {:?}", got);

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName = get_kv_data(mt, &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id: *db3_id,
                table_name: table3_name.to_string(),
            });
        }

        Ok(())
    }

    async fn database_create_get_drop<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let mut util = DbTableHarness::new(mt, tenant_name, "db1", "", "github");
        let tenant = util.tenant();
        info!("--- create db1");
        {
            let res = util.create_db().await;
            info!("create database res: {:?}", res);
            res.unwrap();
            assert_eq!(1, *util.db_id(), "first database id is 1");
        }

        info!("--- create db1 again with if_not_exists=false");
        {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                catalog_name: None,
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::DATABASE_ALREADY_EXISTS,
                ErrorCode::from(err).code()
            );
        }

        info!("--- create db1 again with if_not_exists=true");
        {
            let req = CreateDatabaseReq {
                create_option: CreateOption::CreateIfNotExists,
                catalog_name: None,
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, *res.db_id, "db1 id is 1");
        }

        info!("--- get db1");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "db1"))
                .await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.database_id.db_id, "db1 id is 1");
            assert_eq!("db1", res.name_ident.database_name(), "db1.db is db1");
        }

        info!("--- create db2");
        {
            let mut util2 = DbTableHarness::new(mt, tenant_name, "db2", "", "");
            let res = util2.create_db().await;
            info!("create database res: {:?}", res);
            res.unwrap();
            assert_eq!(
                6,
                *util2.db_id(),
                "second database id is 4: seq increment but no used"
            );
        }

        info!("--- get db2");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "db2"))
                .await?;
            assert_eq!("db2", res.name_ident.database_name(), "db1.db is db1");
        }

        info!("--- get absent db");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "absent"))
                .await;
            debug!("=== get absent database res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            let err_code = ErrorCode::from(err);

            assert_eq!(1003, err_code.code());
            assert!(err_code.message().contains("absent"));
        }

        info!("--- drop db2");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, "db2"),
            })
            .await?;
        }

        info!("--- get db2 should not found");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "db2"))
                .await;
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, ErrorCode::from(err).code());
        }

        info!("--- drop db2 with if_exists=true returns no error");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: true,
                name_ident: DatabaseNameIdent::new(&tenant, "db2"),
            })
            .await?;
        }

        info!("--- create or replace db1");
        {
            let new_engine = "new github";
            let db_name = DatabaseNameIdent::new(&tenant, "db1");
            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "db1"))
                .await?;
            debug!("get present database res: {:?}", res);
            assert_ne!(res.meta.engine, new_engine.to_string());

            let orig_db_id: u64 = get_kv_u64_data(mt, &db_name).await?;
            assert_eq!(orig_db_id, res.database_id.db_id);
            let db_id_name_key = DatabaseIdToName { db_id: orig_db_id };
            let ret_db_name_ident: DatabaseNameIdentRaw = get_kv_data(mt, &db_id_name_key).await?;
            assert_eq!(ret_db_name_ident, DatabaseNameIdentRaw::from(&db_name));

            let req = CreateDatabaseReq {
                create_option: CreateOption::CreateOrReplace,
                catalog_name: Some("default".to_string()),
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
                meta: DatabaseMeta {
                    engine: new_engine.to_string(),
                    ..Default::default()
                },
            };

            mt.create_database(req).await?;

            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "db1"))
                .await?;
            debug!("get present database res: {:?}", res);
            assert_eq!(res.meta.engine, new_engine.to_string());

            let db_id: u64 = get_kv_u64_data(mt, &db_name).await?;
            assert_eq!(db_id, res.database_id.db_id);
            let db_id_name_key = DatabaseIdToName { db_id };
            let ret_db_name_ident: DatabaseNameIdentRaw = get_kv_data(mt, &db_id_name_key).await?;
            assert_eq!(ret_db_name_ident, DatabaseNameIdentRaw::from(&db_name));
            assert_ne!(db_id, orig_db_id);
        }

        Ok(())
    }

    async fn database_create_get_drop_in_diff_tenant<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        info!("--- tenant1 create db1");
        let mut util1 = DbTableHarness::new(mt, "tenant1", "db1", "", "github");
        util1.create_db().await?;
        let db_id_1 = util1.db_id();
        assert_eq!(1, *db_id_1, "first database id is 1");
        let tenant1 = util1.tenant();

        info!("--- tenant1 create db2");
        let mut util2 = DbTableHarness::new(mt, "tenant1", "db2", "", "github");
        util2.create_db().await?;
        let db_id_2 = util2.db_id();
        assert!(*db_id_2 > *db_id_1, "second database id is > {}", db_id_1);

        info!("--- tenant2 create db1");
        let mut util3 = DbTableHarness::new(mt, "tenant2", "db1", "", "github");
        util3.create_db().await?;
        let tenant2 = util3.tenant();
        assert!(*util3.db_id() > *db_id_2, "third database id > {}", db_id_2);

        info!("--- tenant1 get db1");
        {
            let res = util1.get_database().await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.database_id.db_id, "db1 id is 1");
            assert_eq!("db1", res.name_ident.database_name(), "db1.db is db1");
        }

        info!("--- tenant1 get absent db");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant1.clone(), "absent"))
                .await;
            debug!("=== get absent database res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            let err = ErrorCode::from(err);

            assert_eq!(ErrorCode::UNKNOWN_DATABASE, err.code());
            assert!(err.message().contains("absent"));
        }

        info!("--- tenant2 get tenant1's db2");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant2, "db2")).await;
            debug!("=== get other tenant's database res: {:?}", res);
            assert!(res.is_err());
            let res = res.unwrap_err();
            let err = ErrorCode::from(res);

            assert_eq!(ErrorCode::UNKNOWN_DATABASE, err.code());
            assert_eq!("Unknown database 'db2'".to_string(), err.message());
        }

        info!("--- drop db2");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant1, "db2"),
            })
            .await?;
        }

        info!("--- tenant1 get db2 should not found");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant1.clone(), "db2"))
                .await;
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, ErrorCode::from(err).code());
        }

        info!("--- tenant1 drop db2 with if_exists=true returns no error");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: true,
                name_ident: DatabaseNameIdent::new(&tenant1, "db2"),
            })
            .await?;
        }

        Ok(())
    }

    async fn database_list<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        info!("--- prepare db1 and db2");
        let mut db_ids = vec![];
        let db_names = ["db1", "db2"];
        let engines = ["eng1", "eng2"];
        let tenant = Tenant::new_or_err("tenant1", func_name!())?;
        {
            let mut util1 = DbTableHarness::new(mt, "tenant1", "db1", "", "eng1");
            util1.create_db().await?;
            assert_eq!(1, *util1.db_id());
            db_ids.push(util1.db_id());

            let mut util2 = DbTableHarness::new(mt, "tenant1", "db2", "", "eng2");
            util2.create_db().await?;
            assert!(*util2.db_id() > 1);
            db_ids.push(util2.db_id());
        }

        info!("--- list_databases");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant.clone(),
                })
                .await?;

            let got = dbs.iter().map(|x| x.database_id.db_id).collect::<Vec<_>>();
            assert_eq!(db_ids.iter().map(|x| **x).collect::<Vec<_>>(), got);

            for (i, db_info) in dbs.iter().enumerate() {
                assert_eq!(tenant.tenant_name(), db_info.name_ident.tenant_name());
                assert_eq!(db_names[i], db_info.name_ident.database_name());
                assert_eq!(db_ids[i], db_info.database_id);
                assert_eq!(engines[i], db_info.meta.engine);
            }
        }

        Ok(())
    }

    async fn database_list_in_diff_tenant<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        info!("--- prepare db1 and db2");
        let tenant1 = Tenant::new_or_err("tenant1", func_name!())?;
        let tenant2 = Tenant::new_or_err("tenant2", func_name!())?;

        let mut db_ids = vec![];
        {
            let mut util1 = DbTableHarness::new(mt, "tenant1", "db1", "", "eng1");
            util1.create_db().await?;
            assert_eq!(1, *util1.db_id());
            db_ids.push(util1.db_id());

            let mut util2 = DbTableHarness::new(mt, "tenant1", "db2", "", "eng2");
            util2.create_db().await?;
            assert!(*util2.db_id() > 1);
            db_ids.push(util2.db_id());
        }

        let db_id_3 = {
            let mut util3 = DbTableHarness::new(mt, "tenant2", "db3", "", "eng1");
            util3.create_db().await?;
            util3.db_id()
        };

        info!("--- get_databases by tenant1");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant1.clone(),
                })
                .await?;
            let got = dbs.iter().map(|x| x.database_id.db_id).collect::<Vec<_>>();
            assert_eq!(db_ids.iter().map(|x| **x).collect::<Vec<_>>(), got)
        }

        info!("--- get_databases by tenant2");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant2.clone(),
                })
                .await?;
            let want: Vec<u64> = vec![*db_id_3];
            let got = dbs.iter().map(|x| x.database_id.db_id).collect::<Vec<_>>();
            assert_eq!(want, got)
        }

        Ok(())
    }

    async fn database_rename<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = Tenant::new_or_err("tenant1", func_name!())?;
        let db_name = "db1";
        let db2_name = "db2";
        let new_db_name = "db3";

        info!("--- rename not exists db1 to not exists db2");
        {
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                new_db_name: new_db_name.to_string(),
            };

            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_err());
            assert_eq!(
                ErrorCode::UNKNOWN_DATABASE,
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        info!("--- prepare db1 and db2");
        {
            // prepare db1
            let mut util1 = DbTableHarness::new(mt, "tenant1", "db1", "", "eng1");
            util1.create_db().await?;
            assert_eq!(1, *util1.db_id());

            info!("--- rename not exists db4 to exists db1");
            {
                let req = RenameDatabaseReq {
                    if_exists: false,
                    name_ident: DatabaseNameIdent::new(&tenant, "db4"),
                    new_db_name: db_name.to_string(),
                };

                let res = mt.rename_database(req).await;
                info!("rename database res: {:?}", res);
                assert!(res.is_err());
                assert_eq!(
                    ErrorCode::UNKNOWN_DATABASE,
                    ErrorCode::from(res.unwrap_err()).code()
                );
            }

            // prepare db2
            let mut util2 = DbTableHarness::new(mt, "tenant1", "db2", "", "eng1");
            util2.create_db().await?;
            assert!(*util2.db_id() > 1);
        }

        info!("--- rename exists db db1 to exists db db2");
        {
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                new_db_name: db2_name.to_string(),
            };

            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_err());
            assert_eq!(
                ErrorCode::DATABASE_ALREADY_EXISTS,
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        info!("--- rename exists db db1 to not exists mutable db");
        {
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),

                new_db_name: new_db_name.to_string(),
            };
            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_ok());

            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), new_db_name))
                .await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.database_id.db_id, "db3 id is 1");
            assert_eq!("db3", res.name_ident.database_name(), "db3.db is db3");

            info!("--- get old database after rename");
            {
                let res = mt.get_database(GetDatabaseReq::new(tenant, db_name)).await;
                let err = res.err().unwrap();
                assert_eq!(ErrorCode::UNKNOWN_DATABASE, ErrorCode::from(err).code());
            }
        }

        Ok(())
    }

    async fn get_tenant_history_databases<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant_get_tenant_history_databases";
        let db_name_1 = "db1_get_tenant_history_database";
        let db_name_2 = "db2_get_tenant_history_database";

        let mut util1 = DbTableHarness::new(mt, tenant_name, db_name_1, "", "eng");
        let mut util2 = DbTableHarness::new(mt, tenant_name, db_name_2, "", "eng");

        info!("--- create dropped db1 and db2; db2 is non-retainable");
        {
            util1.create_db().await?;
            util1.drop_db().await?;

            util2.create_db().await?;
            util2.drop_db().await?;

            info!("--- update db2's drop_on");
            {
                let dbid2 = *util2.db_id();
                let db2 = mt.get_pb(&DatabaseId { db_id: dbid2 }).await?;
                let mut db2 = db2.unwrap().data;
                db2.drop_on = Some(Utc::now() - Duration::days(1000));

                mt.upsert_pb(&UpsertPB::update(DatabaseId { db_id: dbid2 }, db2))
                    .await?;
            }
        }
        let res = mt
            .get_tenant_history_databases(
                ListDatabaseReq {
                    tenant: util1.tenant(),
                },
                false,
            )
            .await?;

        assert_eq!(1, res.len());

        let res = mt
            .get_tenant_history_databases(
                ListDatabaseReq {
                    tenant: util1.tenant(),
                },
                true,
            )
            .await?;

        assert_eq!(2, res.len());

        Ok(())
    }

    async fn database_drop_undrop_list_history<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1_database_drop_undrop_list_history";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;
        let db_name = "db1_database_drop_undrop_list_history";
        let new_db_name = "db2_database_drop_undrop_list_history";

        let db_name_ident = DatabaseNameIdent::new(&tenant, db_name);
        let new_db_name_ident = DatabaseNameIdent::new(&tenant, new_db_name);

        info!("--- create and drop db1");
        {
            // first create database
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                catalog_name: None,
                name_ident: db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, *res.db_id, "first database id is 1");

            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);

            // then drop db1
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // undrop db1
            mt.undrop_database(UndropDatabaseReq {
                name_ident: db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);
        }

        info!("--- drop and create db1");
        {
            // first drop db1
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // then create database
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                catalog_name: None,
                name_ident: db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            mt.create_database(req).await?;

            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 1,
            }]);
        }

        info!("--- create and rename db2");
        {
            // first create db2
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                catalog_name: None,
                name_ident: new_db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            mt.create_database(req).await?;

            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![
                DroponInfo {
                    name: db_name_ident.to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_db_name_ident.to_string_key(),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 1,
                },
            ]);

            // then drop db2
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: new_db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![
                DroponInfo {
                    name: db_name_ident.to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_db_name_ident.to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
            ]);

            // rename db1 to db2
            mt.rename_database(RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                new_db_name: new_db_name.to_string(),
            })
            .await?;
            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![
                DroponInfo {
                    name: db_name_ident.to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
                DroponInfo {
                    name: new_db_name_ident.to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
            ]);
        }

        Ok(())
    }

    async fn catalog_create_get_list_drop<MT: kvapi::KVApi<Error = MetaError> + CatalogApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";

        let catalog_name = "catalog1";

        let tenant = Tenant {
            tenant: tenant_name.to_string(),
        };

        let name_ident = CatalogNameIdent::new(tenant.clone(), catalog_name);

        info!("--- create catalog1");
        let req = CreateCatalogReq {
            if_not_exists: false,
            name_ident: name_ident.clone(),
            meta: CatalogMeta {
                catalog_option: CatalogOption::Iceberg(IcebergCatalogOption::Rest(
                    IcebergRestCatalogOption {
                        uri: "http://127.0.0.1:8080".to_string(),
                        warehouse: "test".to_string(),
                        props: HashMap::default(),
                    },
                )),
                created_on: Utc::now(),
            },
        };

        let res = mt.create_catalog(&req.name_ident, &req.meta).await?;
        info!("create catalog res: {:?}", res);

        let res = res.unwrap();

        let got = mt.get_catalog(&name_ident).await?;
        assert_eq!(got.id.catalog_id, *res);
        assert_eq!(got.name_ident.tenant, "tenant1");
        assert_eq!(got.name_ident.catalog_name, "catalog1");

        let got = mt
            .list_catalogs(ListCatalogReq::new(tenant.clone()))
            .await?;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name_ident.tenant, "tenant1");
        assert_eq!(got[0].name_ident.catalog_name, "catalog1");

        mt.drop_catalog(&name_ident).await?;

        let got = mt
            .list_catalogs(ListCatalogReq::new(tenant.clone()))
            .await?;
        assert_eq!(got.len(), 0);

        Ok(())
    }

    async fn drop_table_without_tableid_to_name<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let db = "db";
        let table_name = "tbl";

        let mut util = DbTableHarness::new(mt, tenant_name, db, table_name, "");
        let tenant = util.tenant();
        util.create_db().await?;
        let db_id = util.db_id();

        let (table_id, _table_meta) = util
            .create_table_with(
                |mut meta| {
                    meta.schema = Arc::new(TableSchema::new(vec![TableField::new(
                        "number",
                        TableDataType::Number(NumberDataType::UInt64),
                    )])); // Use simple schema (only number field)
                    meta.engine = "JSON".to_string(); // Util was constructed with empty engine
                    meta.options = BTreeMap::new(); // Different from Util's default options
                    meta
                },
                |req| req,
            )
            .await?;

        let table_id_to_name = TableIdToName { table_id };
        // delete TableIdToName before drop table
        delete_test_data(mt, &table_id_to_name).await?;

        mt.drop_table_by_id(DropTableByIdReq {
            if_exists: false,
            tenant: tenant.clone(),
            db_id: *db_id,
            db_name: "db".to_string(),
            table_name: table_name.to_string(),
            tb_id: table_id,
            engine: "FUSE".to_string(),
            temp_prefix: "".to_string(),
        })
        .await?;

        Ok(())
    }

    async fn table_least_visible_time<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb1";
        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        info!("--- prepare db and create table");
        let table_id;
        {
            let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "");
            util.create_db().await?;

            let created_on = Utc::now();
            let (tid, _table_meta) = util
                .create_table_with(
                    |mut meta| {
                        meta.schema = schema(); // Use local schema function (only number field)
                        meta.engine = "JSON".to_string(); // Util was constructed with empty engine
                        meta.updated_on = created_on;
                        meta.created_on = created_on;
                        meta
                    },
                    |req| req,
                )
                .await?;
            table_id = tid;
        }

        info!("--- test lvt");
        {
            let time_small = DateTime::<Utc>::from_timestamp(102, 0).unwrap();
            let time_big = DateTime::<Utc>::from_timestamp(1024, 0).unwrap();
            let time_bigger = DateTime::<Utc>::from_timestamp(1025, 0).unwrap();

            let lvt_small = LeastVisibleTime::new(time_small);
            let lvt_big = LeastVisibleTime::new(time_big);
            let lvt_bigger = LeastVisibleTime::new(time_bigger);

            let lvt_name_ident = LeastVisibleTimeIdent::new(&tenant, table_id);

            let res = mt.get_table_lvt(&lvt_name_ident).await?;
            assert!(res.is_none());

            let res = mt.set_table_lvt(&lvt_name_ident, &lvt_big).await?;
            assert_eq!(res.time, time_big);
            let res = mt.get_table_lvt(&lvt_name_ident).await?;
            assert_eq!(res.unwrap().time, time_big);

            // test lvt never fall back

            let res = mt.set_table_lvt(&lvt_name_ident, &lvt_small).await?;
            assert_eq!(res.time, time_big);
            let res = mt.get_table_lvt(&lvt_name_ident).await?;
            assert_eq!(res.unwrap().time, time_big);

            let res = mt.set_table_lvt(&lvt_name_ident, &lvt_bigger).await?;
            assert_eq!(res.time, time_bigger);
            let res = mt.get_table_lvt(&lvt_name_ident).await?;
            assert_eq!(res.unwrap().time, time_bigger);
        }

        Ok(())
    }

    async fn vacuum_retention_timestamp<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + GarbageCollectionApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "vacuum_retention_timestamp";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        // Test basic timestamp operations - monotonic property
        let first = DateTime::<Utc>::from_timestamp(1_000, 0).unwrap();
        let earlier = DateTime::<Utc>::from_timestamp(500, 0).unwrap();
        let later = DateTime::<Utc>::from_timestamp(2_000, 0).unwrap();

        // Test fetch_set_vacuum_timestamp with correct return semantics
        let old_retention = mt.fetch_set_vacuum_timestamp(&tenant, first).await?;
        // Should return None as old value since never set before
        assert_eq!(old_retention, None);

        // Attempt to set earlier timestamp should return current value (first) unchanged
        let old_retention = mt.fetch_set_vacuum_timestamp(&tenant, earlier).await?;
        assert_eq!(old_retention.unwrap().time, first); // Should return the PREVIOUS value

        // Set later timestamp should work and return previous value (first)
        let old_retention = mt.fetch_set_vacuum_timestamp(&tenant, later).await?;
        assert_eq!(old_retention.unwrap().time, first); // Should return PREVIOUS value (first)

        // Verify current stored value
        let vacuum_ident = VacuumWatermarkIdent::new_global(tenant.clone());
        let stored = mt.get_pb(&vacuum_ident).await?;
        assert_eq!(stored.unwrap().data.time, later);

        // Test undrop retention guard behavior
        {
            let mut util = DbTableHarness::new(
                mt,
                tenant_name,
                "db_retention_guard",
                "tbl_retention_guard",
                "FUSE",
            );
            util.create_db().await?;
            let (table_id, _table_meta) = util.create_table().await?;
            util.drop_table_by_id().await?;

            let table_meta = mt
                .get_pb(&TableId::new(table_id))
                .await?
                .expect("dropped table meta must exist");
            let drop_time = table_meta
                .data
                .drop_on
                .expect("dropped table should carry drop_on timestamp");

            // Set retention timestamp after drop time to block undrop
            let retention_candidate = drop_time + chrono::Duration::seconds(1);
            let old_retention = mt
                .fetch_set_vacuum_timestamp(&tenant, retention_candidate)
                .await?
                .unwrap();
            // Should return the previous retention time (later)
            assert_eq!(old_retention.time, later);

            // Undrop should now fail due to retention guard
            let undrop_err = mt
                .undrop_table(UndropTableReq {
                    name_ident: TableNameIdent::new(&tenant, util.db_name(), util.tbl_name()),
                })
                .await
                .expect_err("undrop must fail once vacuum retention blocks it");

            match undrop_err {
                KVAppError::AppError(AppError::UndropTableRetentionGuard(e)) => {
                    assert_eq!(e.drop_time(), drop_time);
                    assert_eq!(e.retention(), retention_candidate);
                }
                other => panic!("unexpected undrop error: {other:?}"),
            }
        }

        Ok(())
    }

    async fn table_create_get_drop<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let mut util = DbTableHarness::new(mt, tenant_name, "db1", "tb2", "");
        let tenant = util.tenant();

        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            updated_on: created_on,
            created_on,
            ..TableMeta::default()
        };

        let unknown_database_code = ErrorCode::UNKNOWN_DATABASE;

        info!("--- create or get table on unknown db");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                catalog_name: None,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },

                table_meta: table_meta(created_on),
                as_dropped: false,
                table_properties: None,
                table_partition: None,
            };
            // test create table
            {
                let res = mt.create_table(req).await;
                debug!("create table on unknown db res: {:?}", res);

                assert!(res.is_err());
                let err = res.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(unknown_database_code, err.code());
            };
            // test get table
            {
                let req = GetTableReq::new(&tenant, db_name, tbl_name);
                let got = mt.get_table(req).await;
                debug!("get table on unknown db got: {:?}", got);

                assert!(got.is_err());
                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(unknown_database_code, err.code());
            }
        }

        info!("--- prepare db");
        util.create_db().await?;
        let db_id = util.db_id();
        assert_eq!(1, *db_id, "first database id is 1");

        info!("--- create tb2 and get table");
        let created_on = Utc::now();
        let tb_ident_2 = {
            {
                let old_db = util.get_database().await?;
                let (table_id, _) = util
                    .create_table_with(
                        |mut meta| {
                            meta.schema = schema(); // Use local schema function (only number field)
                            meta.engine = "JSON".to_string();
                            meta.updated_on = created_on;
                            meta.created_on = created_on;
                            meta
                        },
                        |req| req,
                    )
                    .await?;
                let cur_db = util.get_database().await?;
                assert!(old_db.meta.seq < cur_db.meta.seq);
                assert!(table_id >= 1, "table id >= 1");

                let tb_id = table_id;

                let got = util.get_table().await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident,
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                    ..Default::default()
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            }
        };
        info!("--- create table again with if_not_exists = true");
        {
            let (table_id, _) = util
                .create_table_with(
                    |mut meta| {
                        meta.schema = schema(); // Use local schema function (only number field)
                        meta.engine = "JSON".to_string();
                        meta.updated_on = created_on;
                        meta.created_on = created_on;
                        meta
                    },
                    |mut req| {
                        req.create_option = CreateOption::CreateIfNotExists;
                        req
                    },
                )
                .await?;
            assert_eq!(
                tb_ident_2.table_id, table_id,
                "new table id is still the same"
            );

            let got = util.get_table().await?;
            let want = TableInfo {
                ident: tb_ident_2,
                desc: format!("'{}'.'{}'", db_name, tbl_name),
                name: tbl_name.into(),
                meta: table_meta(created_on),
                ..Default::default()
            };
            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
        }

        info!("--- create table again with if_not_exists = false");
        {
            let req = CreateTableReq {
                create_option: CreateOption::Create,
                catalog_name: None,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
                table_properties: None,
                table_partition: None,
            };
            let res = mt.create_table(req).await;
            info!("create table res: {:?}", res);

            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(
                format!(
                    "TableAlreadyExists. Code: 2302, Text = Table '{}' already exists.",
                    tbl_name
                ),
                err_code.to_string()
            );

            // get_table returns the old table

            let got = util.get_table().await.unwrap();
            let want = TableInfo {
                ident: tb_ident_2,
                desc: format!("'{}'.'{}'", db_name, tbl_name),
                name: tbl_name.into(),
                meta: table_meta(created_on),
                ..Default::default()
            };
            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get old table");
        }

        info!("--- create another table");
        {
            let created_on = Utc::now();

            let old_db = util.get_database().await?;
            let (table_id, _) = util
                .create_table_with(
                    |mut meta| {
                        meta.schema = schema();
                        meta.engine = "JSON".to_string();
                        meta.options = options();
                        meta.updated_on = created_on;
                        meta.created_on = created_on;
                        meta
                    },
                    |mut req| {
                        req.name_ident.table_name = "tb3".to_string();
                        req
                    },
                )
                .await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            assert!(
                table_id > tb_ident_2.table_id,
                "table id > {}",
                tb_ident_2.table_id
            );
        }

        let tb_info = util.get_table().await?;
        let tb_id = tb_info.ident.table_id;
        info!("--- drop table");
        {
            info!("--- drop table with if_exists = false");
            {
                let plan = DropTableByIdReq {
                    if_exists: false,
                    tenant: tenant.clone(),
                    db_id: *db_id,
                    db_name: "db1".to_string(),
                    table_name: tbl_name.to_string(),
                    tb_id,
                    engine: "FUSE".to_string(),
                    temp_prefix: "".to_string(),
                };
                mt.drop_table_by_id(plan.clone()).await?;

                info!("--- get table after drop");
                {
                    let req = GetTableReq::new(&tenant, db_name, tbl_name);
                    let res = mt.get_table(req).await;
                    let status = res.err().unwrap();
                    let err_code = ErrorCode::from(status);

                    assert_eq!(
                        format!(
                            "UnknownTable. Code: 1025, Text = Unknown table '{:}'.",
                            tbl_name
                        ),
                        err_code.to_string(),
                        "get dropped table {}",
                        tbl_name
                    );
                }

                info!("--- get table history after drop");
                {
                    let cur_db = util.get_database().await?;

                    let got = mt
                        .get_retainable_tables(&TableIdHistoryIdent {
                            database_id: cur_db.database_id.db_id,
                            table_name: tbl_name.to_string(),
                        })
                        .await
                        .unwrap()[0]
                        .clone();
                    let want = TableInfo {
                        ident: tb_ident_2,
                        desc: format!("'{}'.'{}'", db_name, tbl_name),
                        name: tbl_name.into(),
                        meta: table_meta(created_on),
                        ..Default::default()
                    };
                    assert_eq!(got.1.data.created_on, want.meta.created_on);
                    assert!(got.1.data.drop_on.is_some());
                }
            }

            info!("--- drop table with if_exists = false again, error");
            {
                let plan = DropTableByIdReq {
                    if_exists: false,
                    tenant: tenant.clone(),
                    db_id: *db_id,
                    table_name: tbl_name.to_string(),
                    tb_id,
                    db_name: "db3".to_string(),
                    engine: "FUSE".to_string(),
                    temp_prefix: "".to_string(),
                };
                let res = mt.drop_table_by_id(plan).await;
                let err = res.unwrap_err();
                assert_eq!(
                    ErrorCode::UNKNOWN_TABLE,
                    ErrorCode::from(err).code(),
                    "drop table {} with if_exists=false again",
                    tbl_name
                );
            }

            info!("--- drop table with if_exists = true again, ok");
            {
                let plan = DropTableByIdReq {
                    if_exists: true,
                    tenant: tenant.clone(),
                    db_id: *db_id,
                    table_name: tbl_name.to_string(),
                    tb_id,
                    db_name: "db3".to_string(),
                    engine: "FUSE".to_string(),
                    temp_prefix: "".to_string(),
                };
                mt.drop_table_by_id(plan.clone()).await?;
            }
        }

        info!("--- create or replace db1");
        {
            let old_created_on = Utc::now();
            let table = "test_replace";

            let key_dbid_tbname = DBIdTableName {
                db_id: *db_id,
                table_name: table.to_string(),
            };

            let (old_table_id, _) = util
                .create_table_with(
                    |mut meta| {
                        meta.schema = schema();
                        meta.engine = "JSON".to_string();
                        meta.options = options();
                        meta.updated_on = old_created_on;
                        meta.created_on = old_created_on;
                        meta
                    },
                    |mut req| {
                        req.name_ident.table_name = table.to_string();
                        req
                    },
                )
                .await?;

            let res = util.get_table_by_name(table).await?;
            assert_eq!(res.meta.created_on, old_created_on);

            let orig_table_id: u64 = get_kv_u64_data(mt, &key_dbid_tbname).await?;
            assert_eq!(orig_table_id, old_table_id);
            let key_table_id_to_name = TableIdToName {
                table_id: res.ident.table_id,
            };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt, &key_table_id_to_name).await?;
            assert_eq!(ret_table_name_ident, key_dbid_tbname);

            let created_on = Utc::now();

            let (table_id, _) = util
                .create_table_with(
                    |mut meta| {
                        meta.schema = schema();
                        meta.engine = "JSON".to_string();
                        meta.options = options();
                        meta.updated_on = created_on;
                        meta.created_on = created_on;
                        meta
                    },
                    |mut req| {
                        req.create_option = CreateOption::CreateOrReplace;
                        req.name_ident.table_name = table.to_string();
                        req
                    },
                )
                .await?;

            // table meta has been changed
            let res = util.get_table_by_name(table).await?;
            assert_eq!(res.meta.created_on, created_on);

            assert_eq!(table_id, get_kv_u64_data(mt, &key_dbid_tbname).await?);
            let key_table_id_to_name = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt, &key_table_id_to_name).await?;
            assert_eq!(ret_table_name_ident, key_dbid_tbname);
        }

        {
            info!("--- create table as dropped, undrop table by id, etc");

            // recall that there is no table named with "tbl_dropped"
            // - create table as dropped
            let created_on = Utc::now();
            let tbl_name = "tbl_dropped";
            let tbl_meta = {
                let mut v = table_meta(created_on);
                v.drop_on = Some(Utc::now());
                v
            };

            let create_table_req = CreateTableReq {
                create_option: CreateOption::Create,
                catalog_name: None,
                name_ident: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: tbl_meta,
                as_dropped: true,
                table_properties: None,
                table_partition: None,
            };
            let old_db = util.get_database().await?;
            let create_table_as_dropped_resp = mt.create_table(create_table_req.clone()).await?;

            // - verify that table created is invisible
            let req = GetTableReq::new(&tenant, db_name, tbl_name);
            let got = mt.get_table(req).await;
            use databend_common_meta_app::app_error::AppError;
            assert!(matches!(
                got.unwrap_err(),
                KVAppError::AppError(AppError::UnknownTable(_))
            ));

            // - verify other states are as expected
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            assert!(create_table_as_dropped_resp.table_id >= 1, "table id >= 1");

            // -- verify this table is committpable

            {
                let commit_table_req = CommitTableMetaReq {
                    name_ident: create_table_req.name_ident.clone(),
                    db_id: create_table_as_dropped_resp.db_id,
                    table_id: create_table_as_dropped_resp.table_id,
                    prev_table_id: create_table_as_dropped_resp.prev_table_id,
                    orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
                };

                mt.commit_table_meta(commit_table_req).await?;
                // after "un-drop", table should be visible
                let req = GetTableReq::new(&tenant, db_name, tbl_name);
                let tbl = mt.get_table(req).await?;
                // and it should be one which is specified by table id
                assert_eq!(tbl.ident.table_id, create_table_as_dropped_resp.table_id);
            }

            // -- create if not exist (as dropped) should work as expected
            {
                // recall that there is table of same name existing
                // case 1: table exists
                let create_table_if_not_exist_req = {
                    let mut req = create_table_req.clone();
                    req.create_option = CreateOption::CreateIfNotExists;
                    req
                };

                let create_if_not_exist_resp =
                    mt.create_table(create_table_if_not_exist_req).await?;
                // no new table should be created
                assert!(!create_if_not_exist_resp.new_table);
                // the tabled id that returned should be the same
                assert_eq!(
                    create_if_not_exist_resp.table_id,
                    create_table_as_dropped_resp.table_id
                );

                // table should still visible
                let req = GetTableReq::new(&tenant, db_name, tbl_name);
                mt.get_table(req).await?;

                // case 2: table does not exist
                // let's use a brand-new table name "not_exist"
                let create_table_if_not_exist_req = {
                    let mut req = create_table_req.clone();
                    req.name_ident.table_name = "not_exist".to_owned();
                    req.create_option = CreateOption::CreateIfNotExists;
                    req
                };

                let create_if_not_exist_resp =
                    mt.create_table(create_table_if_not_exist_req).await?;
                // new table should be created
                assert!(create_if_not_exist_resp.new_table);
                // table should not be visible
                let req = GetTableReq::new(&tenant, db_name, "not_exist");
                let got = mt.get_table(req).await;
                assert!(matches!(
                    got.unwrap_err(),
                    KVAppError::AppError(AppError::UnknownTable(_))
                ));
            }

            // -- create or replace (as dropped) should work as expected

            {
                let create_or_replace_req = {
                    let mut req = create_table_req.clone();
                    req.create_option = CreateOption::CreateOrReplace;
                    req
                };

                let create_or_replace_resp = mt.create_table(create_or_replace_req).await?;
                // since table of same name has been created, "new_table" (in the sense of table name) should be false
                assert!(!create_or_replace_resp.new_table);
                // but a table of different id should be created
                assert_ne!(
                    create_or_replace_resp.table_id,
                    create_table_as_dropped_resp.table_id
                );

                // the replaced table should be still visible:
                let req = GetTableReq::new(&tenant, db_name, tbl_name);
                let tbl = mt.get_table(req).await?;
                // the visible one should be the one before the create-or-replace-as-dropped
                assert_eq!(create_table_as_dropped_resp.table_id, tbl.ident.table_id);
                // but not the newly created (as dropped) one
                assert_ne!(create_or_replace_resp.table_id, tbl.ident.table_id);
            }
        }

        Ok(())
    }

    async fn table_drop_without_db_id_to_name<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi {
        let mut util = DbTableHarness::new(mt, "tenant1", "db1", "tb2", "JSON");

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (_tid, _table_meta) = util.create_table().await?;
        }

        info!("--- drop db-id-to-name mapping to ensure dropping table does not rely on it");
        {
            let id_to_name_key = DatabaseIdToName {
                db_id: *util.db_id(),
            };
            util.meta_api()
                .upsert_pb(&UpsertPB::delete(id_to_name_key))
                .await?;
        }

        info!("--- drop table to ensure dropping table does not rely on db-id-to-name");
        util.drop_table_by_id().await?;
        Ok(())
    }

    async fn list_db_without_db_id_list<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi {
        // test drop a db without db_id_list
        {
            let tenant_name = "tenant1";
            let tenant = Tenant::new_literal(tenant_name);

            let db = "db1";
            let mut util = DbTableHarness::new(mt, tenant_name, db, "tb2", "JSON");
            util.create_db().await?;

            // remove db id list
            let dbid_idlist = DatabaseIdHistoryIdent::new(&tenant, db);
            util.meta_api()
                .upsert_pb(&UpsertPB::delete(dbid_idlist.clone()))
                .await?;

            // drop db
            util.drop_db().await?;

            // after drop db, check if db id list has been added
            let value = util.meta_api().get_kv(&dbid_idlist.to_string_key()).await?;

            assert!(value.is_some());
            let seqv = value.unwrap();
            let db_id_list: DbIdList = deserialize_struct(&seqv.data)?;
            assert_eq!(db_id_list.id_list[0], *util.db_id());
        }
        // test get_tenant_history_databases can return db without db_id_list
        {
            let tenant2_name = "tenant2";
            let tenant2 = Tenant::new_literal(tenant2_name);

            let db = "db2";
            let mut util = DbTableHarness::new(mt, tenant2_name, db, "tb2", "JSON");
            util.create_db().await?;

            // remove db id list
            let dbid_idlist = DatabaseIdHistoryIdent::new(&tenant2, db);

            util.meta_api()
                .upsert_pb(&UpsertPB::delete(dbid_idlist))
                .await?;

            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: tenant2.clone(),
                    },
                    false,
                )
                .await?;

            // check if get_tenant_history_databases return db_id
            let mut found = false;
            for db_info in res {
                if db_info.database_id.db_id == *util.db_id() {
                    found = true;
                    break;
                }
            }

            assert!(found);
        }
        Ok(())
    }

    async fn drop_table_without_table_id_list<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi {
        // test drop a table without table_id_list
        let tenant = "tenant1";
        let db = "db1";
        let table = "tb1";
        let mut util = DbTableHarness::new(mt, tenant, db, table, "JSON");
        util.create_db().await?;
        let (tid, _table_meta) = util.create_table().await?;

        // remove db id list
        let table_id_idlist = TableIdHistoryIdent {
            database_id: *util.db_id(),
            table_name: table.to_string(),
        };
        util.meta_api()
            .upsert_pb(&UpsertPB::delete(table_id_idlist.clone()))
            .await?;

        // drop table
        util.drop_table_by_id().await?;

        // after drop table, check if table id list has been added
        let value = util
            .meta_api()
            .get_kv(&table_id_idlist.to_string_key())
            .await?;

        assert!(value.is_some());
        let seqv = value.unwrap();
        let id_list: TableIdList = deserialize_struct(&seqv.data)?;
        assert_eq!(id_list.id_list[0], tid);

        Ok(())
    }

    async fn table_rename<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db1_name = "db1";
        let tb2_name = "tb2";
        let db2_name = "db2";
        let tb3_name = "tb3";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let rename_db1tb2_to_db1tb3 = |if_exists| RenameTableReq {
            if_exists,
            name_ident: TableNameIdent {
                tenant: tenant.clone(),
                db_name: db1_name.to_string(),
                table_name: tb2_name.to_string(),
            },
            new_db_name: db1_name.to_string(),
            new_table_name: tb3_name.to_string(),
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- rename table on unknown db");
        {
            let got = mt.rename_table(rename_db1tb2_to_db1tb3(false)).await;
            debug!("--- rename table on unknown database got: {:?}", got);

            assert!(got.is_err());
            assert_eq!(
                ErrorCode::UNKNOWN_DATABASE,
                ErrorCode::from(got.unwrap_err()).code()
            );
        }

        info!("--- prepare db and table");
        let created_on = Utc::now();
        let mut util = DbTableHarness::new(mt, tenant_name, db1_name, tb2_name, "JSON");
        util.create_db().await?;

        info!("--- create table for rename");
        let tb_ident = {
            let old_db = util.get_database().await?;
            let (_table_id, _table_meta) = util
                .create_table_with(
                    |mut meta| {
                        meta.schema = schema(); // Use local schema function (only number field)
                        meta.created_on = created_on;
                        meta
                    },
                    |req| req,
                )
                .await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let got = util.get_table().await?;
            got.ident
        };

        info!("--- rename table, ok");
        {
            let old_db = util.get_database().await?;
            mt.rename_table(rename_db1tb2_to_db1tb3(false)).await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let got = util.get_table_by_name(tb3_name).await?;

            let want = TableInfo {
                ident: tb_ident,
                desc: format!("'{}'.'{}'", db1_name, tb3_name),
                name: tb3_name.into(),
                meta: table_meta(created_on),
                ..Default::default()
            };

            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get renamed table");

            info!("--- get old table after rename");
            {
                let req = GetTableReq::new(&tenant, db1_name, tb2_name);
                let res = mt.get_table(req).await;
                let err = res.err().unwrap();
                assert_eq!(
                    ErrorCode::UnknownTable("").code(),
                    ErrorCode::from(err).code()
                );
            }
        }

        info!("--- db1,tb2(nil) -> db1,tb3(no_nil), error");
        {
            let res = mt.rename_table(rename_db1tb2_to_db1tb3(false)).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UNKNOWN_TABLE,
                ErrorCode::from(err).code(),
                "rename table {} again",
                tb2_name
            );
        }

        info!("--- db1,tb2(nil) -> db1,tb3(no_nil), with if_exist=true, OK");
        {
            mt.rename_table(rename_db1tb2_to_db1tb3(true)).await?;
        }

        info!("--- create db1,db2, ok");
        let tb_ident2 = {
            let mut util2 = DbTableHarness::new(mt, tenant_name, db1_name, tb2_name, "JSON");
            let old_db = util2.get_database().await?;
            let (_table_id, _table_meta) = util2
                .create_table_with(
                    |mut meta| {
                        meta.schema = schema(); // Use local schema function (only number field)
                        meta.created_on = created_on;
                        meta
                    },
                    |req| req,
                )
                .await?;
            let cur_db = util2.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let got = util2.get_table().await?;
            assert_ne!(tb_ident.table_id, got.ident.table_id);
            assert_ne!(tb_ident.seq, got.ident.seq);
            got.ident
        };

        info!("--- db1,tb2(no_nil) -> db1,tb3(no_nil), error");
        {
            let res = mt.rename_table(rename_db1tb2_to_db1tb3(false)).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::TABLE_ALREADY_EXISTS,
                ErrorCode::from(err).code(),
                "rename table {} again after recreate",
                tb2_name
            );
        }

        info!("--- db1,tb2(no_nil) -> db1,tb3(no_nil), if_exists=true, error");
        {
            let res = mt.rename_table(rename_db1tb2_to_db1tb3(true)).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::TABLE_ALREADY_EXISTS,
                ErrorCode::from(err).code(),
                "rename table {} again after recreate",
                tb2_name
            );
        }

        info!("--- rename table to unknown db, error");
        {
            let req = RenameTableReq {
                if_exists: false,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db1_name.to_string(),
                    table_name: tb2_name.to_string(),
                },
                new_db_name: db2_name.to_string(),
                new_table_name: tb3_name.to_string(),
            };
            let res = mt.rename_table(req.clone()).await;
            debug!("--- rename table to other db got: {:?}", res);

            assert!(res.is_err());
            assert_eq!(
                ErrorCode::UNKNOWN_DATABASE,
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        info!("--- prepare other db");
        {
            let mut util = DbTableHarness::new(mt, tenant_name, db2_name, "", "");
            util.create_db().await?;
        }

        info!("--- rename table to other db, ok");
        {
            let req = RenameTableReq {
                if_exists: false,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db1_name.to_string(),
                    table_name: tb2_name.to_string(),
                },
                new_db_name: db2_name.to_string(),
                new_table_name: tb3_name.to_string(),
            };
            let old_db1 = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            let old_db2 = mt.get_database(Self::req_get_db(&tenant, db2_name)).await?;
            mt.rename_table(req.clone()).await?;
            let cur_db1 = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            let cur_db2 = mt.get_database(Self::req_get_db(&tenant, db2_name)).await?;
            assert!(old_db1.meta.seq < cur_db1.meta.seq);
            assert!(old_db2.meta.seq < cur_db2.meta.seq);

            let got = mt
                .get_table((tenant_name, db2_name, tb3_name).into())
                .await?;
            let want = TableInfo {
                ident: tb_ident2,
                desc: format!("'{}'.'{}'", db2_name, tb3_name),
                name: tb3_name.into(),
                meta: table_meta(created_on),
                ..Default::default()
            };
            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get renamed table");
        }

        Ok(())
    }

    async fn table_swap<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db1_name = "db1";
        let tb1_name = "table_a";
        let tb2_name = "table_b";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let _table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: maplit::btreemap! {"opt-1".into() => "val-1".into()},
            created_on,
            ..TableMeta::default()
        };

        let swap_req = |if_exists| SwapTableReq {
            if_exists,
            origin_table: TableNameIdent {
                tenant: tenant.clone(),
                db_name: db1_name.to_string(),
                table_name: tb1_name.to_string(),
            },
            target_table_name: tb2_name.to_string(),
        };

        info!("--- swap tables on unknown db");
        {
            let got = mt.swap_table(swap_req(false)).await;
            debug!("--- swap tables on unknown database got: {:?}", got);

            assert!(got.is_err());
            assert_eq!(
                ErrorCode::UNKNOWN_DATABASE,
                ErrorCode::from(got.unwrap_err()).code()
            );
        }

        info!("--- prepare db and tables");
        let created_on = Utc::now();
        let mut util1 = DbTableHarness::new(mt, tenant_name, db1_name, tb1_name, "JSON");
        util1.create_db().await?;

        let mut util2 = DbTableHarness::new(mt, tenant_name, db1_name, tb2_name, "JSON");

        info!("--- create table_a");
        let tb1_ident = {
            let old_db = util1.get_database().await?;
            let (table_id, _table_meta) = util1
                .create_table_with(
                    |mut meta| {
                        meta.schema = schema();
                        meta.created_on = created_on;
                        meta
                    },
                    |req| req,
                )
                .await?;
            let cur_db = util1.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            let got = util1.get_table().await?;
            assert_eq!(table_id, got.ident.table_id);
            got.ident
        };

        info!("--- create table_b");
        let tb2_ident = {
            let old_db = util2.get_database().await?;
            let (table_id, _table_meta) = util2
                .create_table_with(
                    |mut meta| {
                        meta.schema = schema();
                        meta.created_on = created_on;
                        meta
                    },
                    |req| req,
                )
                .await?;
            let cur_db = util2.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            let got = util2.get_table().await?;
            assert_eq!(table_id, got.ident.table_id);
            got.ident
        };

        info!("--- swap tables, both exist, ok");
        {
            let old_db = util1.get_database().await?;
            let _reply = mt.swap_table(swap_req(false)).await?;
            let cur_db = util1.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            // Verify tables are swapped
            let got_a = mt
                .get_table((tenant_name, db1_name, tb1_name).into())
                .await?;
            let got_b = mt
                .get_table((tenant_name, db1_name, tb2_name).into())
                .await?;

            // table_a name should now point to table_b's id
            assert_eq!(tb2_ident.table_id, got_a.ident.table_id);
            // table_b name should now point to table_a's id
            assert_eq!(tb1_ident.table_id, got_b.ident.table_id);
        }

        info!("--- swap tables again, should restore original mapping");
        {
            let _reply = mt.swap_table(swap_req(false)).await?;

            // Verify tables are swapped back
            let got_a = mt
                .get_table((tenant_name, db1_name, tb1_name).into())
                .await?;
            let got_b = mt
                .get_table((tenant_name, db1_name, tb2_name).into())
                .await?;

            // table_a name should point back to table_a's id
            assert_eq!(tb1_ident.table_id, got_a.ident.table_id);
            // table_b name should point back to table_b's id
            assert_eq!(tb2_ident.table_id, got_b.ident.table_id);
        }

        info!("--- swap non-existent table with if_exists=false, error");
        {
            let swap_req_nonexist = SwapTableReq {
                if_exists: false,
                origin_table: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db1_name.to_string(),
                    table_name: "non_existent".to_string(),
                },
                target_table_name: tb2_name.to_string(),
            };

            let got = mt.swap_table(swap_req_nonexist).await;
            assert!(got.is_err());
            assert_eq!(
                ErrorCode::UNKNOWN_TABLE,
                ErrorCode::from(got.unwrap_err()).code()
            );
        }

        info!("--- swap non-existent table with if_exists=true, ok");
        {
            let swap_req_nonexist = SwapTableReq {
                if_exists: true,
                origin_table: TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: db1_name.to_string(),
                    table_name: "non_existent".to_string(),
                },
                target_table_name: tb2_name.to_string(),
            };

            assert!(mt.swap_table(swap_req_nonexist).await.is_ok());
        }

        Ok(())
    }

    async fn table_update_meta<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;
        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: Default::default(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db and table");
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "JSON");
        util.create_db().await?;
        assert_eq!(1, *util.db_id(), "first database id is 1");

        info!("--- create and get table");
        {
            let created_on = Utc::now();

            {
                let old_db = util.get_database().await?;
                let (table_id, _table_meta) = util
                    .create_table_with(|_meta| table_meta(created_on), |req| req)
                    .await?;
                let cur_db = util.get_database().await?;
                assert!(old_db.meta.seq < cur_db.meta.seq);
                assert!(table_id >= 1, "table id >= 1");
                let tb_id = table_id;

                let got = util.get_table().await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident,
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                    ..Default::default()
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            };
        }

        info!("--- update table meta");
        {
            info!("--- update table meta, normal case");
            {
                let table = util.get_table().await.unwrap();

                let mut new_table_meta = table.meta.clone();
                let table_statistics = TableStatistics {
                    data_bytes: 1,
                    number_of_segments: Some(2),
                    number_of_blocks: Some(200),
                    ..Default::default()
                };
                new_table_meta.statistics = table_statistics;
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                    base_snapshot_location: None,
                    lvt_check: None,
                };

                mt.update_multi_table_meta(UpdateMultiTableMetaReq {
                    update_table_metas: vec![(req, table.as_ref().clone())],
                    ..Default::default()
                })
                .await?
                .unwrap();

                let table = util.get_table().await.unwrap();
                assert_eq!(table.meta, new_table_meta);
            }

            info!("--- update table meta: version mismatch");
            {
                let table = util.get_table().await.unwrap();

                let new_table_meta = table.meta.clone();
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version + 1),
                    new_table_meta: new_table_meta.clone(),
                    base_snapshot_location: None,
                    lvt_check: None,
                };
                let res = mt
                    .update_multi_table_meta(UpdateMultiTableMetaReq {
                        update_table_metas: vec![(req, table.as_ref().clone())],
                        ..Default::default()
                    })
                    .await?;

                let err = res.unwrap_err();
                assert!(!err.is_empty());
            }

            info!("--- update table meta: simulate kv txn retried after commit");
            let txn_sender = IdempotentKVTxnSender::new();
            {
                // 1. Update test table, bump table version, expect success -- just the normal case
                let table = util.get_table().await.unwrap();

                let new_table_meta = table.meta.clone();
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                    base_snapshot_location: None,
                    lvt_check: None,
                };
                let res = mt
                    .update_multi_table_meta_with_sender(
                        UpdateMultiTableMetaReq {
                            update_table_metas: vec![(req.clone(), table.as_ref().clone())],
                            ..Default::default()
                        },
                        &txn_sender,
                    )
                    .await?;

                assert!(res.is_ok());

                // 2. Update test table again
                // Simulate duplicated kv_txn by using the same kv txn_id.
                // Expects:
                // - `update_multi_table_meta_with_txn_id` returns NO error,
                // - the table meta shou NOT be updated

                let table = util.get_table().await.unwrap();

                let table_version = table.ident.seq;
                let mut req = req.clone();
                // Using a fresh table version(seq), otherwise it will fail eagerly during the
                // version checking phase of `update_multi_table_meta_with_txn_id`
                req.seq = MatchSeq::Exact(table_version);
                let prev_table_meta = table.meta.clone();
                // Change the comment, this modification should not be committed
                req.new_table_meta.comment = "some new comment".to_string();
                // Expects no KV api level error, and no app level error.
                // For the convenience of reviewing, using explicit type signature
                let _r: databend_common_meta_app::schema::UpdateTableMetaReply = mt
                    .update_multi_table_meta_with_sender(
                        UpdateMultiTableMetaReq {
                            update_table_metas: vec![(req, table.as_ref().clone())],
                            ..Default::default()
                        },
                        // USING THE SAME KVTxnSender
                        &txn_sender,
                    )
                    .await?
                    .unwrap();

                let table = util.get_table().await.unwrap();

                // The new_table_meta should NOT be committed
                assert_eq!(table.meta, prev_table_meta);
            }

            info!("--- update table meta, with upsert file req");
            {
                let table = util.get_table().await.unwrap();

                let mut new_table_meta = table.meta.clone();
                let table_statistics = TableStatistics {
                    data_bytes: 1,
                    ..Default::default()
                };

                new_table_meta.statistics = table_statistics;
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;

                let mut file_info = BTreeMap::new();
                file_info.insert("test".to_owned(), TableCopiedFileInfo {
                    etag: Some("tag".to_string()),
                    content_length: 1,
                    last_modified: None,
                });

                let upsert_source_table = UpsertTableCopiedFileReq {
                    file_info,
                    ttl: None,
                    insert_if_not_exists: true,
                };

                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                    base_snapshot_location: None,
                    lvt_check: None,
                };
                mt.update_multi_table_meta(UpdateMultiTableMetaReq {
                    update_table_metas: vec![(req, table.as_ref().clone())],
                    copied_files: vec![(table_id, upsert_source_table)],
                    ..Default::default()
                })
                .await?
                .unwrap();

                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.meta, new_table_meta);
            }

            info!("--- update table meta, with non-duplicated upsert file, no duplication");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let mut new_table_meta = table.meta.clone();
                let table_statistics = TableStatistics {
                    data_bytes: 1,
                    ..Default::default()
                };

                new_table_meta.statistics = table_statistics;
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;

                let mut file_info = BTreeMap::new();
                file_info.insert("not_exist".to_owned(), TableCopiedFileInfo {
                    etag: Some("tag_not_exist".to_string()),
                    content_length: 1,
                    last_modified: None,
                });

                let upsert_source_table = UpsertTableCopiedFileReq {
                    file_info,
                    ttl: None,
                    insert_if_not_exists: true,
                };
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                    base_snapshot_location: None,
                    lvt_check: None,
                };
                mt.update_multi_table_meta(UpdateMultiTableMetaReq {
                    update_table_metas: vec![(req, table.as_ref().clone())],
                    copied_files: vec![(table_id, upsert_source_table)],
                    ..Default::default()
                })
                .await?
                .unwrap();

                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.meta, new_table_meta);
            }

            info!("--- update table meta, with duplicated upsert files");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let mut new_table_meta = table.meta.clone();
                let table_statistics = TableStatistics {
                    data_bytes: 1,
                    ..Default::default()
                };

                new_table_meta.statistics = table_statistics;
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;

                let mut file_info = BTreeMap::new();
                file_info.insert("test".to_owned(), TableCopiedFileInfo {
                    etag: Some("tag".to_string()),
                    content_length: 1,
                    last_modified: None,
                });

                let upsert_source_table = UpsertTableCopiedFileReq {
                    file_info,
                    ttl: None,
                    insert_if_not_exists: true,
                };
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                    base_snapshot_location: None,
                    lvt_check: None,
                };
                let result = mt
                    .update_multi_table_meta(UpdateMultiTableMetaReq {
                        update_table_metas: vec![(req, table.as_ref().clone())],
                        copied_files: vec![(table_id, upsert_source_table)],
                        ..Default::default()
                    })
                    .await;
                let err = result.unwrap_err();
                let err = ErrorCode::from(err);
                assert_eq!(ErrorCode::DUPLICATED_UPSERT_FILES, err.code());
            }

            info!("--- update table meta, snapshot_ts must respect LVT");
            {
                let table = util.get_table().await.unwrap();
                let table_id = table.ident.table_id;
                let small_ts = DateTime::<Utc>::from_timestamp(1_000, 0).unwrap();
                let lvt_time = DateTime::<Utc>::from_timestamp(2_000, 0).unwrap();
                let big_time = DateTime::<Utc>::from_timestamp(3_000, 0).unwrap();

                // LVT no set.
                let mut new_table_meta = table.meta.clone();
                new_table_meta.comment = "lvt no set".to_string();
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table.ident.seq),
                    new_table_meta: new_table_meta.clone(),
                    base_snapshot_location: None,
                    lvt_check: Some(TableLvtCheck {
                        tenant: tenant.clone(),
                        time: small_ts,
                    }),
                };
                let result = mt
                    .update_multi_table_meta(UpdateMultiTableMetaReq {
                        update_table_metas: vec![(req, table.as_ref().clone())],
                        ..Default::default()
                    })
                    .await;
                assert!(result.is_ok());
                let table = util.get_table().await.unwrap();
                assert_eq!(table.meta.comment, "lvt no set");

                let lvt_ident = LeastVisibleTimeIdent::new(&tenant, table_id);
                mt.set_table_lvt(&lvt_ident, &LeastVisibleTime::new(lvt_time))
                    .await?;

                // LVT is smaller.
                let mut new_table_meta = table.meta.clone();
                new_table_meta.comment = "lvt guard should fail".to_string();
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table.ident.seq),
                    new_table_meta: new_table_meta.clone(),
                    base_snapshot_location: None,
                    lvt_check: Some(TableLvtCheck {
                        tenant: tenant.clone(),
                        time: small_ts,
                    }),
                };
                let result = mt
                    .update_multi_table_meta(UpdateMultiTableMetaReq {
                        update_table_metas: vec![(req, table.as_ref().clone())],
                        ..Default::default()
                    })
                    .await;
                assert!(result.is_err());

                // LVT is large enough.
                let table = util.get_table().await.unwrap();
                let mut ok_table_meta = table.meta.clone();
                ok_table_meta.comment = "lvt guard success".to_string();
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table.ident.seq),
                    new_table_meta: ok_table_meta.clone(),
                    base_snapshot_location: None,
                    lvt_check: Some(TableLvtCheck {
                        tenant: tenant.clone(),
                        time: big_time,
                    }),
                };
                mt.update_multi_table_meta(UpdateMultiTableMetaReq {
                    update_table_metas: vec![(req, table.as_ref().clone())],
                    ..Default::default()
                })
                .await?
                .unwrap();

                let updated = util.get_table().await.unwrap();
                assert_eq!(updated.meta.comment, "lvt guard success");
            }
        }
        Ok(())
    }

    async fn table_update_mask_policy<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + SecurityApi + DatamaskApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name_1 = "tb1";
        let tbl_name_2 = "tb2";
        let mask_name_1 = "mask1";
        let mask_name_2 = "mask2";

        let schema = || {
            Arc::new(TableSchema::new(vec![
                TableField::new("number", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("c1", TableDataType::Number(NumberDataType::UInt64)),
            ]))
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: Default::default(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let mut util = DbTableHarness::new(mt, tenant_name, db_name, "", "");
            util.create_db().await?;

            assert_eq!(1, *util.db_id(), "first database id is 1");
        }

        let created_on = Utc::now();
        info!("--- create table");
        {
            let mut util = DbTableHarness::new(mt, tenant_name, db_name, "", "");
            let table_meta_val = table_meta(created_on);

            for table_name in [tbl_name_1, tbl_name_2] {
                let (_table_id, _) = util
                    .create_table_with(
                        |_meta| table_meta_val.clone(),
                        |mut req| {
                            req.name_ident.table_name = table_name.to_string();
                            req
                        },
                    )
                    .await?;
            }
        }

        info!("--- create mask policy");
        let mask1_id;
        let mask2_id;
        {
            let req = CreateDatamaskReq {
                name: DataMaskNameIdent::new(tenant.clone(), mask_name_1.to_string()),
                data_mask_meta: DatamaskMeta {
                    args: vec![],
                    return_type: "".to_string(),
                    body: "".to_string(),
                    comment: None,
                    create_on: created_on,
                    update_on: None,
                },
            };
            mt.create_data_mask(req).await??;

            let mask1_name_ident = DataMaskNameIdent::new(tenant.clone(), mask_name_1.to_string());
            mask1_id = get_kv_u64_data(mt, &mask1_name_ident).await?;

            let req = CreateDatamaskReq {
                name: DataMaskNameIdent::new(tenant.clone(), mask_name_2.to_string()),
                data_mask_meta: DatamaskMeta {
                    args: vec![],
                    return_type: "".to_string(),
                    body: "".to_string(),
                    comment: None,
                    create_on: created_on,
                    update_on: None,
                },
            };
            mt.create_data_mask(req).await??;

            let mask2_name_ident = DataMaskNameIdent::new(tenant.clone(), mask_name_2.to_string());
            mask2_id = get_kv_u64_data(mt, &mask2_name_ident).await?;
        }

        let table_id_1;
        info!("--- apply mask1 policy to table 1 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let table_id = res.ident.table_id;
            table_id_1 = table_id;

            let column_id = res
                .meta
                .schema
                .fields
                .iter()
                .find(|f| f.name == "number")
                .unwrap()
                .column_id;
            let req = SetTableColumnMaskPolicyReq {
                tenant: tenant.clone(),
                seq: MatchSeq::Exact(res.ident.seq),
                table_id,
                action: SetSecurityPolicyAction::Set(mask1_id, vec![column_id]),
            };
            mt.set_table_column_mask_policy(req).await?;
            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let mut expect_column_mask_policy = BTreeMap::new();
            expect_column_mask_policy.insert("number".to_string(), mask_name_1.to_string());
            assert_eq!(res.meta.column_mask_policy_columns_ids, {
                let mut map = BTreeMap::new();
                map.insert(0, SecurityPolicyColumnMap {
                    policy_id: mask1_id,
                    columns_ids: vec![0],
                });
                map
            });
            // check mask policy id table id ref
            let tenant = Tenant::new_literal("tenant1");
            let mid = MaskPolicyIdTableId {
                policy_id: mask1_id,
                table_id: table_id_1,
            };
            let ident = MaskPolicyTableIdIdent::new_generic(tenant.clone(), mid);
            let res = mt.get_pb(&ident).await?;

            assert!(res.is_some());
        }

        let table_id_2;
        info!("--- apply mask1 policy to table 2 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_2.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let table_id = res.ident.table_id;
            table_id_2 = table_id;

            let column_id = res
                .meta
                .schema
                .fields
                .iter()
                .find(|f| f.name == "number")
                .unwrap()
                .column_id;
            let req = SetTableColumnMaskPolicyReq {
                tenant: tenant.clone(),
                seq: MatchSeq::Exact(res.ident.seq),
                table_id,
                action: SetSecurityPolicyAction::Set(mask1_id, vec![column_id]),
            };
            mt.set_table_column_mask_policy(req).await?;
            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_2.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let mut expect_column_mask_policy = BTreeMap::new();
            expect_column_mask_policy.insert("number".to_string(), mask_name_1.to_string());
            assert_eq!(res.meta.column_mask_policy_columns_ids, {
                let mut map = BTreeMap::new();
                map.insert(0, SecurityPolicyColumnMap {
                    policy_id: mask1_id,
                    columns_ids: vec![0],
                });
                map
            });
            // check mask policy id table id ref
            let tenant = Tenant::new_literal("tenant1");
            let mid = MaskPolicyIdTableId {
                policy_id: mask1_id,
                table_id: table_id_2,
            };
            let ident = MaskPolicyTableIdIdent::new_generic(tenant.clone(), mid);
            let res = mt.get_pb(&ident).await?;

            assert!(res.is_some());
        }

        info!("--- apply mask2 policy to table 1 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;

            let column_id = res
                .meta
                .schema
                .fields
                .iter()
                .find(|f| f.name == "c1")
                .unwrap()
                .column_id;
            let req = SetTableColumnMaskPolicyReq {
                tenant: tenant.clone(),
                seq: MatchSeq::Exact(res.ident.seq),
                table_id: table_id_1,
                action: SetSecurityPolicyAction::Set(mask2_id, vec![column_id]),
            };
            mt.set_table_column_mask_policy(req).await?;
            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            assert_eq!(res.meta.column_mask_policy_columns_ids, {
                let mut map = BTreeMap::new();
                map.insert(1, SecurityPolicyColumnMap {
                    policy_id: mask2_id,
                    columns_ids: vec![1],
                });
                map.insert(0, SecurityPolicyColumnMap {
                    policy_id: mask1_id,
                    columns_ids: vec![0],
                });
                map
            });
            // check mask policy id table id ref
            let tenant = Tenant::new_literal("tenant1");
            let mid = MaskPolicyIdTableId {
                policy_id: mask2_id,
                table_id: table_id_1,
            };
            let ident = MaskPolicyTableIdIdent::new_generic(tenant.clone(), mid);
            let res = mt.get_pb(&ident).await?;

            assert!(res.is_some());
        }

        info!("--- unset mask policy of table 1 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;

            let req = SetTableColumnMaskPolicyReq {
                tenant: tenant.clone(),
                seq: MatchSeq::Exact(res.ident.seq),
                table_id: table_id_1,
                action: SetSecurityPolicyAction::Unset(mask2_id),
            };
            mt.set_table_column_mask_policy(req).await?;

            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            assert_eq!(res.meta.column_mask_policy, None);
            assert_eq!(res.meta.column_mask_policy_columns_ids, {
                let mut map = BTreeMap::new();
                map.insert(0, SecurityPolicyColumnMap {
                    policy_id: mask1_id,
                    columns_ids: vec![0],
                });
                map
            });

            // check mask policy id table id ref
            let tenant = Tenant::new_literal("tenant1");
            let mid = MaskPolicyIdTableId {
                policy_id: mask1_id,
                table_id: table_id_1,
            };
            let ident = MaskPolicyTableIdIdent::new_generic(tenant.clone(), mid);
            let res = mt.get_pb(&ident).await?;

            assert!(res.is_some());

            let tenant = Tenant::new_literal("tenant1");
            let mid = MaskPolicyIdTableId {
                policy_id: mask2_id,
                table_id: table_id_1,
            };
            let ident = MaskPolicyTableIdIdent::new_generic(tenant.clone(), mid);
            let res = mt.get_pb(&ident).await?;

            assert!(res.is_none());
        }

        Ok(())
    }

    async fn table_update_row_access_policy<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + SecurityApi + RowAccessPolicyApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name_1 = "tb1";
        let tbl_name_2 = "tb2";
        let policy1 = "mask1";
        let policy2 = "mask2";

        info!("--- prepare db");
        {
            let mut util = DbTableHarness::new(mt, tenant_name, db_name, "", "");
            util.create_db().await?;

            assert_eq!(1, *util.db_id(), "first database id is 1");
        }

        info!("--- create table");
        {
            // Create tb1
            let mut util_tb1 = DbTableHarness::new(mt, tenant_name, db_name, tbl_name_1, "JSON");
            let (_table_id, _) = util_tb1.create_table_with(|meta| meta, |req| req).await?;

            // Create tb2
            let mut util_tb2 = DbTableHarness::new(mt, tenant_name, db_name, tbl_name_2, "JSON");
            let (_table_id, _) = util_tb2.create_table_with(|meta| meta, |req| req).await?;
        }

        info!("--- create row access policy");

        let req = CreateRowAccessPolicyReq {
            name: RowAccessPolicyNameIdent::new(tenant.clone(), policy1.to_string()),
            row_access_policy_meta: RowAccessPolicyMeta {
                args: vec![("number".to_string(), "UInt64".to_string())],
                body: "true".to_string(),
                comment: None,
                create_on: Default::default(),
                update_on: None,
            },
        };
        mt.create_row_access_policy(req).await??;

        let name = RowAccessPolicyNameIdent::new(tenant.clone(), policy1.to_string());
        let res = mt.get_row_access_policy(&name).await.unwrap().unwrap();
        let policy1_id = res.0.data;

        let req = CreateRowAccessPolicyReq {
            name: RowAccessPolicyNameIdent::new(tenant.clone(), policy2.to_string()),
            row_access_policy_meta: RowAccessPolicyMeta {
                args: vec![("number".to_string(), "UInt64".to_string())],
                body: "true".to_string(),
                comment: None,
                create_on: Default::default(),
                update_on: None,
            },
        };
        mt.create_row_access_policy(req).await??;

        let table_id_1;
        info!("--- apply mask1 policy to table 1 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let table_id = res.ident.table_id;
            table_id_1 = table_id;

            let req = SetTableRowAccessPolicyReq {
                tenant: tenant.clone(),
                table_id,
                action: SetSecurityPolicyAction::Set(*policy1_id, vec![1]),
            };
            mt.set_table_row_access_policy(req).await??;
            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;

            match &res.meta.row_access_policy_columns_ids {
                Some(r) => assert_eq!(r.policy_id, *policy1_id),
                None => panic!(),
            }
            // check row policy id list
            let tenant = Tenant::new_literal("tenant1");
            let id = RowAccessPolicyIdTableId {
                policy_id: *policy1_id,
                table_id: table_id_1,
            };
            let ident = RowAccessPolicyTableIdIdent::new_generic(tenant.clone(), id);
            let res = mt.get_pb(&ident).await?;

            assert!(res.is_some());
        }

        let table_id_2;
        info!("--- apply mask1 policy to table 2 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_2.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let table_id = res.ident.table_id;
            table_id_2 = table_id;

            let req = SetTableRowAccessPolicyReq {
                tenant: tenant.clone(),
                table_id,
                action: SetSecurityPolicyAction::Set(*policy1_id, vec![1]),
            };
            mt.set_table_row_access_policy(req).await??;
            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_2.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            match &res.meta.row_access_policy_columns_ids {
                Some(r) => assert_eq!(r.policy_id, *policy1_id),
                None => panic!(),
            }
            // check mask policy id list
            let tenant = Tenant::new_literal("tenant1");
            let id = RowAccessPolicyIdTableId {
                policy_id: *policy1_id,
                table_id: table_id_2,
            };
            let ident = RowAccessPolicyTableIdIdent::new_generic(tenant.clone(), id);
            let res = mt.get_pb(&ident).await?;

            assert!(res.is_some());
        }

        info!("--- unset row access policy of table 1 and check");
        {
            let req = SetTableRowAccessPolicyReq {
                tenant: tenant.clone(),
                table_id: table_id_1,
                action: SetSecurityPolicyAction::Unset(*policy1_id),
            };
            mt.set_table_row_access_policy(req).await??;

            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            assert!(res.meta.row_access_policy.is_none());

            // check mask policy id list
            let tenant = Tenant::new_literal("tenant1");
            let id = RowAccessPolicyIdTableId {
                policy_id: *policy1_id,
                table_id: table_id_1,
            };
            let ident = RowAccessPolicyTableIdIdent::new_generic(tenant.clone(), id);
            let res = mt.get_pb(&ident).await?;
            assert!(res.is_none());
        }

        Ok(())
    }

    async fn table_upsert_option<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db and table");
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "JSON");
        util.create_db().await?;
        assert_eq!(1, *util.db_id(), "first database id is 1");

        info!("--- create and get table");
        {
            let created_on = Utc::now();

            {
                let old_db = util.get_database().await?;
                let (table_id, _table_meta) = util
                    .create_table_with(|_meta| table_meta(created_on), |req| req)
                    .await?;
                let cur_db = util.get_database().await?;
                assert!(old_db.meta.seq < cur_db.meta.seq);
                assert!(table_id >= 1, "table id >= 1");
                let tb_id = table_id;

                let got = util.get_table().await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident,
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                    ..Default::default()
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            };
        }

        info!("--- upsert table options");
        {
            info!("--- upsert table options with key1=val1");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                mt.upsert_table_option(UpsertTableOptionReq::new(&table.ident, "key1", "val1"))
                    .await?;

                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }

            info!("--- upsert table options with key1=val1");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let got = mt
                    .upsert_table_option(UpsertTableOptionReq::new(
                        &TableIdent {
                            table_id: table.ident.table_id,
                            seq: table.ident.seq - 1,
                        },
                        "key1",
                        "val2",
                    ))
                    .await;

                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::TABLE_VERSION_MISMATCHED, err.code());

                // table is not affected.
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }

            info!("--- upsert table options with not exist table id");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let got = mt
                    .upsert_table_option(UpsertTableOptionReq::new(
                        &TableIdent {
                            table_id: 1024,
                            seq: table.ident.seq - 1,
                        },
                        "key1",
                        "val2",
                    ))
                    .await;

                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::UNKNOWN_TABLE_ID, err.code());

                // table is not affected.
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }
        }
        Ok(())
    }

    async fn mask_policy_drop_with_table_lifecycle<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + SecurityApi + DatamaskApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant_mask_policy_drop";
        let db_name = "db_mask_policy_drop";
        let table_name = "tb_mask_policy_drop";
        let mask_cleanup_name = "mask_cleanup_after_drop";
        let mask_guard_name = "mask_guard_while_active";

        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        // Prepare database and table.
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, table_name, "FUSE");
        util.create_db().await?;
        let (table_id, _) = util.create_table().await?;

        let table_name_ident =
            TableNameIdent::new(tenant.clone(), db_name.to_string(), table_name.to_string());

        let mut table_info = util.get_table().await?;
        let number_column_id = table_info
            .meta
            .schema
            .fields
            .iter()
            .find(|f| f.name == "number")
            .unwrap()
            .column_id;

        // Create a masking policy and bind it to the table.
        let created_on = Utc::now();
        let mask_cleanup_ident =
            DataMaskNameIdent::new(tenant.clone(), mask_cleanup_name.to_string());
        mt.create_data_mask(CreateDatamaskReq {
            name: mask_cleanup_ident.clone(),
            data_mask_meta: DatamaskMeta {
                args: vec![],
                return_type: "".to_string(),
                body: "".to_string(),
                comment: None,
                create_on: created_on,
                update_on: None,
            },
        })
        .await??;
        let mask_cleanup_id = get_kv_u64_data(mt, &mask_cleanup_ident).await?;

        let set_req = SetTableColumnMaskPolicyReq {
            tenant: tenant.clone(),
            seq: MatchSeq::Exact(table_info.ident.seq),
            table_id,
            action: SetSecurityPolicyAction::Set(mask_cleanup_id, vec![number_column_id]),
        };
        mt.set_table_column_mask_policy(set_req).await?;
        table_info = util.get_table().await?;
        assert!(
            table_info
                .meta
                .column_mask_policy_columns_ids
                .contains_key(&number_column_id)
        );

        // Drop the table (this deletes the table-policy reference), then drop the policy.
        util.drop_table_by_id().await?;

        // Verify the reference was already deleted by drop_table
        let binding_ident =
            MaskPolicyTableIdIdent::new_generic(tenant.clone(), MaskPolicyIdTableId {
                policy_id: mask_cleanup_id,
                table_id,
            });
        assert!(
            mt.get_pb(&binding_ident).await?.is_none(),
            "table-policy reference should be removed when dropping table"
        );

        // Now drop_policy should succeed since there are no active references
        let dropped = mt.drop_data_mask(&mask_cleanup_ident).await??;
        assert!(dropped.is_some());
        assert!(
            mt.get_data_mask(&mask_cleanup_ident).await?.is_none(),
            "policy metadata should be gone after drop"
        );

        // Undrop the table; masking policy entries should disappear.
        mt.undrop_table(UndropTableReq {
            name_ident: table_name_ident.clone(),
        })
        .await?;
        table_info = util.get_table().await?;
        assert!(
            table_info.meta.column_mask_policy_columns_ids.is_empty(),
            "undropped table should no longer reference the dropped policy"
        );

        // Create another masking policy and bind it.
        let mask_guard_ident = DataMaskNameIdent::new(tenant.clone(), mask_guard_name.to_string());
        mt.create_data_mask(CreateDatamaskReq {
            name: mask_guard_ident.clone(),
            data_mask_meta: DatamaskMeta {
                args: vec![],
                return_type: "".to_string(),
                body: "".to_string(),
                comment: None,
                create_on: Utc::now(),
                update_on: None,
            },
        })
        .await??;
        let mask_guard_id = get_kv_u64_data(mt, &mask_guard_ident).await?;

        table_info = util.get_table().await?;
        let set_req = SetTableColumnMaskPolicyReq {
            tenant: tenant.clone(),
            seq: MatchSeq::Exact(table_info.ident.seq),
            table_id,
            action: SetSecurityPolicyAction::Set(mask_guard_id, vec![number_column_id]),
        };
        mt.set_table_column_mask_policy(set_req).await?;
        table_info = util.get_table().await?;
        let policy_entry = table_info
            .meta
            .column_mask_policy_columns_ids
            .get(&number_column_id)
            .expect("mask policy should be attached");
        assert_eq!(mask_guard_id, policy_entry.policy_id);

        // Drop and immediately undrop the table; the binding should persist.
        util.drop_table_by_id().await?;
        mt.undrop_table(UndropTableReq {
            name_ident: table_name_ident.clone(),
        })
        .await?;
        table_info = util.get_table().await?;
        let policy_entry = table_info
            .meta
            .column_mask_policy_columns_ids
            .get(&number_column_id)
            .expect("mask policy should remain after undrop when policy still exists");
        assert_eq!(mask_guard_id, policy_entry.policy_id);

        // Dropping the policy should now fail because the table is active.
        let err = mt.drop_data_mask(&mask_guard_ident).await?;
        let err = err.unwrap_err();
        let err = ErrorCode::from(err);
        assert_eq!(ErrorCode::ConstraintError("").code(), err.code());

        let binding_ident =
            MaskPolicyTableIdIdent::new_generic(tenant.clone(), MaskPolicyIdTableId {
                policy_id: mask_guard_id,
                table_id,
            });
        assert!(
            mt.get_pb(&binding_ident).await?.is_some(),
            "binding should remain when policy drop is rejected"
        );

        Ok(())
    }

    async fn row_access_policy_drop_with_table_lifecycle<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + SecurityApi + RowAccessPolicyApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant_row_policy_drop";
        let db_name = "db_row_policy_drop";
        let table_name = "tb_row_policy_drop";
        let policy_cleanup_name = "row_cleanup_after_drop";
        let policy_guard_name = "row_guard_while_active";

        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        // Prepare database and table.
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, table_name, "FUSE");
        util.create_db().await?;
        let (table_id, _) = util.create_table().await?;

        let table_name_ident =
            TableNameIdent::new(tenant.clone(), db_name.to_string(), table_name.to_string());

        let mut table_info = util.get_table().await?;
        let column_id = table_info.meta.schema.fields[0].column_id;

        // Create a row access policy and bind it to the table.
        let policy_cleanup_ident =
            RowAccessPolicyNameIdent::new(tenant.clone(), policy_cleanup_name.to_string());
        mt.create_row_access_policy(CreateRowAccessPolicyReq {
            name: policy_cleanup_ident.clone(),
            row_access_policy_meta: RowAccessPolicyMeta {
                args: vec![("number".to_string(), "UInt64".to_string())],
                body: "true".to_string(),
                comment: None,
                create_on: Utc::now(),
                update_on: None,
            },
        })
        .await??;
        let cleanup_policy_id = {
            let res = mt
                .get_row_access_policy(&policy_cleanup_ident)
                .await?
                .expect("row access policy exists");
            *res.0.data
        };

        let set_req = SetTableRowAccessPolicyReq {
            tenant: tenant.clone(),
            table_id,
            action: SetSecurityPolicyAction::Set(cleanup_policy_id, vec![column_id]),
        };
        mt.set_table_row_access_policy(set_req).await??;
        table_info = util.get_table().await?;
        assert!(
            table_info
                .meta
                .row_access_policy_columns_ids
                .as_ref()
                .is_some(),
            "row access policy should be recorded in table meta"
        );

        // Drop the table (this deletes the table-policy reference), then drop the policy.
        util.drop_table_by_id().await?;

        // Verify the reference was already deleted by drop_table
        let binding_ident =
            RowAccessPolicyTableIdIdent::new_generic(tenant.clone(), RowAccessPolicyIdTableId {
                policy_id: cleanup_policy_id,
                table_id,
            });
        assert!(
            mt.get_pb(&binding_ident).await?.is_none(),
            "table-policy reference should be removed when dropping table"
        );

        // Now drop_policy should succeed since there are no active references
        let dropped = mt.drop_row_access_policy(&policy_cleanup_ident).await??;
        assert!(dropped.is_some());
        assert!(
            mt.get_row_access_policy(&policy_cleanup_ident)
                .await?
                .is_none(),
            "policy metadata should be gone after drop"
        );

        // Undrop the table; row access policy entries should disappear.
        mt.undrop_table(UndropTableReq {
            name_ident: table_name_ident.clone(),
        })
        .await?;
        table_info = util.get_table().await?;
        assert!(
            table_info.meta.row_access_policy_columns_ids.is_none(),
            "undropped table should no longer reference the dropped row access policy"
        );

        // Create another row access policy and bind it.
        let policy_guard_ident =
            RowAccessPolicyNameIdent::new(tenant.clone(), policy_guard_name.to_string());
        mt.create_row_access_policy(CreateRowAccessPolicyReq {
            name: policy_guard_ident.clone(),
            row_access_policy_meta: RowAccessPolicyMeta {
                args: vec![("number".to_string(), "UInt64".to_string())],
                body: "true".to_string(),
                comment: None,
                create_on: Utc::now(),
                update_on: None,
            },
        })
        .await??;
        let guard_policy_id = {
            let res = mt
                .get_row_access_policy(&policy_guard_ident)
                .await?
                .expect("row access policy exists");
            *res.0.data
        };
        let set_req = SetTableRowAccessPolicyReq {
            tenant: tenant.clone(),
            table_id,
            action: SetSecurityPolicyAction::Set(guard_policy_id, vec![column_id]),
        };
        mt.set_table_row_access_policy(set_req).await??;
        table_info = util.get_table().await?;
        let policy_entry = table_info
            .meta
            .row_access_policy_columns_ids
            .as_ref()
            .expect("row access policy should be attached after setting");
        assert_eq!(guard_policy_id, policy_entry.policy_id);

        // Drop and immediately undrop the table; the binding should persist.
        util.drop_table_by_id().await?;
        mt.undrop_table(UndropTableReq {
            name_ident: table_name_ident.clone(),
        })
        .await?;
        table_info = util.get_table().await?;
        let policy_entry = table_info
            .meta
            .row_access_policy_columns_ids
            .as_ref()
            .expect("row access policy should remain after undrop when policy still exists");
        assert_eq!(guard_policy_id, policy_entry.policy_id);

        // Dropping the policy should now fail because the table is active.
        let err = mt.drop_row_access_policy(&policy_guard_ident).await?;
        let err = err.unwrap_err();
        let err = ErrorCode::from(err);
        assert_eq!(ErrorCode::ConstraintError("").code(), err.code());

        let binding_ident =
            RowAccessPolicyTableIdIdent::new_generic(tenant.clone(), RowAccessPolicyIdTableId {
                policy_id: guard_policy_id,
                table_id,
            });
        assert!(
            mt.get_pb(&binding_ident).await?.is_some(),
            "binding should remain when row access policy drop is rejected"
        );

        // Clean up by dropping the table and policy.
        util.drop_table_by_id().await?;
        let dropped = mt.drop_row_access_policy(&policy_guard_ident).await??;
        assert!(dropped.is_some());
        assert!(
            mt.get_pb(&binding_ident).await?.is_none(),
            "binding should be removed after successful row access policy drop"
        );

        Ok(())
    }

    async fn database_drop_out_of_retention_time_history<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1_database_drop_out_of_retention_time_history";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1_database_drop_out_of_retention_time_history";
        let db_name_ident = DatabaseNameIdent::new(&tenant, db_name);

        info!("--- create and drop db1");
        {
            let drop_on = Some(Utc::now() - Duration::days(1));

            // first create database
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                catalog_name: None,
                name_ident: db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    // drop_on,
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await?;
            let db_id = res.db_id;
            info!("create database res: {:?}", res);

            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;

            // assert not return out of retention time data
            assert_eq!(res.len(), 1);

            let drop_data = DatabaseMeta {
                engine: "github".to_string(),
                drop_on,
                ..Default::default()
            };
            let id_key = db_id;
            let data = serialize_struct(&drop_data)?;
            upsert_test_data(mt, &id_key, data).await?;

            let res = mt
                .get_tenant_history_databases(
                    ListDatabaseReq {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    },
                    false,
                )
                .await?;

            // assert not return out of retention time data
            assert_eq!(res.len(), 0);
        }

        Ok(())
    }

    async fn create_out_of_retention_time_db<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi>(
        self,
        mt: &MT,
        db_name: DatabaseNameIdent,
        drop_on: Option<DateTime<Utc>>,
        delete: bool,
    ) -> anyhow::Result<()> {
        let req = CreateDatabaseReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident: db_name.clone(),
            meta: DatabaseMeta {
                engine: "github".to_string(),
                // drop_on,
                ..Default::default()
            },
        };

        let res = mt.create_database(req).await?;
        if drop_on.is_some() {
            let db_id = res.db_id;
            info!("create database res: {:?}", res);

            let drop_data = DatabaseMeta {
                engine: "github".to_string(),
                drop_on,
                ..Default::default()
            };
            let id_key = db_id;
            let data = serialize_struct(&drop_data)?;
            upsert_test_data(mt, &id_key, data).await?;

            if delete {
                delete_test_data(mt, &db_name).await?;
            }
        }
        Ok(())
    }

    async fn database_gc_out_of_retention_time<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + GarbageCollectionApi,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let util = DbTableHarness::new(
            mt,
            "tenant1_database_gc_out_of_retention_time",
            "db1_database_gc_out_of_retention_time",
            "",
            "",
        );
        let tenant = util.tenant();

        let db_name = "db1_database_gc_out_of_retention_time";
        let db_name_ident1 = DatabaseNameIdent::new(&tenant, db_name);

        let dbid_idlist1 = DatabaseIdHistoryIdent::new(&tenant, db_name);

        let db_name2 = "db2_database_gc_out_of_retention_time";
        let db_name_ident2 = DatabaseNameIdent::new(&tenant, db_name2);

        let dbid_idlist2 = DatabaseIdHistoryIdent::new(&tenant, db_name2);

        let drop_on = Some(Utc::now() - Duration::days(1));

        // create db_name_ident1 with two dropped table
        self.create_out_of_retention_time_db(mt, db_name_ident1.clone(), drop_on, true)
            .await?;
        self.create_out_of_retention_time_db(
            mt,
            db_name_ident1.clone(),
            Some(Utc::now() - Duration::days(2)),
            false,
        )
        .await?;
        // create db_name_ident2 with one dropped value and one non-dropped table
        self.create_out_of_retention_time_db(mt, db_name_ident2.clone(), drop_on, true)
            .await?;
        self.create_out_of_retention_time_db(mt, db_name_ident2.clone(), None, false)
            .await?;

        let id_list: DbIdList = get_kv_data(mt, &dbid_idlist1).await?;
        assert_eq!(id_list.len(), 2);
        let old_id_list = id_list.id_list().clone();

        let id_list: DbIdList = get_kv_data(mt, &dbid_idlist2).await?;
        assert_eq!(id_list.len(), 2);

        {
            let req = ListDroppedTableReq::new(&tenant);
            let resp = mt.get_drop_table_infos(req).await?;

            let req = GcDroppedTableReq {
                tenant: tenant.clone(),
                catalog: "default".to_string(),
                drop_ids: resp.drop_ids.clone(),
            };
            mt.gc_drop_tables(req).await?;
        }

        // assert db id list key has been removed
        let id_list: Result<DbIdList, KVAppError> = get_kv_data(mt, &dbid_idlist1).await;
        assert!(id_list.is_err());

        // assert old db meta and id to name mapping has been removed
        for db_id in old_id_list.iter() {
            let id_key = DatabaseId { db_id: *db_id };
            let id_mapping = DatabaseIdToName { db_id: *db_id };

            let meta_res: Result<DatabaseMeta, KVAppError> = get_kv_data(mt, &id_key).await;
            assert!(meta_res.is_err());

            let mapping_res: Result<DatabaseNameIdentRaw, KVAppError> =
                get_kv_data(mt, &id_mapping).await;
            assert!(mapping_res.is_err());
        }

        let id_list: DbIdList = get_kv_data(mt, &dbid_idlist2).await?;
        assert_eq!(id_list.len(), 1);

        Ok(())
    }

    /// Return table id and table meta
    async fn create_out_of_retention_time_table<MT: kvapi::KVApi<Error = MetaError> + TableApi>(
        self,
        mt: &MT,
        name_ident: TableNameIdent,
        dbid_tbname: DBIdTableName,
        drop_on: Option<DateTime<Utc>>,
        delete: bool,
    ) -> anyhow::Result<(u64, TableMeta)> {
        let created_on = Utc::now();
        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let create_table_meta = TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            created_on,
            ..TableMeta::default()
        };

        let req = CreateTableReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident,
            table_meta: create_table_meta.clone(),
            as_dropped: false,
            table_properties: None,
            table_partition: None,
        };

        let res = mt.create_table(req).await?;
        let table_id = res.table_id;
        info!("create table res: {:?}", res);

        let drop_data = TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            created_on,
            drop_on,
            ..TableMeta::default()
        };

        let id_key = TableId { table_id };
        let data = serialize_struct(&drop_data)?;
        upsert_test_data(mt, &id_key, data).await?;

        if delete {
            delete_test_data(mt, &dbid_tbname).await?;
        }
        Ok((table_id, drop_data))
    }

    async fn table_gc_out_of_retention_time<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + GarbageCollectionApi,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let mut util = DbTableHarness::new(
            mt,
            "tenant1_table_gc_out_of_retention_time",
            "db1_table_gc_out_of_retention_time",
            "tb1_table_gc_out_of_retention_time",
            "",
        );
        let tenant = util.tenant();

        let db1_name = util.db_name();
        let tb1_name = util.tbl_name();
        let tbl_name_ident = TableNameIdent {
            tenant: tenant.clone(),
            db_name: db1_name.clone(),
            table_name: tb1_name.clone(),
        };

        util.create_db().await?;
        let db_id = util.db_id();
        assert_eq!(1, *db_id, "first database id is 1");
        let one_day_before = Some(Utc::now() - Duration::days(1));
        let two_day_before = Some(Utc::now() - Duration::days(2));

        self.create_out_of_retention_time_table(
            mt,
            tbl_name_ident.clone(),
            DBIdTableName {
                db_id: *db_id,
                table_name: tb1_name.clone(),
            },
            one_day_before,
            true,
        )
        .await?;
        let (table_id, table_meta) = self
            .create_out_of_retention_time_table(
                mt,
                tbl_name_ident.clone(),
                DBIdTableName {
                    db_id: *db_id,
                    table_name: tb1_name.clone(),
                },
                two_day_before,
                false,
            )
            .await?;

        info!("--- create and get stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                ttl: Some(std::time::Duration::from_secs(86400)),
                insert_if_not_exists: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta.clone(),
                base_snapshot_location: None,
                lvt_check: None,
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident,
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            mt.update_multi_table_meta(req).await?.unwrap();

            let key = TableCopiedFileNameIdent {
                table_id,
                file: "file".to_string(),
            };

            let stage_file: TableCopiedFileInfo = get_kv_data(mt, &key).await?;
            assert_eq!(stage_file, stage_info);
        }

        let table_id_idlist = TableIdHistoryIdent {
            database_id: *db_id,
            table_name: tb1_name.clone(),
        };

        // save old id list
        let id_list: TableIdList = get_kv_data(mt, &table_id_idlist).await?;
        assert_eq!(id_list.len(), 2);
        let old_id_list = id_list.id_list().clone();

        // gc the drop tables
        {
            let req = ListDroppedTableReq::new(&tenant);
            let resp = mt.get_drop_table_infos(req).await?;

            let req = GcDroppedTableReq {
                tenant: tenant.clone(),
                catalog: "default".to_string(),
                drop_ids: resp.drop_ids.clone(),
            };
            mt.gc_drop_tables(req).await?;
        }

        // assert table id list key has been removed
        let id_list: Result<TableIdList, KVAppError> = get_kv_data(mt, &table_id_idlist).await;
        assert!(id_list.is_err());

        // assert old table meta and id to name mapping has been removed
        for table_id in old_id_list.iter() {
            let id_key = TableId {
                table_id: *table_id,
            };
            let id_mapping = TableIdToName {
                table_id: *table_id,
            };
            let meta_res: Result<DatabaseMeta, KVAppError> = get_kv_data(mt, &id_key).await;
            let mapping_res: Result<DBIdTableName, KVAppError> = get_kv_data(mt, &id_mapping).await;
            assert!(meta_res.is_err());
            assert!(mapping_res.is_err());
        }

        info!("--- assert stage file info has been removed");
        {
            let key = TableCopiedFileNameIdent {
                table_id,
                file: "file".to_string(),
            };

            let resp: Result<TableCopiedFileInfo, KVAppError> = get_kv_data(mt, &key).await;
            assert!(resp.is_err());
        }

        Ok(())
    }

    async fn db_table_gc_out_of_retention_time<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + GarbageCollectionApi + IndexApi,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "db_table_gc_out_of_retention_time";
        let tenant = Tenant::new_literal(tenant_name);
        let db1_name = "db1";
        let tb1_name = "tb1";
        let idx1_name = "idx1";
        let tbl_name_ident = TableNameIdent {
            tenant: tenant.clone(),
            db_name: db1_name.to_string(),
            table_name: tb1_name.to_string(),
        };

        let mut util = DbTableHarness::new(mt, tenant_name, db1_name, tb1_name, "JSON");
        util.create_db().await?;
        info!("create database res: {:?}", ());
        let db_id = util.db_id();

        let created_on = Utc::now();
        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let (table_id, _table_meta) = util.create_table_with(|meta| meta, |req| req).await?;
        info!("--- create and get stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                ttl: Some(std::time::Duration::from_secs(86400)),
                insert_if_not_exists: true,
            };

            let create_table_meta = TableMeta {
                schema: schema(),
                engine: "JSON".to_string(),
                created_on,
                ..TableMeta::default()
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: create_table_meta.clone(),
                base_snapshot_location: None,
                lvt_check: None,
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            mt.update_multi_table_meta(req).await?.unwrap();

            let key = TableCopiedFileNameIdent {
                table_id,
                file: "file".to_string(),
            };

            let stage_file: TableCopiedFileInfo = get_kv_data(mt, &key).await?;
            assert_eq!(stage_file, stage_info);
        }

        let agg_index_create_req = CreateIndexReq {
            create_option: CreateOption::CreateIfNotExists,
            name_ident: IndexNameIdent::new(&tenant, idx1_name),
            meta: IndexMeta {
                table_id,
                index_type: IndexType::AGGREGATING,
                created_on: Utc::now(),
                dropped_on: None,
                updated_on: None,
                original_query: "select sum(number) from tb1".to_string(),
                query: "select sum(number) from tb1".to_string(),
                sync_creation: false,
            },
        };

        let res = mt.create_index(agg_index_create_req).await?;
        let index_id = res.index_id;

        // drop the db
        let drop_on = Some(Utc::now() - Duration::days(1));
        let drop_data = DatabaseMeta {
            engine: "github".to_string(),
            drop_on,
            ..Default::default()
        };
        let id_key = db_id;
        let data = serialize_struct(&drop_data)?;
        upsert_test_data(mt, &id_key, data).await?;

        let dbid_idlist1 = DatabaseIdHistoryIdent::new(&tenant, db1_name);
        let old_id_list: DbIdList = get_kv_data(mt, &dbid_idlist1).await?;
        assert_eq!(old_id_list.len(), 1);

        let table_id_idlist = TableIdHistoryIdent {
            database_id: *db_id,
            table_name: tb1_name.to_string(),
        };

        // save old id list
        let id_list: TableIdList = get_kv_data(mt, &table_id_idlist).await?;
        assert_eq!(id_list.len(), 1);
        let old_table_id_list = id_list.id_list().clone();

        // gc the data
        {
            let req = ListDroppedTableReq::new(&tenant);
            let resp = mt.get_drop_table_infos(req).await?;

            let req = GcDroppedTableReq {
                tenant: tenant.clone(),
                catalog: "default".to_string(),
                drop_ids: resp.drop_ids.clone(),
            };
            mt.gc_drop_tables(req).await?;
        }

        // assert db id list has been removed
        let id_list: Result<DbIdList, KVAppError> = get_kv_data(mt, &dbid_idlist1).await;
        assert!(id_list.is_err());

        // assert old db meta and id to name mapping has been removed
        for db_id in old_id_list.id_list.iter() {
            let id_key = DatabaseId { db_id: *db_id };
            let id_mapping = DatabaseIdToName { db_id: *db_id };

            let meta_res: Result<DatabaseMeta, KVAppError> = get_kv_data(mt, &id_key).await;
            assert!(meta_res.is_err());

            let mapping_res: Result<DatabaseNameIdentRaw, KVAppError> =
                get_kv_data(mt, &id_mapping).await;
            assert!(mapping_res.is_err());
        }

        // check table data has been gc
        let id_list: Result<TableIdList, KVAppError> = get_kv_data(mt, &table_id_idlist).await;
        assert!(id_list.is_err());

        // assert old table meta and id to name mapping has been removed
        for table_id in old_table_id_list.iter() {
            let id_key = TableId {
                table_id: *table_id,
            };
            let id_mapping = TableIdToName {
                table_id: *table_id,
            };
            let meta_res: Result<DatabaseMeta, KVAppError> = get_kv_data(mt, &id_key).await;
            let mapping_res: Result<DBIdTableName, KVAppError> = get_kv_data(mt, &id_mapping).await;
            assert!(meta_res.is_err());
            assert!(mapping_res.is_err());
        }

        // check table's indexes have been cleaned
        {
            let index_id = IndexId::new(index_id);
            let id_ident = IndexIdIdent::new_generic(&tenant, index_id);
            let id_to_name_key = IndexIdToNameIdent::new_generic(tenant, index_id);

            let agg_index_meta: Result<IndexMeta, KVAppError> = get_kv_data(mt, &id_ident).await;
            let agg_index_name_ident: Result<IndexNameIdentRaw, KVAppError> =
                get_kv_data(mt, &id_to_name_key).await;

            assert!(agg_index_meta.is_err());
            assert!(agg_index_name_ident.is_err());
        }

        info!("--- assert stage file info has been removed");
        {
            let key = TableCopiedFileNameIdent {
                table_id,
                file: "file".to_string(),
            };

            let resp: Result<TableCopiedFileInfo, KVAppError> = get_kv_data(mt, &key).await;
            assert!(resp.is_err());
        }

        Ok(())
    }

    async fn table_drop_out_of_retention_time_history<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant_table_drop_history";
        let db_name = "table_table_drop_history_db1";
        let tbl_name = "table_table_drop_history_tb1";
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "");

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        info!("--- prepare db");
        util.create_db().await?;

        let created_on = Utc::now();
        info!("--- create and get table");
        {
            let old_db = util.get_database().await?;
            let (table_id, _) = util.create_table_with(|meta| meta, |req| req).await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            assert!(table_id >= 1, "table id >= 1");

            let res = util.list_history_tables(false).await?;

            assert_eq!(res.len(), 1);

            let tbid = TableId { table_id };
            let create_drop_table_meta = TableMeta {
                schema: schema(),
                engine: "JSON".to_string(),
                created_on,
                drop_on: Some(created_on - Duration::days(1)),
                ..TableMeta::default()
            };
            let data = serialize_struct(&create_drop_table_meta)?;

            upsert_test_data(mt, &tbid, data).await?;
            // assert not return out of retention time data
            let res = util.list_history_tables(false).await?;

            assert_eq!(res.len(), 0);

            let res = util.list_history_tables(true).await?;
            assert_eq!(res.len(), 1);
        }

        Ok(())
    }

    async fn table_history_filter<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let util = DbTableHarness::new(mt, "tenant1", "db1", "tb1", "JSON");
        let tenant = util.tenant();

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };
        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: BTreeMap::new(),
            updated_on: created_on,
            created_on,
            ..TableMeta::default()
        };
        let created_on = Utc::now();

        // The expected drop_ids built with and without retention time boundary
        let mut drop_ids_boundary = vec![];
        let mut drop_ids_no_boundary = vec![];

        // first create a database drop within filter time
        info!("--- create db1");
        let db1_id;
        {
            let mut util = DbTableHarness::new(mt, "tenant1", "db1", "tb1", "JSON");
            util.create_db().await?;
            db1_id = util.db_id();
            let db_name = DatabaseNameIdent::new(&tenant, "db1");

            let (db1_tb1_id, _) = util.create_table_with(|meta| meta, |req| req).await?;

            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
            })
            .await?;

            drop_ids_boundary.push(DroppedId::new_table(*db1_id, db1_tb1_id, "tb1"));
            drop_ids_boundary.push(DroppedId::Db {
                db_id: *db1_id,
                db_name: db_name.database_name().to_string(),
            });

            drop_ids_no_boundary.push(DroppedId::new_table(*db1_id, db1_tb1_id, "tb1"));
            drop_ids_no_boundary.push(DroppedId::Db {
                db_id: *db1_id,
                db_name: db_name.database_name().to_string(),
            });
        }

        // second create a database drop outof filter time, but has a table drop within filter time
        info!("--- create db2");
        let db2_id;
        {
            let mut util_db2 = DbTableHarness::new(mt, "tenant1", "db2", "tb1", "JSON");
            util_db2.create_db().await?;
            db2_id = util_db2.db_id();
            drop_ids_no_boundary.push(DroppedId::Db {
                db_id: *db2_id,
                db_name: "db2".to_string(),
            });

            info!("--- create and drop db2.tb1");
            {
                let table_name = TableNameIdent {
                    tenant: tenant.clone(),
                    db_name: "db2".to_string(),
                    table_name: "tb1".to_string(),
                };
                let (table_id, _) = util_db2
                    .create_table_with(|_meta| table_meta(created_on), |req| req)
                    .await?;
                drop_ids_boundary.push(DroppedId::new_table(
                    *db2_id,
                    table_id,
                    table_name.table_name.clone(),
                ));
                drop_ids_no_boundary.push(DroppedId::new_table(
                    *db2_id,
                    table_id,
                    table_name.table_name.clone(),
                ));

                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: table_name.tenant.clone(),
                    db_id: *db2_id,
                    db_name: "db2".to_string(),
                    table_name: table_name.table_name.clone(),
                    tb_id: table_id,
                    engine: "FUSE".to_string(),
                    temp_prefix: "".to_string(),
                })
                .await?;
            }

            info!("--- create and drop db2.tb2, but make its drop time out of filter time");
            {
                let mut table_meta = table_meta(created_on);
                let table_name_ident = TableNameIdent::new(&tenant, "db2", "tb2");
                let (table_id, _) = util_db2
                    .create_table_with(
                        |meta| meta,
                        |mut req| {
                            req.name_ident.table_name = "tb2".to_string();
                            req
                        },
                    )
                    .await?;
                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: table_name_ident.tenant.clone(),
                    db_id: *db2_id,
                    db_name: "db2".to_string(),
                    table_name: table_name_ident.table_name.clone(),
                    tb_id: table_id,
                    engine: "FUSE".to_string(),
                    temp_prefix: "".to_string(),
                })
                .await?;
                let id_key = TableId { table_id };
                table_meta.drop_on = Some(created_on + Duration::seconds(100));
                let data = serialize_struct(&table_meta)?;
                upsert_test_data(mt, &id_key, data).await?;

                drop_ids_no_boundary.push(DroppedId::new_table(
                    *db2_id,
                    table_id,
                    "tb2".to_string(),
                ));
            }

            info!("--- create db2.tb3");
            let db2_tb3_id;
            {
                let (table_id, _) = util_db2
                    .create_table_with(
                        |meta| meta,
                        |mut req| {
                            req.name_ident.table_name = "tb3".to_string();
                            req
                        },
                    )
                    .await?;
                db2_tb3_id = table_id;
            }

            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, "db2"),
            })
            .await?;
            // change db meta to make this db drop time outof filter time
            let drop_db_meta = DatabaseMeta {
                drop_on: Some(created_on + Duration::seconds(100)),
                ..Default::default()
            };
            let id_key = db2_id;
            let data = serialize_struct(&drop_db_meta)?;
            upsert_test_data(mt, &id_key, data).await?;

            drop_ids_no_boundary.push(DroppedId::new_table(*db2_id, db2_tb3_id, "tb3".to_string()));
        }

        // third create a database not dropped, but has a table drop within filter time
        let db3_id;
        {
            let mut util_db3 = DbTableHarness::new(mt, "tenant1", "db3", "tb1", "FUSE");
            util_db3.create_db().await?;
            db3_id = util_db3.db_id();

            info!("--- create and drop db3.tb1");
            {
                let (table_id, _) = util_db3
                    .create_table_with(|_meta| table_meta(created_on), |req| req)
                    .await?;
                drop_ids_boundary.push(DroppedId::new_table(*db3_id, table_id, "tb1"));
                drop_ids_no_boundary.push(DroppedId::new_table(*db3_id, table_id, "tb1"));
                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: util_db3.tenant(),
                    db_id: *db3_id,
                    db_name: "db3".to_string(),
                    table_name: "tb1".to_string(),
                    tb_id: table_id,
                    engine: "FUSE".to_string(),
                    temp_prefix: "".to_string(),
                })
                .await?;
            }

            info!("--- create and drop db3.tb2, but make its drop time out of filter time");
            {
                let mut table_meta = table_meta(created_on);
                let (table_id, _) = util_db3
                    .create_table_with(
                        |_meta| table_meta.clone(),
                        |mut req| {
                            req.name_ident.table_name = "tb2".to_string();
                            req
                        },
                    )
                    .await?;
                drop_ids_no_boundary.push(DroppedId::new_table(*db3_id, table_id, "tb2"));
                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: util_db3.tenant(),
                    db_id: *db3_id,
                    db_name: "db3".to_string(),
                    table_name: "tb2".to_string(),
                    tb_id: table_id,
                    engine: "FUSE".to_string(),
                    temp_prefix: "".to_string(),
                })
                .await?;
                let id_key = TableId { table_id };
                table_meta.drop_on = Some(created_on + Duration::seconds(100));
                let data = serialize_struct(&table_meta)?;
                upsert_test_data(mt, &id_key, data).await?;
            }

            info!("--- create db3.tb3");
            {
                let (_table_id, _) = util_db3
                    .create_table_with(
                        |_meta| table_meta(created_on),
                        |mut req| {
                            req.name_ident.table_name = "tb3".to_string();
                            req
                        },
                    )
                    .await?;
            }
        }

        // case 1: test AllDroppedTables with filter time
        {
            let now = Utc::now();
            let req = ListDroppedTableReq::new(&tenant).with_retention_boundary(now);
            let resp = mt.get_drop_table_infos(req).await?;

            let got = resp
                .drop_ids
                .iter()
                .map(|x| x.cmp_key())
                .collect::<BTreeSet<_>>();

            let want = drop_ids_boundary
                .iter()
                .map(|x| x.cmp_key())
                .collect::<BTreeSet<_>>();

            assert_eq!(got, want);

            let expected: BTreeSet<String> = [
                "'db1'.'tb1'".to_string(),
                "'db2'.'tb1'".to_string(),
                "'db3'.'tb1'".to_string(),
            ]
            .into_iter()
            .collect();
            let actual: BTreeSet<String> = resp
                .vacuum_tables
                .iter()
                .map(|(db_name_ident, table_niv)| {
                    format!(
                        "'{}'.'{}'",
                        db_name_ident.database_name(),
                        &table_niv.name().table_name
                    )
                })
                .collect();
            assert_eq!(expected, actual);
        }

        // case 2: test AllDroppedTables without filter time
        {
            let req = ListDroppedTableReq::new(&tenant);
            let resp = mt.get_drop_table_infos(req).await?;

            let got = resp
                .drop_ids
                .iter()
                .map(|x| x.cmp_key())
                .collect::<BTreeSet<_>>();

            let want = drop_ids_no_boundary
                .iter()
                .map(|x| x.cmp_key())
                .collect::<BTreeSet<_>>();

            assert_eq!(got, want);

            let expected: BTreeSet<String> = [
                "'db1'.'tb1'".to_string(),
                "'db2'.'tb1'".to_string(),
                "'db2'.'tb2'".to_string(),
                "'db2'.'tb3'".to_string(),
                "'db3'.'tb1'".to_string(),
                "'db3'.'tb2'".to_string(),
            ]
            .iter()
            .cloned()
            .collect();
            let actual: BTreeSet<String> = resp
                .vacuum_tables
                .iter()
                .map(|(db_name_ident, table_niv)| {
                    format!(
                        "'{}'.'{}'",
                        db_name_ident.database_name(),
                        &table_niv.name().table_name
                    )
                })
                .collect();
            assert_eq!(expected, actual);
        }
        Ok(())
    }

    // construct dropped tables: db1.tb[0..DEFAULT_MGET_SIZE + 1], db2.[0..DEFAULT_MGET_SIZE], db3.{tb1}
    // case 1: with no limit it will return all these tables
    // case 2: with limit 1 it will return db1.tb[0]
    // case 3: with limit DEFAULT_MGET_SIZE it will return db1.tb[0..DEFAULT_MGET_SIZE]
    // case 4: with limit 2 * DEFAULT_MGET_SIZE it will return db1.tb[0..DEFAULT_MGET_SIZE + 1], db2.[0..DEFAULT_MGET_SIZE - 1]
    // case 5: with limit 3 * DEFAULT_MGET_SIZE it will return db1.tb[0..DEFAULT_MGET_SIZE + 1], db2.[0..DEFAULT_MGET_SIZE], db3.{tb1}
    async fn table_history_filter_with_limit<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        async fn create_dropped_table<
            MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
        >(
            mt: &MT,
            tenant: &str,
            db: &str,
            db_id: u64,
            number: usize,
        ) -> anyhow::Result<Vec<DroppedId>> {
            let schema = || {
                Arc::new(TableSchema::new(vec![TableField::new(
                    "number",
                    TableDataType::Number(NumberDataType::UInt64),
                )]))
            };

            let table_meta = |created_on| TableMeta {
                schema: schema(),
                engine: "JSON".to_string(),
                options: BTreeMap::new(),
                updated_on: created_on,
                created_on,
                ..TableMeta::default()
            };
            let mut drop_ids = vec![];
            let created_on = Utc::now();
            for i in 0..number {
                let table_name = format!("tb{:?}", i);
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    catalog_name: None,
                    name_ident: TableNameIdent {
                        tenant: Tenant::new_or_err(tenant, func_name!())?,
                        db_name: db.to_string(),
                        table_name: table_name.clone(),
                    },

                    table_meta: table_meta(created_on),
                    as_dropped: false,
                    table_properties: None,
                    table_partition: None,
                };
                let resp = mt.create_table(req.clone()).await?;

                drop_ids.push(DroppedId::new_table(
                    db_id,
                    resp.table_id,
                    table_name.clone(),
                ));

                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: req.name_ident.tenant.clone(),
                    db_id,
                    table_name: req.name_ident.table_name.clone(),
                    db_name: db.to_string(),
                    tb_id: resp.table_id,
                    engine: "FUSE".to_string(),
                    temp_prefix: "".to_string(),
                })
                .await?;
            }
            Ok(drop_ids)
        }

        let mut case1_drop_ids = vec![];
        let mut case2_drop_ids = vec![];
        let mut case3_drop_ids = vec![];
        let mut case4_drop_ids = vec![];
        let mut case5_drop_ids = vec![];

        info!("--- create db1 tables");
        {
            let test_db_name = "db1";
            let mut util = DbTableHarness::new(mt, tenant_name, test_db_name, "", "");
            util.create_db().await?;
            let db_id = util.db_id();

            let drop_ids =
                create_dropped_table(mt, tenant_name, test_db_name, *db_id, DEFAULT_MGET_SIZE + 1)
                    .await?;
            let case_drop_ids_vec = vec![
                &mut case1_drop_ids,
                &mut case2_drop_ids,
                &mut case3_drop_ids,
                &mut case4_drop_ids,
                &mut case5_drop_ids,
            ];
            for case_drop_ids in case_drop_ids_vec {
                case_drop_ids.extend(drop_ids.clone());
            }
        }

        info!("--- create db2 tables");
        {
            let test_db_name = "db2";
            let mut util = DbTableHarness::new(mt, tenant_name, test_db_name, "", "");
            util.create_db().await?;
            let db_id = util.db_id();

            let drop_ids =
                create_dropped_table(mt, tenant_name, test_db_name, *db_id, DEFAULT_MGET_SIZE)
                    .await?;
            let case_drop_ids_vec = vec![
                &mut case1_drop_ids,
                &mut case4_drop_ids,
                &mut case5_drop_ids,
            ];
            for case_drop_ids in case_drop_ids_vec {
                case_drop_ids.extend(drop_ids.clone());
            }
        }

        info!("--- create db3 tables");
        {
            let test_db_name = "db3";
            let mut util = DbTableHarness::new(mt, tenant_name, test_db_name, "", "");
            util.create_db().await?;
            let db_id = util.db_id();

            let drop_ids = create_dropped_table(mt, tenant_name, test_db_name, *db_id, 1).await?;
            let case_drop_ids_vec = vec![&mut case1_drop_ids, &mut case5_drop_ids];
            for case_drop_ids in case_drop_ids_vec {
                case_drop_ids.extend(drop_ids.clone());
            }
        }

        let limit_and_drop_ids = vec![
            (None, case1_drop_ids.len(), case1_drop_ids),
            (Some(1), 1, case2_drop_ids),
            (Some(DEFAULT_MGET_SIZE), DEFAULT_MGET_SIZE, case3_drop_ids),
            (
                Some(DEFAULT_MGET_SIZE * 2),
                DEFAULT_MGET_SIZE * 2,
                case4_drop_ids,
            ),
            (
                Some(DEFAULT_MGET_SIZE * 3),
                DEFAULT_MGET_SIZE * 2 + 2,
                case5_drop_ids,
            ),
        ];
        for (limit, number, drop_ids) in limit_and_drop_ids {
            let req = ListDroppedTableReq::new(&tenant);
            let req = if let Some(limit) = limit {
                req.with_limit(limit)
            } else {
                req
            };
            let resp = mt.get_drop_table_infos(req).await?;
            assert_eq!(resp.drop_ids.len(), number);

            let drop_ids_set: HashSet<u64> = drop_ids
                .iter()
                .map(|l| {
                    if let DroppedId::Table { name: _, id } = l {
                        id.table_id
                    } else {
                        unreachable!()
                    }
                })
                .collect();

            for id in resp.drop_ids {
                if let DroppedId::Table { name: _, id } = id {
                    assert!(drop_ids_set.contains(&id.table_id));
                } else {
                    unreachable!()
                }
            }
        }
        Ok(())
    }

    async fn table_drop_undrop_list_history<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant_drop_undrop_list_history_db1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "table_drop_undrop_list_history_db1";
        let tbl_name = "table_drop_undrop_list_history_tb2";
        let new_tbl_name = "new_table_drop_undrop_list_history_tb2";
        let tbl_name_ident = TableNameIdent {
            tenant: tenant.clone(),
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "JSON");
        util.create_db().await?;

        assert_eq!(1, *util.db_id(), "first database id is 1");
        let db_id = util.db_id();

        let created_on = Utc::now();
        let create_table_meta = table_meta(created_on);
        info!("--- create and get table");
        {
            let old_db = util.get_database().await?;
            let (table_id, _) = util
                .create_table_with(|_meta| table_meta(created_on), |req| req)
                .await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            assert!(table_id >= 1, "table id >= 1");

            let res = util.list_history_tables(false).await?;

            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);
        }
        let tb_info = util.get_table().await?;
        let tb_id = tb_info.ident.table_id;

        info!("--- drop and undrop table");
        {
            // first drop table
            let old_db = util.get_database().await?;
            mt.drop_table_by_id(DropTableByIdReq {
                if_exists: false,
                tenant: tbl_name_ident.tenant.clone(),
                db_id: old_db.database_id.db_id,
                table_name: tbl_name_ident.table_name.clone(),
                db_name: db_name.to_string(),
                tb_id,
                engine: "FUSE".to_string(),
                temp_prefix: "".to_string(),
            })
            .await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let res = util.list_history_tables(false).await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // then undrop table
            let old_db = util.get_database().await?;
            let plan = UndropTableReq {
                name_ident: tbl_name_ident.clone(),
            };
            mt.undrop_table(plan).await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let res = util.list_history_tables(false).await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);
        }

        info!("--- drop and create table");
        {
            // first drop table
            let old_db = util.get_database().await?;
            mt.drop_table_by_id(DropTableByIdReq {
                if_exists: false,
                tenant: tenant.clone(),
                db_id: old_db.database_id.db_id,
                table_name: tbl_name.to_string(),
                db_name: db_name.to_string(),
                tb_id,
                engine: "FUSE".to_string(),
                temp_prefix: "".to_string(),
            })
            .await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let res = util.list_history_tables(false).await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // then create table
            let old_db = util.get_database().await?;
            let (table_id_from_util, _) = util
                .create_table_with(|_meta| create_table_meta.clone(), |req| req)
                .await?;
            let res = CreateTableReply {
                table_id: table_id_from_util,
                table_id_seq: None,
                db_id: *util.db_id(),
                prev_table_id: None,
                new_table: true,
                orphan_table_name: None,
                spec_vec: None,
            };
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            assert!(res.table_id >= 1, "table id >= 1");

            let res = util.list_history_tables(false).await?;

            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 1,
            }]);

            // then drop table
            let old_db = util.get_database().await?;
            let tb_info = util.get_table().await?;
            mt.drop_table_by_id(DropTableByIdReq {
                if_exists: false,
                tenant: tenant.clone(),
                db_id: old_db.database_id.db_id,
                table_name: tbl_name.to_string(),
                db_name: tbl_name.to_string(),
                tb_id: tb_info.ident.table_id,
                engine: "FUSE".to_string(),
                temp_prefix: "".to_string(),
            })
            .await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let res = util.list_history_tables(false).await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                drop_on_cnt: 2,
                non_drop_on_cnt: 0,
            }]);

            let old_db = util.get_database().await?;
            mt.undrop_table(UndropTableReq {
                name_ident: tbl_name_ident.clone(),
            })
            .await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let res = util.list_history_tables(false).await?;

            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 1,
            }]);

            let res = mt
                .undrop_table(UndropTableReq {
                    name_ident: tbl_name_ident.clone(),
                })
                .await;
            assert!(res.is_err());
            let code = ErrorCode::from(res.unwrap_err()).code();
            let undrop_table_already_exists = ErrorCode::UNDROP_TABLE_ALREADY_EXISTS;
            assert_eq!(undrop_table_already_exists, code);
        }

        info!("--- rename table");
        {
            // first create drop table2
            let old_db = util.get_database().await?;
            let (_table_id, _) = util
                .create_table_with(
                    |_meta| create_table_meta.clone(),
                    |mut req| {
                        req.name_ident.table_name = new_tbl_name.to_string();
                        req
                    },
                )
                .await?;
            let res = util.list_history_tables(false).await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            calc_and_compare_drop_on_table_result(res, vec![
                DroponInfo {
                    name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: DBIdTableName::new(*db_id, new_tbl_name).to_string_key(),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 1,
                },
            ]);

            let new_tb_info = util.get_table_by_name(new_tbl_name).await?;

            // then drop table2
            let drop_plan = DropTableByIdReq {
                if_exists: false,
                tenant: tenant.clone(),
                db_id: cur_db.database_id.db_id,
                table_name: tbl_name.to_string(),
                db_name: db_name.to_string(),
                tb_id: new_tb_info.ident.table_id,
                engine: "FUSE".to_string(),
                temp_prefix: "".to_string(),
            };

            let old_db = util.get_database().await?;
            mt.drop_table_by_id(drop_plan.clone()).await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let res = util.list_history_tables(false).await?;
            calc_and_compare_drop_on_table_result(res, vec![
                DroponInfo {
                    name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: DBIdTableName::new(*db_id, new_tbl_name).to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
            ]);

            // then rename table to table2
            let rename_dbtb_to_dbtb1 = |if_exists| RenameTableReq {
                if_exists,
                name_ident: tbl_name_ident.clone(),
                new_db_name: db_name.to_string(),
                new_table_name: new_tbl_name.to_string(),
            };

            let old_db = util.get_database().await?;
            let _ = mt.rename_table(rename_dbtb_to_dbtb1(false)).await;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);

            let res = util.list_history_tables(false).await?;
            calc_and_compare_drop_on_table_result(res, vec![
                DroponInfo {
                    name: DBIdTableName::new(*db_id, tbl_name).to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
                DroponInfo {
                    name: DBIdTableName::new(*db_id, new_tbl_name).to_string_key(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
            ]);
        }

        Ok(())
    }

    async fn table_commit_table_meta<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + GarbageCollectionApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "table_commit_table_meta_tenant";
        let db_name = "db1";
        let tbl_name = "tb2";

        info!("--- prepare db");
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "");
        util.create_db().await?;
        let db_id = util.db_id();

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };
        let drop_table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            drop_on: Some(created_on),
            ..TableMeta::default()
        };
        let created_on = Utc::now();

        let create_table_req = CreateTableReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident: TableNameIdent {
                tenant: util.tenant(),
                db_name: db_name.to_string(),
                table_name: tbl_name.to_string(),
            },
            table_meta: drop_table_meta(created_on),
            as_dropped: true,
            table_properties: None,
            table_partition: None,
        };

        let create_table_as_dropped_resp = mt.create_table(create_table_req.clone()).await?;

        // commit table meta with a wrong prev_table_id will fail
        {
            let commit_table_req = CommitTableMetaReq {
                name_ident: create_table_req.name_ident.clone(),
                db_id: create_table_as_dropped_resp.db_id,
                table_id: create_table_as_dropped_resp.table_id,
                prev_table_id: Some(1111),
                orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
            };
            let resp = mt.commit_table_meta(commit_table_req).await;
            use databend_common_meta_app::app_error::AppError;
            assert!(matches!(
                resp.unwrap_err(),
                KVAppError::AppError(AppError::CommitTableMetaError(_))
            ));
        }

        // commit table meta with a wrong orphan table id list will fail
        {
            // first update orphan_table_id_list
            let mut orphan_table_id_list: TableIdList = TableIdList::new();
            orphan_table_id_list.append(1);
            orphan_table_id_list.append(2);
            let key_table_id_list = TableIdHistoryIdent {
                database_id: *db_id,
                table_name: create_table_as_dropped_resp
                    .orphan_table_name
                    .clone()
                    .unwrap(),
            };
            upsert_test_data(
                mt,
                &key_table_id_list,
                serialize_struct(&orphan_table_id_list)?,
            )
            .await?;

            let commit_table_req = CommitTableMetaReq {
                name_ident: create_table_req.name_ident.clone(),
                db_id: create_table_as_dropped_resp.db_id,
                table_id: create_table_as_dropped_resp.table_id,
                prev_table_id: create_table_as_dropped_resp.prev_table_id,
                orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
            };
            let resp = mt.commit_table_meta(commit_table_req).await;
            use databend_common_meta_app::app_error::AppError;
            assert!(matches!(
                resp.unwrap_err(),
                KVAppError::AppError(AppError::CommitTableMetaError(_))
            ));
        }

        {
            // first update orphan_table_id_list
            let mut orphan_table_id_list: TableIdList = TableIdList::new();
            orphan_table_id_list.append(1);
            let key_table_id_list = TableIdHistoryIdent {
                database_id: *db_id,
                table_name: create_table_as_dropped_resp
                    .orphan_table_name
                    .clone()
                    .unwrap(),
            };
            upsert_test_data(
                mt,
                &key_table_id_list,
                serialize_struct(&orphan_table_id_list)?,
            )
            .await?;

            // replace with a new as_dropped = false table
            util.create_table_with(
                |_meta| table_meta(created_on),
                |mut req| {
                    req.create_option = CreateOption::CreateOrReplace;
                    req
                },
            )
            .await?;

            let table_name_ident = TableNameIdent {
                tenant: util.tenant(),
                db_name: db_name.to_string(),
                table_name: tbl_name.to_string(),
            };
            let commit_table_req = CommitTableMetaReq {
                name_ident: table_name_ident,
                db_id: create_table_as_dropped_resp.db_id,
                table_id: create_table_as_dropped_resp.table_id,
                prev_table_id: create_table_as_dropped_resp.prev_table_id,
                orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
            };
            let resp = mt.commit_table_meta(commit_table_req).await;
            use databend_common_meta_app::app_error::AppError;
            assert!(matches!(
                resp.unwrap_err(),
                KVAppError::AppError(AppError::CommitTableMetaError(_))
            ));
        }

        {
            use databend_common_meta_app::app_error::AppError;

            // replace with a new as_dropped = true and table_meta.as_drop is None table
            let create_table_req = CreateTableReq {
                create_option: CreateOption::CreateOrReplace,
                catalog_name: Some("default".to_string()),
                name_ident: TableNameIdent {
                    tenant: util.tenant(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: true,
                table_properties: None,
                table_partition: None,
            };

            let resp = mt.create_table(create_table_req.clone()).await;
            assert!(matches!(
                resp.unwrap_err(),
                KVAppError::AppError(AppError::CreateAsDropTableWithoutDropTime(_))
            ));

            let create_table_req = CreateTableReq {
                create_option: CreateOption::CreateOrReplace,
                catalog_name: Some("default".to_string()),
                name_ident: TableNameIdent {
                    tenant: util.tenant(),
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: drop_table_meta(created_on),
                as_dropped: true,
                table_properties: None,
                table_partition: None,
            };

            let create_table_as_dropped_resp = mt.create_table(create_table_req.clone()).await?;

            let commit_table_req = CommitTableMetaReq {
                name_ident: create_table_req.name_ident.clone(),
                db_id: create_table_as_dropped_resp.db_id,
                table_id: create_table_as_dropped_resp.table_id,
                prev_table_id: create_table_as_dropped_resp.prev_table_id,
                orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
            };
            mt.commit_table_meta(commit_table_req).await?;
        }

        // verify the orphan table id list will be vacuum
        {
            // use a new tenant and db do test
            let db_name = "orphan_db";
            let tenant_name = "orphan_tenant";
            let mut orphan_util = DbTableHarness::new(mt, tenant_name, db_name, "", "");
            orphan_util.create_db().await?;
            let db_id = orphan_util.db_id();
            let tenant = orphan_util.tenant();

            let create_table_req = CreateTableReq {
                create_option: CreateOption::CreateOrReplace,
                catalog_name: Some("default".to_string()),
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: drop_table_meta(created_on),
                as_dropped: true,
                table_properties: None,
                table_partition: None,
            };

            let create_table_as_dropped_resp = mt.create_table(create_table_req.clone()).await?;

            let key_table_id_list = TableIdHistoryIdent {
                database_id: *db_id,
                table_name: create_table_as_dropped_resp
                    .orphan_table_name
                    .clone()
                    .unwrap(),
            };

            // assert orphan table id list and table meta exists
            let seqv = mt.get_kv(&key_table_id_list.to_string_key()).await?;
            assert!(seqv.is_some() && seqv.unwrap().seq != 0);
            let table_key = TableId {
                table_id: create_table_as_dropped_resp.table_id,
            };
            let seqv = mt.get_kv(&table_key.to_string_key()).await?;
            assert!(seqv.is_some() && seqv.unwrap().seq != 0);

            // vacuum drop table
            let req = ListDroppedTableReq::new(&tenant);
            let resp = mt.get_drop_table_infos(req).await?;
            assert!(!resp.drop_ids.is_empty());

            let req = GcDroppedTableReq {
                tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                catalog: "default".to_string(),
                drop_ids: resp.drop_ids.clone(),
            };
            mt.gc_drop_tables(req).await?;

            // assert orphan table id list and table meta has been vacuum
            let seqv = mt.get_kv(&key_table_id_list.to_string_key()).await?;
            assert!(seqv.is_none());
            let table_key = TableId {
                table_id: create_table_as_dropped_resp.table_id,
            };
            let seqv = mt.get_kv(&table_key.to_string_key()).await?;
            assert!(seqv.is_none());
        }

        Ok(())
    }

    async fn concurrent_commit_table_meta<
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + 'static,
    >(
        &self,
        b: B,
    ) -> anyhow::Result<()> {
        let db_name = "db";
        let tenant_name = "concurrent_commit_table_meta";
        let tbl_name = "tb";

        let mt = Arc::new(b.build().await);
        let mut util = DbTableHarness::new(&*mt, tenant_name, db_name, tbl_name, "");
        util.create_db().await?;
        let db_id = util.db_id();

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let drop_table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            drop_on: Some(created_on),
            ..TableMeta::default()
        };
        let created_on = Utc::now();

        let create_table_req = CreateTableReq {
            create_option: CreateOption::CreateOrReplace,
            catalog_name: Some("default".to_string()),
            name_ident: TableNameIdent {
                tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                db_name: db_name.to_string(),
                table_name: tbl_name.to_string(),
            },
            table_meta: drop_table_meta(created_on),
            as_dropped: true,
            table_properties: None,
            table_partition: None,
        };

        let concurrent_count: usize = 5;
        let runtime = Runtime::with_worker_threads(concurrent_count, None)?;
        let mut handles = vec![];

        for _i in 0..concurrent_count {
            let create_table_req = create_table_req.clone();
            let arc_mt = mt.clone();

            let handle = runtime.spawn(async move {
                let resp = arc_mt.create_table(create_table_req.clone()).await;

                // assert that when create table concurrently with correct params return error,
                // the error MUST be TxnRetryMaxTimes
                if resp.is_err() {
                    assert!(matches!(
                        resp.unwrap_err(),
                        KVAppError::AppError(AppError::TxnRetryMaxTimes(_))
                    ));
                    return;
                }

                let resp = resp.unwrap();

                let commit_table_req = CommitTableMetaReq {
                    name_ident: create_table_req.name_ident.clone(),
                    db_id: *db_id,
                    table_id: resp.table_id,
                    prev_table_id: resp.prev_table_id,
                    orphan_table_name: resp.orphan_table_name.clone(),
                };
                let resp = arc_mt.commit_table_meta(commit_table_req).await;

                // assert that when commit_table_meta concurrently with correct params return error,
                // the error MUST be TxnRetryMaxTimes or CommitTableMetaError(prev table id has been changed)
                if resp.is_err() {
                    assert!(
                        matches!(
                            resp.clone().unwrap_err(),
                            KVAppError::AppError(AppError::TxnRetryMaxTimes(_))
                        ) || matches!(
                            resp.unwrap_err(),
                            KVAppError::AppError(AppError::CommitTableMetaError(_))
                        )
                    );
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    async fn get_table_by_id<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "");
        util.create_db().await?;

        assert_eq!(1, *util.db_id(), "first database id is 1");

        info!("--- create and get table");
        {
            let created_on = Utc::now();

            {
                let old_db = util.get_database().await?;
                let (table_id, _) = util
                    .create_table_with(|_meta| table_meta(created_on), |req| req)
                    .await?;
                let cur_db = util.get_database().await?;
                assert!(old_db.meta.seq < cur_db.meta.seq);
                assert!(table_id >= 1, "table id >= 1");
                let tb_id = table_id;

                let got = util.get_table().await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident,
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                    ..Default::default()
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            };
        }

        info!("--- get_table_by_id ");
        {
            info!("--- get_table_by_id ");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let seqv = mt.get_table_by_id(table.ident.table_id).await?.unwrap();
                let table_meta = seqv.data;
                assert_eq!(table_meta.options.get("opt‐1"), Some(&"val-1".into()));
            }

            info!("--- get_table_by_id with not exists table_id");
            {
                let got = mt.get_table_by_id(1024).await?;

                assert!(got.is_none());
            }
        }
        Ok(())
    }

    async fn get_table_name_by_id<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi {
        let tenant_name = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";

        let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "eng1");
        let table_id;

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (tid, _table_meta) = util.create_table().await?;
            table_id = tid;
        }

        info!("--- get_table_name_by_id ");
        {
            info!("--- get_table_name_by_id ");
            {
                let got = mt.get_table_name_by_id(table_id).await?;
                assert!(got.is_some());
                assert_eq!(tbl_name.to_owned(), got.unwrap());
            }

            info!("--- get_table_name_by_id with not exists table_id");
            {
                let got = mt.get_table_name_by_id(1024).await?;
                assert!(got.is_none());
            }
        }
        Ok(())
    }

    async fn get_db_name_by_id<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";

        info!("--- prepare and get db");
        {
            let mut util = DbTableHarness::new(mt, tenant_name, db_name, "", "");
            util.create_db().await?;

            assert_eq!(1, *util.db_id(), "first database id is 1");

            let got = mt.get_db_name_by_id(*util.db_id()).await?;
            assert_eq!(got, db_name.to_string())
        }

        info!("--- get_db_name_by_id ");
        {
            info!("--- get_db_name_by_id ");
            {
                let plan = GetDatabaseReq {
                    inner: DatabaseNameIdent::new(&tenant, db_name),
                };

                let db = mt.get_database(plan).await.unwrap();

                let got = mt.get_db_name_by_id(db.database_id.db_id).await?;

                assert_eq!(got, db_name.to_string());
            }

            info!("--- get_db_name_by_id with not exists db_id");
            {
                let got = mt.get_db_name_by_id(1024).await;

                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::UNKNOWN_DATABASE_ID, err.code());
            }
        }
        Ok(())
    }

    async fn test_sequence_0<MT: kvapi::KVApi<Error = MetaError> + SequenceApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        self.test_sequence_with_version(mt, 0).await
    }

    async fn test_sequence_1<MT: kvapi::KVApi<Error = MetaError> + SequenceApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        self.test_sequence_with_version(mt, 1).await
    }

    async fn test_sequence_with_version<MT: kvapi::KVApi<Error = MetaError> + SequenceApi>(
        &self,
        mt: &MT,
        storage_version: u64,
    ) -> anyhow::Result<()> {
        let tenant = Tenant {
            tenant: "tenant1".to_string(),
        };
        let sequence_name = "seq";

        let create_on = Utc::now();

        info!("--- create sequence");
        {
            let req = CreateSequenceReq {
                create_option: CreateOption::Create,
                ident: SequenceIdent::new(&tenant, sequence_name),
                create_on,
                start: 1,
                increment: 1,
                comment: Some("seq".to_string()),
                storage_version,
            };

            mt.create_sequence(req).await?;
        }

        info!("--- get sequence");
        {
            let req = SequenceIdent::new(&tenant, sequence_name);
            let resp = mt.get_sequence(&req).await?;
            let resp = resp.unwrap().data;
            assert_eq!(resp.comment, Some("seq".to_string()));
            assert_eq!(resp.current, 1);
        }

        info!("--- list sequence");
        {
            let req = CreateSequenceReq {
                create_option: CreateOption::Create,
                ident: SequenceIdent::new(&tenant, "seq1"),
                create_on,
                start: 1,
                increment: 1,
                comment: Some("seq1".to_string()),
                storage_version,
            };

            mt.create_sequence(req).await?;
            let values = mt.list_sequences(&tenant).await?;
            assert_eq!(values.len(), 2);
            assert_eq!(values[0].0, sequence_name);
            assert_eq!(values[0].1.current, 1);
            assert_eq!(values[1].0, "seq1");
            assert_eq!(values[1].1.current, 1);
        }

        info!("--- get sequence nextval");
        {
            let req = GetSequenceNextValueReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
                count: 10,
            };
            let resp = mt.get_sequence_next_value(req).await?;
            assert_eq!(resp.start, 1);
            assert_eq!(resp.end, 11);
        }

        info!("--- get sequence after nextval");
        {
            let req = SequenceIdent::new(&tenant, sequence_name);
            let resp = mt.get_sequence(&req).await?;
            let resp = resp.unwrap().data;
            assert_eq!(resp.comment, Some("seq".to_string()));
            assert_eq!(resp.current, 11);
        }

        info!("--- list sequence after nextval");
        {
            let values = mt.list_sequences(&tenant).await?;
            assert_eq!(values.len(), 2);
            assert_eq!(values[0].0, sequence_name);
            assert_eq!(values[0].1.current, 11);
            assert_eq!(values[1].0, "seq1");
            assert_eq!(values[1].1.current, 1);
        }

        info!("--- replace sequence");
        {
            let req = CreateSequenceReq {
                create_option: CreateOption::CreateOrReplace,
                ident: SequenceIdent::new(&tenant, sequence_name),
                create_on,
                start: 1,
                increment: 1,
                comment: Some("seq1".to_string()),
                storage_version,
            };

            mt.create_sequence(req).await?;

            let req = SequenceIdent::new(&tenant, sequence_name);

            let resp = mt.get_sequence(&req).await?;
            let resp = resp.unwrap().data;
            assert_eq!(resp.comment, Some("seq1".to_string()));
            assert_eq!(resp.current, 1);
        }

        info!("--- drop sequence");
        {
            let req = DropSequenceReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
                if_exists: true,
            };

            mt.drop_sequence(req).await?;

            let sequence_ident = SequenceIdent::new(&tenant, sequence_name);
            let req = SequenceIdent::new(&tenant, sequence_name);
            let resp = mt.get_sequence(&req).await?;
            assert!(resp.is_none());

            let storage_ident = SequenceStorageIdent::new_from(sequence_ident);
            let got = mt.get_kv(&storage_ident.to_string_key()).await?;
            assert!(
                got.is_none(),
                "storage_version==1 storage should be removed too"
            );
        }

        info!("--- list sequence after drop");
        {
            let values = mt.list_sequences(&tenant).await?;
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].0, "seq1");
            assert_eq!(values[0].1.current, 1);
        }

        Ok(())
    }

    async fn get_table_copied_file<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";

        let db_name = "db1";
        let tbl_name = "tb2";
        let table_id;

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };
        let created_on = Utc::now();
        let tbl_name_ident = TableNameIdent {
            tenant: Tenant::new_or_err(tenant_name, func_name!())?,
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };
        info!("--- prepare db and table");
        {
            let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "");
            util.create_db().await?;

            let (resp_table_id, _) = util
                .create_table_with(|_meta| table_meta(created_on), |req| req)
                .await?;
            table_id = resp_table_id;
        }

        info!("--- create and get stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                ttl: Some(std::time::Duration::from_secs(86400)),
                insert_if_not_exists: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
                base_snapshot_location: None,
                lvt_check: None,
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            mt.update_multi_table_meta(req).await?.unwrap();

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 1);
            let resp_stage_info = resp.file_info.get("file");
            assert_eq!(resp_stage_info.unwrap(), &stage_info);
        }

        info!("--- test again with expire stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file2".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                // Make it expire at once.
                ttl: Some(std::time::Duration::from_secs(0)),
                insert_if_not_exists: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
                base_snapshot_location: None,
                lvt_check: None,
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            mt.update_multi_table_meta(req).await?.unwrap();

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file2".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 0);
        }

        Ok(())
    }

    async fn truncate_table<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi {
        let mut util = DbTableHarness::new(mt, "tenant1", "db1", "tb2", "JSON");
        let table_id;

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (tid, _table_meta) = util.create_table().await?;
            table_id = tid;
        }

        let n = 7;
        info!("--- create copied file infos");
        let file_infos = util.update_copied_files(n).await?;

        info!("--- get copied file infos");
        {
            let req = GetTableCopiedFileReq {
                table_id,
                files: file_infos.keys().cloned().collect(),
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(file_infos, resp.file_info);
        }

        info!("--- list copied file infos");
        {
            let resp = mt.list_table_copied_file_info(table_id).await?;
            assert_eq!(file_infos, resp.file_info);
        }

        info!("--- truncate table and get stage file info again");
        {
            let req = TruncateTableReq {
                table_id,
                batch_size: Some(2),
            };

            mt.truncate_table(req).await?;

            let req = GetTableCopiedFileReq {
                table_id,
                files: file_infos.keys().cloned().collect(),
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 0);
        }

        Ok(())
    }

    async fn table_list<MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;
        let db_name = "db1";

        info!("--- prepare db and create 2 tables: tb1 tb2");
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, "", "eng1");
        util.create_db().await?;
        assert_eq!(1, *util.db_id(), "first database id is 1");

        let db_id = DatabaseId::new(1u64);

        let tb_ids = {
            // Table schema with metadata(due to serde issue).
            let schema = Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]));

            let options = maplit::btreemap! {"opt‐1".into() => "val-1".into()};

            // Create first table "tb1"
            let mut util1 = DbTableHarness::new(mt, tenant_name, db_name, "tb1", "JSON");
            let old_db = util1.get_database().await?;
            let (tb_id1, _) = util1
                .create_table_with(
                    |meta| TableMeta {
                        schema: schema.clone(),
                        options: options.clone(),
                        ..meta
                    },
                    |req| req,
                )
                .await?;
            let cur_db = util1.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            assert!(tb_id1 >= 1, "table id >= 1");

            // Create second table "tb2"
            let mut util2 = DbTableHarness::new(mt, tenant_name, db_name, "tb2", "JSON");
            let old_db = util2.get_database().await?;
            let (tb_id2, _) = util2
                .create_table_with(
                    |meta| TableMeta {
                        schema: schema.clone(),
                        options: options.clone(),
                        ..meta
                    },
                    |req| req,
                )
                .await?;
            let cur_db = util2.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            assert!(tb_id2 > tb_id1, "table id > tb_id1: {}", tb_id1);

            vec![tb_id1, tb_id2]
        };

        info!("--- get_tables");
        {
            let res = mt.list_tables(ListTableReq::new(&tenant, db_id)).await?;
            assert_eq!(tb_ids.len(), res.len());
            assert_eq!(tb_ids[0], res[0].1.table_id);
            assert_eq!(tb_ids[1], res[1].1.table_id);
        }

        Ok(())
    }

    /// Test listing many tables that exceeds default mget chunk size.
    async fn table_list_many<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi {
        // Create tables that exceeds the default mget chunk size
        let n = DEFAULT_MGET_SIZE + 20;

        let mut util = DbTableHarness::new(mt, "tenant1", "db1", "tb1", "eng1");

        info!("--- prepare db");
        {
            util.create_db().await?;
        }

        info!("--- create {} tables", n);
        {
            for i in 0..n {
                let table_name = format!("tb_{:0>5}", i);

                let (table_id, _) = util
                    .create_table_with(
                        |meta| meta,
                        |mut req| {
                            req.name_ident.table_name = table_name.clone();
                            req
                        },
                    )
                    .await?;

                if i % 100 == 0 {
                    info!("--- created {} tables: table_id={}", i, table_id);
                }
            }
        }

        info!("--- get_tables");
        {
            let res = mt
                .list_tables(ListTableReq::new(&util.tenant(), util.db_id()))
                .await?;
            assert_eq!(n, res.len());
        }

        Ok(())
    }

    async fn table_index_create_drop<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + IndexApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(TableSchema::new(vec![
                TableField::new("title", TableDataType::String),
                TableField::new("content", TableDataType::String),
                TableField::new("author", TableDataType::String),
            ]))
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            created_on,
            ..TableMeta::default()
        };

        let created_on = Utc::now();

        info!("--- prepare db and table");
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "");
        util.create_db().await?;

        let (resp_table_id, _) = util
            .create_table_with(|_meta| table_meta(created_on), |req| req)
            .await?;
        let table_id = resp_table_id;

        let index_name_1 = "idx1".to_string();
        let index_version_1;
        let index_column_ids_1 = vec![0, 1];
        let index1_drop_start_time;
        let index1_drop_end_time;
        let index_name_2 = "idx2".to_string();
        let index_version_2;
        let index2_drop_start_time;
        let index2_drop_end_time;
        let index_column_ids_2 = vec![2];
        let index_name_3 = "idx3".to_string();
        let index_column_ids_3 = vec![3];
        let index_version_3;
        let index3_drop_start_time;
        let index3_drop_end_time;

        {
            info!("--- create table index 1");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                index_type: TableIndexType::Inverted,
                tenant: tenant.clone(),
                table_id,
                name: index_name_1.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
            };
            let res = mt.create_table_index(req).await;
            assert!(res.is_ok());

            index_version_1 = {
                let seqv = mt.get_table_by_id(table_id).await?.unwrap();
                let index = seqv.data.indexes.get(&index_name_1).unwrap();
                index.version.clone()
            };

            info!("--- create table index 2 with duplicate column id");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                tenant: tenant.clone(),
                name: index_name_2.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
                index_type: TableIndexType::Inverted,
            };
            let res = mt.create_table_index(req).await;
            assert!(res.is_err());

            info!("--- create table index 2");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                tenant: tenant.clone(),
                name: index_name_2.clone(),
                column_ids: index_column_ids_2.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
                index_type: TableIndexType::Inverted,
            };
            let res = mt.create_table_index(req).await;
            assert!(res.is_ok());

            index_version_2 = {
                let seqv = mt.get_table_by_id(table_id).await?.unwrap();
                let index = seqv.data.indexes.get(&index_name_2).unwrap();
                index.version.clone()
            };
        }

        {
            info!("--- create table index again with if_not_exists = false");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                tenant: tenant.clone(),
                name: index_name_1.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
                index_type: TableIndexType::Inverted,
            };

            let res = mt.create_table_index(req).await;
            assert!(res.is_err());
            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(ErrorCode::INDEX_ALREADY_EXISTS, err_code.code());
        }

        {
            info!("--- create table index again with if_not_exists = true");
            let req = CreateTableIndexReq {
                create_option: CreateOption::CreateIfNotExists,
                table_id,
                tenant: tenant.clone(),
                name: index_name_1.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
                index_type: TableIndexType::Inverted,
            };

            let res = mt.create_table_index(req).await;
            assert!(res.is_ok());
        }

        {
            info!("--- create table index with invalid column id");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                tenant: tenant.clone(),
                name: index_name_3.clone(),
                column_ids: index_column_ids_3.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
                index_type: TableIndexType::Inverted,
            };
            let res = mt.create_table_index(req).await;
            assert!(res.is_err());
        }

        {
            info!("--- create table index with duplicate column id and different index type");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                tenant: tenant.clone(),
                name: index_name_3.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
                index_type: TableIndexType::Ngram,
            };
            let res = mt.create_table_index(req).await;
            assert!(res.is_ok());

            index_version_3 = {
                let seqv = mt.get_table_by_id(table_id).await?.unwrap();
                let index = seqv.data.indexes.get(&index_name_3).unwrap();
                index.version.clone()
            };
        }

        {
            info!("--- check table index");
            let seqv = mt.get_table_by_id(table_id).await?.unwrap();
            let table_meta = seqv.data;
            assert_eq!(table_meta.indexes.len(), 3);

            let index1 = table_meta.indexes.get(&index_name_1);
            assert!(index1.is_some());
            let index1 = index1.unwrap();
            assert_eq!(index1.column_ids, index_column_ids_1);

            let index2 = table_meta.indexes.get(&index_name_2);
            assert!(index2.is_some());
            let index2 = index2.unwrap();
            assert_eq!(index2.column_ids, index_column_ids_2);

            let index3 = table_meta.indexes.get(&index_name_3);
            assert!(index3.is_some());
            let index3 = index3.unwrap();
            assert_eq!(index3.column_ids, index_column_ids_1);
        }

        {
            info!("--- drop table index");
            let req = DropTableIndexReq {
                index_type: TableIndexType::Inverted,
                tenant: tenant.clone(),
                if_exists: false,
                table_id,
                name: index_name_1.clone(),
            };
            index1_drop_start_time = Utc::now();
            let res = mt.drop_table_index(req).await;
            index1_drop_end_time = Utc::now();
            assert!(res.is_ok());

            let req = DropTableIndexReq {
                index_type: TableIndexType::Inverted,
                tenant: tenant.clone(),
                if_exists: false,
                table_id,
                name: index_name_1.clone(),
            };
            let res = mt.drop_table_index(req).await;
            assert!(res.is_err());

            let req = DropTableIndexReq {
                index_type: TableIndexType::Inverted,
                tenant: tenant.clone(),
                if_exists: true,
                table_id,
                name: index_name_1.clone(),
            };
            let res = mt.drop_table_index(req).await;
            assert!(res.is_ok());

            info!("--- drop table index with different index type");
            let req = DropTableIndexReq {
                index_type: TableIndexType::Inverted,
                tenant: tenant.clone(),
                if_exists: true,
                table_id,
                name: index_name_3.clone(),
            };
            let res = mt.drop_table_index(req).await;
            assert!(res.is_err());

            let req = DropTableIndexReq {
                index_type: TableIndexType::Ngram,
                tenant: tenant.clone(),
                if_exists: true,
                table_id,
                name: index_name_3.clone(),
            };
            index3_drop_start_time = Utc::now();
            let res = mt.drop_table_index(req).await;
            index3_drop_end_time = Utc::now();
            assert!(res.is_ok());
        }

        {
            info!("--- get marked deleted table indexes after drop");
            let results = vec![
                mt.list_marked_deleted_table_indexes(&tenant, Some(table_id))
                    .await?,
                mt.list_marked_deleted_table_indexes(&tenant, None).await?,
            ];
            for res in results {
                let table_indexes = res.table_indexes.get(&table_id);
                assert!(table_indexes.is_some());
                let table_indexes = table_indexes.unwrap();
                assert_eq!(table_indexes.len(), 2);
                let (index_name, index_version, index_meta) = table_indexes[0].clone();
                assert_eq!(index_name, index_name_1);
                assert_eq!(index_version, index_version_1);
                assert!(matches!(
                    index_meta.index_type,
                    MarkedDeletedIndexType::INVERTED
                ));
                assert!(index_meta.dropped_on > index1_drop_start_time);
                assert!(index_meta.dropped_on < index1_drop_end_time);

                let (index_name, index_version, index_meta) = table_indexes[1].clone();
                assert_eq!(index_name, index_name_3);
                assert_eq!(index_version, index_version_3);
                assert!(matches!(
                    index_meta.index_type,
                    MarkedDeletedIndexType::NGRAM
                ));
                assert!(index_meta.dropped_on > index3_drop_start_time);
                assert!(index_meta.dropped_on < index3_drop_end_time);
            }
        }

        {
            info!("--- check table index after drop");
            let seqv = mt.get_table_by_id(table_id).await?.unwrap();
            let table_meta = seqv.data;
            assert_eq!(table_meta.indexes.len(), 1);

            let index1 = table_meta.indexes.get(&index_name_1);
            assert!(index1.is_none());

            let index2 = table_meta.indexes.get(&index_name_2);
            assert!(index2.is_some());
            let index2 = index2.unwrap();
            assert_eq!(index2.column_ids, index_column_ids_2);
        }

        {
            info!("--- replace index_2");
            let req = CreateTableIndexReq {
                create_option: CreateOption::CreateOrReplace,
                table_id,
                tenant: tenant.clone(),
                name: index_name_2.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
                index_type: TableIndexType::Inverted,
            };
            index2_drop_start_time = Utc::now();
            let res = mt.create_table_index(req).await;
            index2_drop_end_time = Utc::now();
            assert!(res.is_ok(), "{}", res.err().unwrap());
        }

        {
            info!("--- get marked deleted table indexes after replace");
            let results = vec![
                mt.list_marked_deleted_table_indexes(&tenant, Some(table_id))
                    .await?,
                mt.list_marked_deleted_table_indexes(&tenant, None).await?,
            ];

            for res in results {
                let table_indexes = res.table_indexes.get(&table_id);
                assert!(table_indexes.is_some());
                let mut table_indexes = table_indexes.unwrap().clone();
                assert_eq!(table_indexes.len(), 3);
                table_indexes.sort_by(|a, b| a.0.cmp(&b.0));
                let (actual_index_name_1, actual_index_version_1, actual_index_meta_1) =
                    table_indexes[0].clone();
                let (actual_index_name_2, actual_index_version_2, actual_index_meta_2) =
                    table_indexes[1].clone();
                let (actual_index_name_3, actual_index_version_3, actual_index_meta_3) =
                    table_indexes[2].clone();
                assert_eq!(actual_index_name_1, index_name_1);
                assert_eq!(actual_index_name_2, index_name_2);
                assert_eq!(actual_index_name_3, index_name_3);
                assert_eq!(actual_index_version_1, index_version_1);
                assert_eq!(actual_index_version_2, index_version_2);
                assert_eq!(actual_index_version_3, index_version_3);
                assert!(matches!(
                    actual_index_meta_1.index_type,
                    MarkedDeletedIndexType::INVERTED
                ));
                assert!(matches!(
                    actual_index_meta_2.index_type,
                    MarkedDeletedIndexType::INVERTED
                ));
                assert!(matches!(
                    actual_index_meta_3.index_type,
                    MarkedDeletedIndexType::NGRAM
                ));
                assert!(actual_index_meta_1.dropped_on > index1_drop_start_time);
                assert!(actual_index_meta_1.dropped_on < index1_drop_end_time);
                assert!(actual_index_meta_2.dropped_on > index2_drop_start_time);
                assert!(actual_index_meta_2.dropped_on < index2_drop_end_time);
                assert!(actual_index_meta_3.dropped_on > index3_drop_start_time);
                assert!(actual_index_meta_3.dropped_on < index3_drop_end_time);
            }

            {
                info!("--- remove marked deleted table indexes");
                mt.remove_marked_deleted_table_indexes(&tenant, table_id, &[
                    (index_name_1, index_version_1),
                    (index_name_2, index_version_2),
                    (index_name_3, index_version_3),
                ])
                .await?;

                let res = mt
                    .list_marked_deleted_table_indexes(&tenant, Some(table_id))
                    .await?;
                assert_eq!(res.table_indexes.len(), 0);
                let res = mt.list_marked_deleted_table_indexes(&tenant, None).await?;
                assert_eq!(res.table_indexes.len(), 0);
            }
        }

        Ok(())
    }

    async fn index_create_list_drop<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + IndexApi {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_literal(tenant_name);

        let mut util = DbTableHarness::new(mt, tenant_name, "db1", "tb1", "eng1");
        let table_id;
        let index_id;
        let index_id_2;

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (tid, _table_meta) = util.create_table().await?;
            table_id = tid;
        }

        let created_on = Utc::now();

        let index_name_1 = "idx1";
        let index_meta_1 = IndexMeta {
            table_id,
            index_type: IndexType::AGGREGATING,
            created_on,
            dropped_on: None,
            updated_on: None,
            original_query: "SELECT a".to_string(),
            query: "SELECT b".to_string(),
            sync_creation: false,
        };

        let index_name_2 = "idx2";
        let index_meta_2 = IndexMeta {
            table_id,
            index_type: IndexType::AGGREGATING,
            created_on,
            dropped_on: None,
            updated_on: None,
            original_query: "SELECT a, SUM(b) FROM tb1 WHERE b > 1 GROUP BY b".to_string(),
            query: "SELECT a, SUM(b) FROM tb1 WHERE b > 1 GROUP BY b".to_string(),
            sync_creation: false,
        };

        let name_ident_1 = IndexNameIdent::new(&tenant, index_name_1);

        let name_ident_2 = IndexNameIdent::new(&tenant, index_name_2);

        {
            info!("--- list index with no create before");
            let req = ListIndexesReq::new(&tenant, Some(table_id));

            let res = mt.list_indexes(req).await?;
            assert!(res.is_empty())
        }

        {
            info!("--- create index");
            let req = CreateIndexReq {
                create_option: CreateOption::Create,
                name_ident: name_ident_1.clone(),
                meta: index_meta_1.clone(),
            };

            let res = mt.create_index(req).await?;
            index_id = res.index_id;

            // check reverse index id -> name
            {
                let index_id = IndexId::new(index_id);
                let id_ident = IndexIdToNameIdent::new_generic(&tenant, index_id);
                let raw_name: IndexNameIdentRaw = get_kv_data(mt, &id_ident).await?;
                assert_eq!(name_ident_1.to_raw(), raw_name);
            }

            let req = CreateIndexReq {
                create_option: CreateOption::Create,
                name_ident: name_ident_2.clone(),
                meta: index_meta_2.clone(),
            };

            let res = mt.create_index(req).await?;
            index_id_2 = res.index_id;
        }

        {
            info!("--- create index again with if_not_exists = false");
            let req = CreateIndexReq {
                create_option: CreateOption::Create,
                name_ident: name_ident_1.clone(),
                meta: index_meta_1.clone(),
            };

            let res = mt.create_index(req).await;
            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(ErrorCode::INDEX_ALREADY_EXISTS, err_code.code());
        }

        {
            info!("--- create index again with if_not_exists = true");
            let req = CreateIndexReq {
                create_option: CreateOption::CreateIfNotExists,
                name_ident: name_ident_1.clone(),
                meta: index_meta_1.clone(),
            };

            let res = mt.create_index(req).await?;
            assert_eq!(index_id, res.index_id);
        }

        {
            info!("--- list index");
            let req = ListIndexesReq::new(&tenant, None);

            let res = mt.list_indexes(req).await?;
            assert_eq!(2, res.len());

            let req = ListIndexesReq::new(&tenant, Some(u64::MAX));

            let res = mt.list_indexes(req).await?;
            assert!(res.is_empty())
        }

        {
            info!("--- list indexes by table id");
            let req = ListIndexesReq::new(&tenant, Some(table_id));

            let res = mt.list_indexes(req).await?;
            assert_eq!(2, res.len());
        }

        {
            info!("--- get marked deleted indexes");
            let res = mt
                .list_marked_deleted_indexes(&tenant, Some(table_id))
                .await?;
            assert_eq!(res.table_indexes.len(), 0);

            let res = mt.list_marked_deleted_indexes(&tenant, None).await?;
            assert_eq!(res.table_indexes.len(), 0);
        }

        {
            info!("--- drop index");

            let res = mt.drop_index(&name_ident_2).await?;
            assert!(res.is_some())
        }

        {
            info!("--- get marked deleted indexes after drop one");
            let results = vec![
                mt.list_marked_deleted_indexes(&tenant, Some(table_id))
                    .await?,
                mt.list_marked_deleted_indexes(&tenant, None).await?,
            ];
            for res in results {
                assert_eq!(res.table_indexes.len(), 1);
                let index = res.table_indexes.get(&table_id);
                assert!(index.is_some());
                let index = index.unwrap();
                assert_eq!(index.len(), 1);
                let (res_index_id, res_index_meta) = index[0].clone();
                assert_eq!(res_index_id, index_id_2);
                assert_eq!(
                    res_index_meta.index_type,
                    MarkedDeletedIndexType::AGGREGATING
                );
            }
        }

        {
            info!("--- list index after drop one");
            let req = ListIndexesReq::new(&tenant, Some(table_id));

            let res = mt.list_indexes(req).await?;
            assert_eq!(1, res.len());
        }

        {
            info!("--- check list index content");
            let req = ListIndexesReq::new(&tenant, Some(table_id));

            let res = mt.list_indexes(req).await?;
            assert_eq!(1, res.len());
            assert_eq!(
                vec![(
                    index_name_1.to_string(),
                    IndexId::new(index_id),
                    index_meta_1.clone()
                )],
                res
            );
        }

        {
            info!("--- list index after drop all");
            let res = mt.drop_index(&name_ident_1).await?;
            assert!(res.is_some());

            let req = ListIndexesReq::new(&tenant, Some(table_id));

            let res = mt.list_indexes(req).await?;
            assert!(res.is_empty())
        }

        {
            info!("--- get marked deleted indexes after drop all");
            let results = vec![
                mt.list_marked_deleted_indexes(&tenant, Some(table_id))
                    .await?,
                mt.list_marked_deleted_indexes(&tenant, None).await?,
            ];
            for res in results {
                assert_eq!(res.table_indexes.len(), 1);
                let index = res.table_indexes.get(&table_id);
                assert!(index.is_some());
                let index = index.unwrap();
                assert_eq!(index.len(), 2);
                let res_index_ids = index.iter().map(|(id, _)| id).collect::<HashSet<_>>();
                assert!(res_index_ids.contains(&index_id));
                assert!(res_index_ids.contains(&index_id_2));

                assert!(index.iter().all(|(_, meta)| {
                    matches!(meta.index_type, MarkedDeletedIndexType::AGGREGATING)
                }));
            }
        }

        {
            info!("--- remove marked deleted indexes");
            mt.remove_marked_deleted_index_ids(&tenant, table_id, &[index_id])
                .await?;
            let results = vec![
                mt.list_marked_deleted_indexes(&tenant, Some(table_id))
                    .await?,
                mt.list_marked_deleted_indexes(&tenant, None).await?,
            ];
            for res in results {
                assert_eq!(res.table_indexes.len(), 1);
                let index = res.table_indexes.get(&table_id);
                assert!(index.is_some());
                let index = index.unwrap();
                assert_eq!(index.len(), 1);
                let (res_index_id, res_index_meta) = index[0].clone();
                assert_eq!(res_index_id, index_id_2);
                assert_eq!(
                    res_index_meta.index_type,
                    MarkedDeletedIndexType::AGGREGATING
                );
            }

            mt.remove_marked_deleted_index_ids(&tenant, table_id, &[index_id_2])
                .await?;
            let res = mt
                .list_marked_deleted_indexes(&tenant, Some(table_id))
                .await?;
            assert_eq!(res.table_indexes.len(), 0);
            let res = mt.list_marked_deleted_indexes(&tenant, None).await?;
            assert_eq!(res.table_indexes.len(), 0);
        }

        {
            info!("--- drop unknown index");
            let res = mt.drop_index(&name_ident_1).await?;
            assert!(res.is_none())
        }

        // create or replace index
        {
            info!("--- create or replace index");
            let replace_index_name = "replace_idx";
            let replace_name_ident = IndexNameIdent::new(&tenant, replace_index_name);
            let req = CreateIndexReq {
                create_option: CreateOption::Create,
                name_ident: replace_name_ident.clone(),
                meta: index_meta_1.clone(),
            };

            let res = mt.create_index(req).await?;
            let old_index_id = IndexId::new(res.index_id);
            let old_index_id_ident = old_index_id.into_t_ident(&tenant);

            let meta: IndexMeta = get_kv_data(mt, &old_index_id_ident).await?;
            assert_eq!(meta, index_meta_1);

            let resp = mt.get_index(&replace_name_ident).await?.unwrap();

            assert_eq!(resp.index_meta, index_meta_1);

            let req = CreateIndexReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: replace_name_ident.clone(),
                meta: index_meta_2.clone(),
            };

            let res = mt.create_index(req).await?;

            // assert old index id key has been deleted
            let meta: Result<IndexMeta, _> = get_kv_data(mt, &old_index_id_ident).await;
            assert_eq!(
                meta.unwrap_err().to_string(),
                "fail to access meta-store: fail to get_kv_data: not found, source: "
            );

            // assert old id-to-name has been deleted.
            {
                let old_index_id_to_name_ident =
                    IndexIdToNameIdent::new_generic(&tenant, old_index_id);
                let meta: Result<IndexNameIdentRaw, _> =
                    get_kv_data(mt, &old_index_id_to_name_ident).await;
                assert_eq!(
                    meta.unwrap_err().to_string(),
                    "fail to access meta-store: fail to get_kv_data: not found, source: "
                );
            }

            // assert new index id key has been created
            let index_id = IndexId::new(res.index_id);
            let index_id_ident = index_id.into_t_ident(&tenant);
            let meta: IndexMeta = get_kv_data(mt, &index_id_ident).await?;
            assert_eq!(meta, index_meta_2);

            let resp = mt.get_index(&replace_name_ident).await?.unwrap();

            assert_eq!(resp.index_meta, index_meta_2);
        }

        Ok(())
    }

    async fn table_lock_revision<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + LockApi {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_literal(tenant_name);

        let mut util = DbTableHarness::new(mt, tenant_name, "db1", "tb1", "eng1");
        let table_id;

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (tid, _table_meta) = util.create_table().await?;
            table_id = tid;
        }

        {
            info!("--- create table lock revision 1");
            let req1 = CreateLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
                ttl: std::time::Duration::from_secs(2),
                user: "root".to_string(),
                node: "node1".to_string(),
                query_id: "query1".to_string(),
            };
            let res1 = mt.create_lock_revision(req1).await?;

            info!("--- create table lock revision 2");
            let req2 = CreateLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
                ttl: std::time::Duration::from_secs(2),
                user: "root".to_string(),
                node: "node1".to_string(),
                query_id: "query2".to_string(),
            };
            let res2 = mt.create_lock_revision(req2).await?;
            assert!(res2.revision > res1.revision);

            info!("--- list table lock revisiosn");
            let req3 = ListLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
            };
            let res3 = mt.list_lock_revisions(req3).await?;
            assert_eq!(res3.len(), 2);
            assert_eq!(res3[0].0, res1.revision);
            assert_eq!(res3[1].0, res2.revision);

            info!("--- extend table lock revision 2 expire");
            let req4 = ExtendLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
                ttl: std::time::Duration::from_secs(4),
                revision: res2.revision,
                acquire_lock: true,
            };
            mt.extend_lock_revision(req4).await?;

            info!("--- table lock revision 1 retired");
            std::thread::sleep(std::time::Duration::from_secs(2));
            let req5 = ListLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
            };
            let res5 = mt.list_lock_revisions(req5).await?;
            assert_eq!(res5.len(), 1);
            assert_eq!(res5[0].0, res2.revision);

            info!("--- delete table lock revision 2");
            let req6 = DeleteLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
                revision: res2.revision,
            };
            mt.delete_lock_revision(req6).await?;

            info!("--- check table locks is empty");
            let req7 = ListLockRevReq {
                lock_key: LockKey::Table { tenant, table_id },
            };
            let res7 = mt.list_lock_revisions(req7).await?;
            assert_eq!(res7.len(), 0);
        }

        Ok(())
    }

    async fn gc_dropped_db_after_undrop<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi + GarbageCollectionApi,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1_gc_dropped_db_after_undrop";
        let db_name = "db1_gc_dropped_db_after_undrop";
        let mut util = DbTableHarness::new(mt, tenant_name, db_name, "", "eng");

        let tenant = util.tenant();
        let db_name_ident = DatabaseNameIdent::new(&tenant, db_name);

        // 1. Create database
        util.create_db().await?;
        let db_id = *util.db_id();

        info!("Created database with ID: {}", db_id);

        // 2. Drop database
        util.drop_db().await?;

        // 2.1. Check database is marked as dropped
        let req = ListDroppedTableReq::new(&tenant);
        let resp = mt.get_drop_table_infos(req).await?;

        // Filter for our specific database ID
        let drop_ids: Vec<DroppedId> = resp
            .drop_ids
            .into_iter()
            .filter(|id| {
                if let DroppedId::Db { db_id: id, .. } = id {
                    *id == db_id
                } else {
                    false
                }
            })
            .collect();

        assert!(
            !drop_ids.is_empty(),
            "Database being tested should be dropped"
        );

        // 3. Undrop the database
        //
        // A more rigorous test would verify the race condition protection, but difficult to implement as an integration test:
        // Ideally, we would undrop the database precisely after the `gc_drop_tables` process has verified
        // that the database is marked as dropped, but before committing the kv transaction that removes the database metadata.

        let undrop_req = UndropDatabaseReq {
            name_ident: db_name_ident.clone(),
        };
        mt.undrop_database(undrop_req).await?;

        let req = GcDroppedTableReq {
            tenant: tenant.clone(),
            catalog: "default".to_string(),
            drop_ids,
        };

        // 4. Check that gc_drop_tables operation has NOT removed database's meta data
        mt.gc_drop_tables(req.clone()).await?;

        // 5. Verify the database is still accessible
        let get_req = GetDatabaseReq::new(tenant.clone(), db_name.to_string());
        let db_info = mt.get_database(get_req).await?;
        assert_eq!(db_info.database_id.db_id, db_id, "Database ID should match");
        assert!(
            db_info.meta.drop_on.is_none(),
            "Database should not be marked as dropped"
        );

        Ok(())
    }

    // pub async fn share_create_get_drop<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
    //     let tenant1 = "tenant1";
    //     let share_name1 = "share1";
    //     let share_name2 = "share2";
    //     info!("--- create {}", share_name1);
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name1.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create share res: {:?}", res);
    //         let res = res.unwrap();
    //         assert_eq!(1, res.share_id, "first share id is 1");
    //     }
    //
    //     info!("--- get share1");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, share_name1)).await;
    //         debug!("get present share res: {:?}", res);
    //         let res = res?;
    //         assert_eq!(1, res.id, "db1 id is 1");
    //         assert_eq!(
    //             share_name1.to_string(),
    //             res.name,
    //             "share1.db is {}",
    //             share_name1
    //         );
    //     }
    //
    //     info!("--- create share1 again with if_not_exists=false");
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name1.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create share res: {:?}", res);
    //         let err = res.unwrap_err();
    //         assert_eq!(
    //             ErrorCode::ShareAlreadyExists("").code(),
    //             ErrorCode::from(err).code()
    //         );
    //     }
    //
    //     info!("--- create share1 again with if_not_exists=true");
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: true,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name1.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create database res: {:?}", res);
    //
    //         let res = res.unwrap();
    //         assert_eq!(1, res.share_id, "share1 id is 1");
    //     }
    //
    //     info!("--- create share2");
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name2.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create share res: {:?}", res);
    //         let res = res.unwrap();
    //         assert_eq!(2, res.share_id, "second share id is 2 ");
    //     }
    //
    //     info!("--- get share2");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, share_name2)).await?;
    //         assert_eq!(2, res.id, "share2 id is 2");
    //         assert_eq!(
    //             share_name2.to_string(),
    //             res.name,
    //             "share2.name is {}",
    //             share_name2
    //         );
    //     }
    //
    //     info!("--- get absent share");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, "absent")).await;
    //         debug!("=== get absent share res: {:?}", res);
    //         assert!(res.is_err());
    //         let err = res.unwrap_err();
    //         let err_code = ErrorCode::from(err);
    //
    //         assert_eq!(ErrorCode::unknown_share_code(), err_code.code());
    //         assert!(err_code.message().contains("absent"));
    //     }
    //
    //     info!("--- drop share2");
    //     {
    //         mt.drop_share(DropShareReq {
    //             if_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name2.to_string(),
    //         })
    //         .await?;
    //     }
    //
    //     info!("--- get share2 should not found");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, share_name2)).await;
    //         let err = res.unwrap_err();
    //         assert_eq!(
    //             ErrorCode::UnknownShare("").code(),
    //             ErrorCode::from(err).code()
    //         );
    //     }
    //
    //     info!("--- drop share2 with if_exists=true returns no error");
    //     {
    //         mt.drop_share(DropShareReq {
    //             if_exists: true,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name2.to_string(),
    //         })
    //         .await?;
    //     }
    //
    //     Ok(())
    // }
    //
}

/// Supporting utils
impl SchemaApiTestSuite {
    fn req_get_db(tenant: impl ToTenant, db_name: impl ToString) -> GetDatabaseReq {
        GetDatabaseReq {
            inner: DatabaseNameIdent::new(tenant, db_name),
        }
    }
}

// Test write and read meta on different nodes
// This is meant for testing distributed SchemaApi impl, to ensure a read-after-write consistency.
impl SchemaApiTestSuite {
    /// Create db one node, get db on another
    pub async fn database_get_diff_nodes<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create db1 on node_a");
        let mut util = DbTableHarness::new(node_a, "tenant1", "db1", "", "github");
        util.create_db().await?;
        let tenant = util.tenant();
        assert_eq!(1, *util.db_id(), "first database id is 1");

        info!("--- get db1 on node_b");
        {
            let res = node_b
                .get_database(GetDatabaseReq::new(tenant.clone(), "db1"))
                .await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.database_id.db_id, "db1 id is 1");
            assert_eq!("db1", res.name_ident.database_name(), "db1.db is db1");
        }

        info!("--- get nonexistent-db on node_b, expect correct error");
        {
            let res = node_b
                .get_database(GetDatabaseReq::new(tenant.clone(), "nonexistent"))
                .await;
            debug!("get present database res: {:?}", res);
            let err = res.unwrap_err();
            let err = ErrorCode::from(err);
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, err.code());
            assert_eq!("Unknown database 'nonexistent'", err.message());
            assert_eq!(
                "UnknownDatabase. Code: 1003, Text = Unknown database 'nonexistent'.",
                err.to_string()
            );
        }

        Ok(())
    }

    /// Create dbs on node_a, list dbs on node_b
    pub async fn list_database_diff_nodes<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create db1 and db3 on node_a");
        let mut db_ids = vec![];

        // Create db1
        let mut util1 = DbTableHarness::new(node_a, "tenant1", "db1", "", "github");
        util1.create_db().await?;
        db_ids.push(util1.db_id());
        let tenant = util1.tenant();

        // Create db3
        let mut util3 = DbTableHarness::new(node_a, "tenant1", "db3", "", "github");
        util3.create_db().await?;
        db_ids.push(util3.db_id());

        info!("--- list databases from node_b");
        {
            let res = node_b
                .list_databases(ListDatabaseReq {
                    tenant: tenant.clone(),
                })
                .await;
            debug!("get database list: {:?}", res);
            let res = res?;
            assert_eq!(2, res.len(), "database list len is 2");
            assert_eq!(db_ids[0], res[0].database_id, "db1 id");
            assert_eq!("db1", res[0].name_ident.database_name(), "db1.name is db1");
            assert_eq!(db_ids[1], res[1].database_id, "db3 id");
            assert_eq!("db3", res[1].name_ident.database_name(), "db3.name is db3");
        }

        Ok(())
    }

    /// Create table on node_a, list table on node_b
    pub async fn list_table_diff_nodes<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create db1 and tb1, tb2 on node_a");
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;
        let db_name = "db1";

        let mut tb_ids = vec![];
        let db_id;

        {
            let mut util = DbTableHarness::new(node_a, tenant_name, db_name, "", "github");
            let res = util.create_db().await;
            info!("create database res: {:?}", res);
            assert!(res.is_ok());
            db_id = util.db_id();

            let tables = vec!["tb1", "tb2"];
            let schema = Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]));

            let options = maplit::btreemap! {"opt-1".into() => "val-1".into()};
            for tb in tables {
                let mut table_util = DbTableHarness::new(node_a, tenant_name, db_name, tb, "JSON");
                let old_db = table_util.get_database().await?;
                let (table_id, _) = table_util
                    .create_table_with(
                        |meta| TableMeta {
                            schema: schema.clone(),
                            options: options.clone(),
                            ..meta
                        },
                        |req| req,
                    )
                    .await?;
                let cur_db = table_util.get_database().await?;
                assert!(old_db.meta.seq < cur_db.meta.seq);
                tb_ids.push(table_id);
            }
        }

        info!("--- list tables from node_b");
        {
            let res = node_b.list_tables(ListTableReq::new(&tenant, db_id)).await;
            debug!("get table list: {:?}", res);
            let res = res?;
            assert_eq!(2, res.len(), "table list len is 2");
            assert_eq!(tb_ids[0], res[0].1.table_id, "tb1 id");
            assert_eq!("tb1", res[0].0, "tb1.name is tb1");
            assert_eq!(tb_ids[1], res[1].1.table_id, "tb2 id");
            assert_eq!("tb2", res[1].0, "tb2.name is tb2");
        }

        Ok(())
    }

    /// Create table on node_a, get table on node_b
    pub async fn table_get_diff_nodes<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create table tb1 on node_a");
        let mut util = DbTableHarness::new(node_a, "tenant1", "db1", "tb1", "github");
        util.create_db().await?;
        let tenant = util.tenant();

        let options = maplit::btreemap! {"opt‐1".into() => "val-1".into()};
        let tb_id = {
            let old_db = util.get_database().await?;
            let (table_id, _) = util
                .create_table_with(|meta| TableMeta { options, ..meta }, |req| req)
                .await?;
            let cur_db = util.get_database().await?;
            assert!(old_db.meta.seq < cur_db.meta.seq);
            table_id
        };

        info!("--- get tb1 on node_b");
        {
            let res = util.get_table().await;
            debug!("get present table res: {:?}", res);
            let res = res?;
            assert_eq!(tb_id, res.ident.table_id, "tb1 id is 1");
            assert_eq!("tb1", res.name, "tb1.name is tb1");
        }

        info!("--- get nonexistent-table on node_b, expect correct error");
        {
            let res = node_b
                .get_table(GetTableReq::new(&tenant, "db1", "nonexistent"))
                .await;
            debug!("get present table res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_TABLE, ErrorCode::from(err).code());
        }

        Ok(())
    }

    async fn update_table_with_copied_files<
        MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + TableApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";

        let db_name = "db1";
        let tbl_name = "tb2";
        let table_id;

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        let created_on = Utc::now();

        info!("--- prepare db and table");
        let tbl_name_ident = TableNameIdent {
            tenant: Tenant::new_or_err(tenant_name, func_name!())?,
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };
        {
            let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "JSON");
            util.create_db().await?;

            let (table_id_result, _) = util
                .create_table_with(|_meta| table_meta(created_on), |req| req)
                .await?;
            table_id = table_id_result;
        };

        info!("--- create and get stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                ttl: Some(std::time::Duration::from_secs(86400)),
                insert_if_not_exists: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
                base_snapshot_location: None,
                lvt_check: None,
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            mt.update_multi_table_meta(req).await?.unwrap();

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 1);
            let resp_stage_info = resp.file_info.get("file");
            assert_eq!(resp_stage_info.unwrap(), &stage_info);
        }

        info!("--- upsert table copied files with duplication, fail if duplicated");
        {
            // get previous file info
            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string()],
            };
            let resp = mt.get_table_copied_file_info(req).await?;
            let previous_file_info = resp.file_info.get("file");

            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());
            file_info.insert("file_not_exist".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                ttl: Some(std::time::Duration::from_secs(86400)),
                insert_if_not_exists: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
                base_snapshot_location: None,
                lvt_check: None,
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            let result = mt.update_multi_table_meta(req).await;
            let err = result.unwrap_err();
            let err = ErrorCode::from(err);
            assert_eq!(ErrorCode::DUPLICATED_UPSERT_FILES, err.code());

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string(), "file_not_exist".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            // "file" exist
            assert_eq!(resp.file_info.len(), 1);
            // "file" not modified
            let resp_stage_info = resp.file_info.get("file");
            assert_eq!(resp_stage_info, previous_file_info);
        }

        info!("--- upsert table copied files with duplication, duplicated checking disabled");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());
            file_info.insert("file_not_exist".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                ttl: Some(std::time::Duration::from_secs(86400)),
                insert_if_not_exists: false,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
                base_snapshot_location: None,
                lvt_check: None,
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident,
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            mt.update_multi_table_meta(req).await?.unwrap();

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string(), "file_not_exist".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 2);
            let resp_stage_info = resp.file_info.get("file");
            assert_eq!(resp_stage_info.unwrap(), &stage_info);
        }

        Ok(())
    }

    async fn dictionary_create_list_drop<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi + DictionaryApi {
        let tenant_name = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";
        let dict_name1 = "dict1";
        let dict_name2 = "dict2";
        let dict_name3 = "dict3";

        let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "eng1");
        let dict_tenant = util.tenant();
        let dict_id;

        info!("--- prepare db");
        {
            util.create_db().await?;
        }

        let db_name_ident =
            DatabaseNameIdent::new(Tenant::new_literal(tenant_name), db_name.to_string());
        let get_db_req = GetDatabaseReq {
            inner: db_name_ident.clone(),
        };
        let db_info = mt.get_database(get_db_req).await?;
        let db_id = db_info.database_id.db_id;

        let schema = || {
            Arc::new(TableSchema::new(vec![
                TableField::new("id", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("name", TableDataType::String),
            ]))
        };
        let options = || {
            maplit::btreemap! {
                "host".into() => "0.0.0.0".into(),
                "port".into() => "3306".into(),
                "username".into() => "root".into(),
                "password".into() => "1234".into(),
                "db".into() => "test".into()
            }
        };

        let dictionary_meta = |source: &str| DictionaryMeta {
            source: source.to_string(),
            schema: schema(),
            options: options(),
            created_on: Utc::now(),
            ..DictionaryMeta::default()
        };

        {
            info!("--- list dictionary with no create before");
            let req = ListDictionaryReq::new(dict_tenant.clone(), db_id);
            let res = mt.list_dictionaries(req).await?;
            assert!(res.is_empty());
        }

        let dict_ident1 = DictionaryNameIdent::new(
            dict_tenant.clone(),
            DictionaryIdentity::new(db_id, dict_name1.to_string()),
        );
        let dict_ident2 = DictionaryNameIdent::new(
            dict_tenant.clone(),
            DictionaryIdentity::new(db_id, dict_name2.to_string()),
        );
        let dict3 = DictionaryIdentity::new(db_id, dict_name3.to_string());
        let dict_ident3 = DictionaryNameIdent::new(dict_tenant.clone(), dict3.clone());

        {
            info!("--- create dictionary");
            let req = CreateDictionaryReq {
                dictionary_ident: dict_ident1.clone(),
                dictionary_meta: dictionary_meta("mysql"),
            };
            let res = mt.create_dictionary(req).await;
            assert!(res.is_ok());
            dict_id = res.unwrap().dictionary_id;
        }

        {
            info!("--- create dictionary again");
            let req = CreateDictionaryReq {
                dictionary_ident: dict_ident1.clone(),
                dictionary_meta: dictionary_meta("mysql"),
            };
            let res = mt.create_dictionary(req).await;
            assert!(res.is_err());
            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(ErrorCode::DICTIONARY_ALREADY_EXISTS, err_code.code());
        }

        {
            info!("--- prepare db2");
            let db_name = "db2";
            let mut util = DbTableHarness::new(mt, tenant_name, db_name, tbl_name, "eng1");
            {
                util.create_db().await?;
            }

            let db_id = util.db_id();

            {
                info!("--- list dictionary from db2 with no create before");
                let req = ListDictionaryReq::new(dict_tenant.clone(), *db_id);
                let res = mt.list_dictionaries(req).await?;
                assert!(res.is_empty());
            }
        }

        {
            info!("--- get dictionary");
            let req = dict_ident1.clone();
            let res = mt.get_dictionary(req).await?;
            assert!(res.is_some());
            let dict_reply = res.unwrap();
            assert_eq!(*dict_reply.0.data, dict_id);
            assert_eq!(dict_reply.1.data.source, "mysql".to_string());

            let req = dict_ident2.clone();
            let res = mt.get_dictionary(req).await?;
            assert!(res.is_none());
        }

        {
            info!("--- update dictionary");
            let req = UpdateDictionaryReq {
                dictionary_ident: dict_ident1.clone(),
                dictionary_meta: dictionary_meta("postgresql"),
            };
            let res = mt.update_dictionary(req).await;
            assert!(res.is_ok());

            let req = dict_ident1.clone();
            let res = mt.get_dictionary(req).await?;
            assert!(res.is_some());
            let dict_reply = res.unwrap();
            assert_eq!(*dict_reply.0.data, dict_id);
            assert_eq!(dict_reply.1.source, "postgresql".to_string());
        }

        {
            info!("--- update unknown dictionary");
            let req = UpdateDictionaryReq {
                dictionary_ident: dict_ident2.clone(),
                dictionary_meta: dictionary_meta("postgresql"),
            };
            let res = mt.update_dictionary(req).await;
            assert!(res.is_err());
            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(ErrorCode::UNKNOWN_DICTIONARY, err_code.code());
        }

        {
            info!("--- list dictionary");
            let req = ListDictionaryReq::new(dict_tenant.clone(), db_id);
            let res = mt.list_dictionaries(req).await?;

            assert_eq!(1, res.len());
        }

        {
            info!("--- rename dictionary");
            let rename_req = RenameDictionaryReq {
                name_ident: dict_ident1.clone(),
                new_dict_ident: dict3.clone(),
            };
            mt.rename_dictionary(rename_req).await?;
            let req = dict_ident1.clone();
            let res = mt.get_dictionary(req).await?;
            assert!(res.is_none());

            let req = dict_ident3.clone();
            let res = mt.get_dictionary(req).await?;
            assert!(res.is_some());

            let dict_reply = res.unwrap();
            assert_eq!(*dict_reply.0.data, dict_id);
            assert_eq!(dict_reply.1.source, "postgresql".to_string());

            info!("--- rename unknown dictionary");
            let rename_req = RenameDictionaryReq {
                name_ident: dict_ident1.clone(),
                new_dict_ident: dict3.clone(),
            };
            let res = mt.rename_dictionary(rename_req).await;
            assert!(res.is_err());
        }

        {
            info!("--- drop dictionary");
            let req = dict_ident3.clone();
            let res = mt.drop_dictionary(req).await?;
            assert!(res.is_some());
        }

        {
            info!("--- drop unknown dictionary");
            let req = dict_ident2.clone();
            let res = mt.drop_dictionary(req).await?;
            assert!(res.is_none());
        }

        {
            info!("--- list dictionary after drop one");
            let req = ListDictionaryReq::new(dict_tenant.clone(), db_id);
            let res = mt.list_dictionaries(req).await?;
            assert_eq!(0, res.len());
        }

        Ok(())
    }
}
