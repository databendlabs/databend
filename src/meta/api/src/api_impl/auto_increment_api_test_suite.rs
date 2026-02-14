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

use chrono::Utc;
use databend_common_expression::AutoIncrementExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::schema::AutoIncrementStorageIdent;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GetAutoIncrementNextValueReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::Key;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_types::MetaError;
use fastrace::func_name;
use log::info;

use crate::AutoIncrementApi;
use crate::DatabaseApi;
use crate::DatamaskApi;
use crate::GarbageCollectionApi;
use crate::RowAccessPolicyApi;
use crate::SchemaApi;
use crate::TableApi;
use crate::kv_pb_api::KVPbApi;

/// Test suite of `AutoIncrementApi`.
///
/// It is not used by this crate, but is used by other crate that impl `AutoIncrementApi`,
/// to ensure an impl works as expected,
/// such as `meta/embedded` and `metasrv`.
#[derive(Copy, Clone)]
pub struct AutoIncrementApiTestSuite {}

impl AutoIncrementApiTestSuite {
    /// Test AutoIncrementApi on a single node
    pub async fn test_single_node<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: kvapi::KVApi<Error = MetaError>
            + SchemaApi
            + DatamaskApi
            + AutoIncrementApi
            + RowAccessPolicyApi
            + 'static,
    {
        let suite = AutoIncrementApiTestSuite {};
        suite.table_commit_table_meta(&b.build().await).await?;
        Ok(())
    }

    #[fastrace::trace]
    async fn table_commit_table_meta<MT: AutoIncrementApi + kvapi::KVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "table_commit_table_meta_tenant";
        let db_name = "db1";
        let tbl_name = "tb2";

        info!("--- prepare db");
        let mut util = Util::new(mt, tenant_name, db_name, "");
        util.create_db().await?;

        let schema = || {
            Arc::new(TableSchema::new(vec![
                TableField::new("number", TableDataType::Number(NumberDataType::UInt64))
                    .with_auto_increment_expr(Some(AutoIncrementExpr {
                        column_id: 0,
                        start: 0,
                        step: 1,
                        is_ordered: true,
                    })),
            ]))
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

        // verify the auto increment will be vacuum
        {
            // use a new tenant and db do test
            let db_name = "orphan_db";
            let tenant_name = "orphan_tenant";
            let mut orphan_util = Util::new(mt, tenant_name, db_name, "");
            orphan_util.create_db().await?;
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

            let auto_increment_key =
                AutoIncrementKey::new(create_table_as_dropped_resp.table_id, 0);
            let auto_increment_sequence_storage =
                AutoIncrementStorageIdent::new_generic(&tenant, auto_increment_key.clone());

            // assert auto increment exists
            let seqv = mt
                .get_kv(&auto_increment_sequence_storage.to_string_key())
                .await?;
            assert!(seqv.is_some() && seqv.unwrap().seq != 0);

            // auto increment next val
            let expr = AutoIncrementExpr {
                column_id: 0,
                start: 0,
                step: 1,
                is_ordered: true,
            };
            let next_val_req = GetAutoIncrementNextValueReq {
                tenant: tenant.clone(),
                expr,
                key: auto_increment_key,
                count: 1,
            };
            mt.get_auto_increment_next_value(next_val_req)
                .await?
                .unwrap();

            // assert auto increment current is 1 after next val
            let seqv = mt.get_pb(&auto_increment_sequence_storage).await?;
            assert!(seqv.as_ref().unwrap().seq != 0);
            assert_eq!(seqv.as_ref().unwrap().data.inner().0, 1);

            // assert auto increment exists
            let seqv = mt
                .get_kv(&auto_increment_sequence_storage.to_string_key())
                .await?;
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

            // assert auto increment has been vacuum
            let seqv = mt
                .get_kv(&auto_increment_sequence_storage.to_string_key())
                .await?;
            assert!(seqv.is_none());
        }

        Ok(())
    }
}

struct Util<'a, MT>
// where MT: AutoIncrementApi
where MT: kvapi::KVApi<Error = MetaError> + AutoIncrementApi
{
    tenant: Tenant,
    db_name: String,
    engine: String,
    db_id: u64,
    mt: &'a MT,
}

impl<'a, MT> Util<'a, MT>
where MT: AutoIncrementApi + kvapi::KVApi<Error = MetaError>
{
    fn new(
        mt: &'a MT,
        tenant: impl ToString,
        db_name: impl ToString,
        engine: impl ToString,
    ) -> Self {
        Self {
            tenant: Tenant::new_or_err(tenant, func_name!()).unwrap(),
            db_name: db_name.to_string(),
            engine: engine.to_string(),
            db_id: 0,
            mt,
        }
    }

    fn tenant(&self) -> Tenant {
        self.tenant.clone()
    }

    fn engine(&self) -> String {
        self.engine.clone()
    }

    fn db_name(&self) -> String {
        self.db_name.clone()
    }

    async fn create_db(&mut self) -> anyhow::Result<()> {
        let plan = CreateDatabaseReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident: DatabaseNameIdent::new(self.tenant(), self.db_name()),
            meta: DatabaseMeta {
                engine: self.engine(),
                ..DatabaseMeta::default()
            },
        };

        let res = self.mt.create_database(plan).await?;
        self.db_id = *res.db_id;

        Ok(())
    }
}
