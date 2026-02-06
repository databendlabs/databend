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

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_api::DatabaseApi;
use databend_common_meta_api::TableApi;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::ListTableReq;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::table_niv::TableNIV;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_kvapi::kvapi;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaError;
use fastrace::func_name;

pub(crate) struct DbTableHarness<'a, MT>
where MT: kvapi::KVApi<Error = MetaError>
{
    tenant: Tenant,
    db_name: String,
    table_name: String,
    engine: String,
    created_on: DateTime<Utc>,
    db_id: u64,
    table_id: u64,
    mt: &'a MT,
}

impl<'a, MT> DbTableHarness<'a, MT>
where MT: kvapi::KVApi<Error = MetaError>
{
    pub(crate) fn new(
        mt: &'a MT,
        tenant: impl ToString,
        db_name: impl ToString,
        tbl_name: impl ToString,
        engine: impl ToString,
    ) -> Self {
        Self {
            tenant: Tenant::new_or_err(tenant, func_name!()).unwrap(),
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
            engine: engine.to_string(),
            created_on: Utc::now(),
            db_id: 0,
            table_id: 0,
            mt,
        }
    }

    pub(crate) fn tenant(&self) -> Tenant {
        self.tenant.clone()
    }

    pub(crate) fn db_name(&self) -> String {
        self.db_name.clone()
    }

    pub(crate) fn db_id(&self) -> DatabaseId {
        DatabaseId::new(self.db_id)
    }

    pub(crate) fn tbl_name(&self) -> String {
        self.table_name.clone()
    }

    pub(crate) fn engine(&self) -> String {
        self.engine.clone()
    }

    pub(crate) fn schema(&self) -> Arc<TableSchema> {
        Arc::new(TableSchema::new(vec![
            TableField::new("number", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("variant", TableDataType::Variant),
        ]))
    }

    pub(crate) fn options(&self) -> BTreeMap<String, String> {
        maplit::btreemap! {"opt‐1".into() => "val-1".into()}
    }

    pub(crate) fn table_meta(&self) -> TableMeta {
        TableMeta {
            schema: self.schema(),
            engine: self.engine(),
            options: self.options(),
            created_on: self.created_on,
            ..TableMeta::default()
        }
    }

    pub(crate) fn meta_api(&self) -> &MT {
        self.mt
    }
}

impl<'a, MT> DbTableHarness<'a, MT>
where MT: kvapi::KVApi<Error = MetaError> + DatabaseApi
{
    pub(crate) async fn create_db(&mut self) -> anyhow::Result<()> {
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

    #[allow(dead_code)]
    pub(crate) async fn drop_db(&self) -> anyhow::Result<()> {
        let req = DropDatabaseReq {
            if_exists: false,
            name_ident: DatabaseNameIdent::new(self.tenant(), self.db_name()),
        };

        self.mt.drop_database(req).await?;
        Ok(())
    }

    pub(crate) async fn get_database(
        &self,
    ) -> anyhow::Result<std::sync::Arc<databend_common_meta_app::schema::DatabaseInfo>> {
        let req = GetDatabaseReq::new(self.tenant(), self.db_name());
        let res = self.mt.get_database(req).await?;
        Ok(res)
    }
}

impl<'a, MT> DbTableHarness<'a, MT>
where MT: kvapi::KVApi<Error = MetaError> + TableApi
{
    /// Create table but let user customize the table meta
    pub(crate) async fn create_table_with(
        &mut self,
        f: impl FnOnce(TableMeta) -> TableMeta,
        r: impl FnOnce(CreateTableReq) -> CreateTableReq,
    ) -> anyhow::Result<(u64, TableMeta)> {
        let table_meta = self.table_meta();

        let table_meta = f(table_meta);

        let req = CreateTableReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident: TableNameIdent {
                tenant: self.tenant(),
                db_name: self.db_name(),
                table_name: self.tbl_name(),
            },
            table_meta: table_meta.clone(),
            as_dropped: false,
            table_properties: None,
            table_partition: None,
        };

        let req = r(req);

        let resp = self.mt.create_table(req.clone()).await?;
        let table_id = resp.table_id;

        self.table_id = table_id;

        Ok((table_id, table_meta))
    }

    pub(crate) async fn create_table(&mut self) -> anyhow::Result<(u64, TableMeta)> {
        let table_meta = self.table_meta();
        let req = CreateTableReq {
            create_option: CreateOption::Create,
            catalog_name: None,
            name_ident: TableNameIdent {
                tenant: self.tenant(),
                db_name: self.db_name(),
                table_name: self.tbl_name(),
            },
            table_meta: table_meta.clone(),
            as_dropped: false,
            table_properties: None,
            table_partition: None,
        };
        let resp = self.mt.create_table(req.clone()).await?;
        let table_id = resp.table_id;

        self.table_id = table_id;

        Ok((table_id, table_meta))
    }

    pub(crate) async fn drop_table_by_id(&mut self) -> anyhow::Result<()> {
        let req = DropTableByIdReq {
            tenant: self.tenant().clone(),
            table_name: self.tbl_name(),
            if_exists: false,
            db_id: self.db_id,
            tb_id: self.table_id,
            db_name: "".to_string(),
            engine: "FUSE".to_string(),
            temp_prefix: "".to_string(),
        };
        self.mt.drop_table_by_id(req.clone()).await?;

        Ok(())
    }

    pub(crate) async fn update_copied_files(
        &self,
        n: usize,
    ) -> anyhow::Result<BTreeMap<String, TableCopiedFileInfo>> {
        let mut file_infos = BTreeMap::new();

        for i in 0..n {
            let stage_info = TableCopiedFileInfo {
                etag: Some(format!("etag{}", i)),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            file_infos.insert(format!("file{}", i), stage_info);
        }

        let copied_file_req = UpsertTableCopiedFileReq {
            file_info: file_infos.clone(),
            ttl: Some(std::time::Duration::from_secs(86400)),
            insert_if_not_exists: true,
        };

        let req = UpdateTableMetaReq {
            table_id: self.table_id,
            seq: MatchSeq::Any,
            new_table_meta: self.table_meta(),
            base_snapshot_location: None,
            lvt_check: None,
        };

        let req = UpdateMultiTableMetaReq {
            update_table_metas: vec![(req, Default::default())],
            copied_files: vec![(self.table_id, copied_file_req)],
            ..Default::default()
        };

        self.mt.update_multi_table_meta(req).await?.unwrap();

        Ok(file_infos)
    }

    pub(crate) async fn get_table(
        &self,
    ) -> anyhow::Result<std::sync::Arc<databend_common_meta_app::schema::TableInfo>> {
        let req = GetTableReq::new(&self.tenant(), self.db_name(), self.tbl_name());
        let res = self.mt.get_table(req).await?;
        Ok(res)
    }

    pub(crate) async fn get_table_by_name(
        &self,
        table_name: &str,
    ) -> anyhow::Result<std::sync::Arc<databend_common_meta_app::schema::TableInfo>> {
        let req = GetTableReq::new(&self.tenant(), self.db_name(), table_name);
        let res = self.mt.get_table(req).await?;
        Ok(res)
    }

    pub(crate) async fn list_history_tables(
        &self,
        include_dropped: bool,
    ) -> anyhow::Result<Vec<TableNIV>> {
        let req = ListTableReq::new(&self.tenant(), self.db_id());
        let res = self.mt.list_history_tables(include_dropped, req).await?;
        Ok(res)
    }
}
