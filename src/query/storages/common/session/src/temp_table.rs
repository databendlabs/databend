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

use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateTempTableReq;
use databend_common_storage::DataOperator;
use databend_storages_common_table_meta::meta::parse_storage_prefix;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct TempTblMgr {
    desc_to_id: HashMap<String, u64>,
    id_to_table: HashMap<u64, TempTable>,
    next_id: u64,
}

#[derive(Debug, Clone)]
pub struct TempTable {
    pub db_name: String,
    pub table_name: String,
    pub meta: TableMeta,
    pub _copied_files: BTreeMap<String, TableCopiedFileInfo>,
}

impl TempTblMgr {
    pub fn init() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(TempTblMgr {
            desc_to_id: HashMap::new(),
            id_to_table: HashMap::new(),
            next_id: 0,
        }))
    }

    pub fn create_table(&mut self, req: CreateTableReq) -> Result<CreateTableReply> {
        let CreateTableReq {
            create_option,
            mut name_ident,
            table_meta,
            as_dropped,
        } = req;
        let orphan_table_name = match as_dropped {
            true => {
                name_ident.table_name = format!("orphan@{}", name_ident.table_name);
                Some(name_ident.table_name.clone())
            }
            false => None,
        };
        let Some(db_id) = table_meta.options.get(OPT_KEY_DATABASE_ID) else {
            return Err(ErrorCode::Internal(format!(
                "Database id not set in table options"
            )));
        };
        let db_id = db_id.parse::<u64>()?;
        let desc = format!("{}.{}", name_ident.db_name, name_ident.table_name);
        let new_table = match (self.desc_to_id.entry(desc.clone()), create_option) {
            (Entry::Occupied(_), CreateOption::Create) => {
                return Err(ErrorCode::TableAlreadyExists(format!(
                    "Temporary table {} already exists",
                    desc
                )));
            }
            (Entry::Occupied(mut e), CreateOption::CreateOrReplace) => {
                let table_id = self.next_id;
                e.insert(table_id);
                self.id_to_table.insert(table_id, TempTable {
                    db_name: name_ident.db_name,
                    table_name: name_ident.table_name,
                    meta: table_meta,
                    _copied_files: BTreeMap::new(),
                });
                self.next_id += 1;
                true
            }
            (Entry::Occupied(_), CreateOption::CreateIfNotExists) => false,
            (Entry::Vacant(e), _) => {
                let table_id = self.next_id;
                e.insert(table_id);
                self.id_to_table.insert(table_id, TempTable {
                    db_name: name_ident.db_name,
                    table_name: name_ident.table_name,
                    meta: table_meta,
                    _copied_files: BTreeMap::new(),
                });
                self.next_id += 1;
                true
            }
        };
        Ok(CreateTableReply {
            table_id: 0,
            table_id_seq: Some(0),
            db_id,
            new_table,
            spec_vec: None,
            prev_table_id: None,
            orphan_table_name,
        })
    }

    pub fn commit_table_meta(
        &mut self,
        req: &CommitTableMetaReq,
    ) -> Result<Option<CommitTableMetaReply>> {
        let orphan_desc = format!(
            "{}.{}",
            req.name_ident.db_name,
            req.orphan_table_name.as_ref().unwrap()
        );
        let desc = format!("{}.{}", req.name_ident.db_name, req.name_ident.table_name);
        match self.desc_to_id.remove(&orphan_desc) {
            Some(id) => {
                self.desc_to_id.insert(desc, id);
                Ok(Some(CommitTableMetaReply {}))
            }
            None => Ok(None),
        }
    }

    pub fn rename_table(&mut self, req: &RenameTableReq) -> Result<Option<RenameTableReply>> {
        let RenameTableReq {
            if_exists: _,
            name_ident,
            new_db_name,
            new_table_name,
        } = req;
        let desc = format!("{}.{}", name_ident.db_name, name_ident.table_name);
        match self.desc_to_id.remove(&desc) {
            Some(id) => {
                let new_desc = format!("{}.{}", new_db_name, new_table_name);
                self.desc_to_id.insert(new_desc, id);
                let table = self.id_to_table.get_mut(&id).unwrap();
                table.db_name = new_db_name.clone();
                table.table_name = new_table_name.clone();
                Ok(Some(RenameTableReply {
                    table_id: 0,
                    share_table_info: None,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn get_table_meta_by_id(&self, id: u64) -> Option<TableMeta> {
        self.id_to_table.get(&id).map(|t| t.meta.clone())
    }

    pub fn get_table_name_by_id(&self, id: u64) -> Option<String> {
        self.id_to_table.get(&id).map(|t| t.table_name.clone())
    }

    pub fn is_temp_table(&self, database_name: &str, table_name: &str) -> bool {
        let desc = format!("{}.{}", database_name, table_name);
        self.desc_to_id.contains_key(&desc)
    }

    pub fn get_table(&self, database_name: &str, table_name: &str) -> Result<Option<TableInfo>> {
        let desc = format!("{}.{}", database_name, table_name);
        let id = self.desc_to_id.get(&desc);
        let Some(id) = id else {
            return Ok(None);
        };
        let Some(table) = self.id_to_table.get(id) else {
            return Err(ErrorCode::Internal(format!(
                "Got table id {:?} but not found meta in temp table manager {:?}",
                id, self
            )));
        };
        let ident = TableIdent {
            table_id: *id,
            ..Default::default()
        };
        let table_info = TableInfo::new(database_name, table_name, ident, table.meta.clone());
        Ok(Some(table_info))
    }

    pub fn update_multi_table_meta(&mut self, req: Vec<UpdateTempTableReq>) {
        for r in req {
            let UpdateTempTableReq {
                table_id,
                new_table_meta,
                copied_files,
                ..
            } = r;
            let table = self.id_to_table.get_mut(&table_id).unwrap();
            table.meta = new_table_meta;
            table._copied_files = copied_files;
        }
    }
}

pub async fn drop_table_by_id(mgr: TempTblMgrRef, req: DropTableByIdReq) -> Result<DropTableReply> {
    let DropTableByIdReq {
        if_exists, tb_id, ..
    } = req;
    let dir = {
        let mut guard = mgr.lock();
        let entry = guard.id_to_table.entry(tb_id);
        match (entry, if_exists) {
            (Entry::Occupied(e), _) => {
                let dir = parse_storage_prefix(&e.get().meta.options, tb_id)?;
                let table = e.remove();
                let desc = format!("{}.{}", table.db_name, table.table_name);
                guard.desc_to_id.remove(&desc).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Table not found in temp table manager {:?}, drop table request: {:?}",
                        guard, req
                    ))
                })?;
                dir
            }
            (Entry::Vacant(_), true) => {
                return Err(ErrorCode::UnknownTable(format!(
                    "Table not found in temp table manager {:?}, drop table request: {:?}",
                    *guard, req
                )));
            }
            (Entry::Vacant(_), false) => {
                return Ok(Default::default());
            }
        }
    };
    let op = DataOperator::instance().operator();
    op.remove_all(&dir).await?;
    Ok(Default::default())
}

pub type TempTblMgrRef = Arc<Mutex<TempTblMgr>>;
