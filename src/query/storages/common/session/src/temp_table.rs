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
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use parking_lot::Mutex;

/// `TempTblId` is an unique identifier for a temporary table.
///
/// It should **not** be used as `MetaId`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Copy)]
struct TempTblId {
    inner: u64,
}

impl TempTblId {
    fn zero() -> Self {
        TempTblId { inner: 0 }
    }

    fn increment(&mut self) {
        self.inner += 1;
    }

    fn new(inner: u64) -> Self {
        Self { inner }
    }

    fn get_inner(&self) -> u64 {
        self.inner
    }
}

#[derive(Debug, Clone)]
pub struct TempTblMgr {
    name_to_id: HashMap<TableNameIdent, TempTblId>,
    id_to_name: HashMap<TempTblId, TableNameIdent>,
    id_to_meta: HashMap<TempTblId, TableMeta>,
    next_id: TempTblId,
}

impl TempTblMgr {
    pub fn init() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(TempTblMgr {
            name_to_id: HashMap::new(),
            id_to_name: HashMap::new(),
            id_to_meta: HashMap::new(),
            next_id: TempTblId::zero(),
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
        let new_table = match (self.name_to_id.entry(name_ident.clone()), create_option) {
            (Entry::Occupied(_), CreateOption::Create) => {
                return Err(ErrorCode::TableAlreadyExists(format!(
                    "Temporary table {} already exists",
                    name_ident
                )));
            }
            (Entry::Occupied(mut e), CreateOption::CreateOrReplace) => {
                let table_id = self.next_id;
                e.insert(table_id);
                self.id_to_name.insert(table_id, name_ident.clone());
                self.id_to_meta.insert(table_id, table_meta);
                self.next_id.increment();
                true
            }
            (Entry::Occupied(_), CreateOption::CreateIfNotExists) => false,
            (Entry::Vacant(e), _) => {
                let table_id = self.next_id;
                e.insert(table_id);
                self.id_to_name.insert(table_id, name_ident.clone());
                self.id_to_meta.insert(table_id, table_meta);
                self.next_id.increment();
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
        let orhan_name_ident = TableNameIdent {
            table_name: req.orphan_table_name.clone().unwrap(),
            ..req.name_ident.clone()
        };
        match self.name_to_id.remove(&orhan_name_ident) {
            Some(id) => {
                self.name_to_id.insert(req.name_ident.clone(), id);
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
        match self.name_to_id.remove(name_ident) {
            Some(id) => {
                let new_name_ident = TableNameIdent {
                    table_name: new_table_name.clone(),
                    db_name: new_db_name.clone(),
                    ..name_ident.clone()
                };
                self.name_to_id.insert(new_name_ident, id);
                Ok(Some(RenameTableReply {
                    table_id: 0,
                    share_table_info: None,
                }))
            }
            None => Ok(None),
        }
    }

    pub fn get_table_meta_by_id(&self, id: u64) -> Option<TableMeta> {
        self.id_to_meta.get(&TempTblId::new(id)).cloned()
    }

    pub fn get_table_name_by_id(&self, id: u64) -> Option<String> {
        self.id_to_name
            .get(&TempTblId::new(id))
            .map(|i| i.table_name.clone())
    }

    pub fn is_temp_table(&self, tenant: Tenant, database_name: &str, table_name: &str) -> bool {
        self.name_to_id.contains_key(&TableNameIdent {
            table_name: table_name.to_string(),
            db_name: database_name.to_string(),
            tenant: tenant.clone(),
        })
    }

    pub fn get_table(
        &self,
        tenant: &Tenant,
        database_name: &str,
        table_name: &str,
    ) -> Result<Option<TableInfo>> {
        let id = self.name_to_id.get(&TableNameIdent {
            table_name: table_name.to_string(),
            db_name: database_name.to_string(),
            tenant: tenant.clone(),
        });
        let Some(id) = id else {
            return Ok(None);
        };
        let Some(meta) = self.id_to_meta.get(id) else {
            return Err(ErrorCode::Internal(format!(
                "Got table id {:?} but not found meta in temp table manager {:?}",
                id, self
            )));
        };
        let ident = TableIdent {
            table_id: id.get_inner(),
            ..Default::default()
        };
        let table_info = TableInfo::new(database_name, table_name, ident, meta.clone());
        Ok(Some(table_info))
    }

    pub fn drop_table_by_id(&mut self, req: DropTableByIdReq) -> Result<DropTableReply> {
        todo!()
    }
}

pub type TempTblMgrRef = Arc<Mutex<TempTblMgr>>;
