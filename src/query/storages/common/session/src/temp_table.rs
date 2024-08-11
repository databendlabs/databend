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
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use parking_lot::Mutex;
#[derive(Debug, Clone)]
pub struct TempTblMgr {
    name_to_id: HashMap<TableNameIdent, u64>,
    id_to_meta: HashMap<u64, TableMeta>,
    next_id: u64,
}

impl TempTblMgr {
    pub fn init() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(TempTblMgr {
            name_to_id: HashMap::new(),
            id_to_meta: HashMap::new(),
            next_id: 0,
        }))
    }

    pub fn create_table(&mut self, req: CreateTableReq) -> Result<CreateTableReply> {
        let CreateTableReq {
            create_option,
            name_ident,
            table_meta,
            as_dropped: _,
        } = req;
        let Some(db_id) = table_meta.options.get(OPT_KEY_DATABASE_ID) else {
            return Err(ErrorCode::Internal(format!(
                "Database id not set in table options"
            )));
        };
        let db_id = db_id.parse::<u64>()?;
        let table_id = match (self.name_to_id.entry(name_ident.clone()), create_option) {
            (Entry::Occupied(_), CreateOption::Create) => {
                return Err(ErrorCode::TableAlreadyExists(format!(
                    "Temporary table {} already exists",
                    name_ident
                )));
            }
            (Entry::Occupied(mut e), CreateOption::CreateOrReplace) => {
                let table_id = self.next_id;
                e.insert(table_id);
                self.id_to_meta.insert(table_id, table_meta);
                self.next_id += 1;
                table_id
            }
            (Entry::Occupied(e), CreateOption::CreateIfNotExists) => {
                let table_id = *e.get();
                table_id
            }
            (Entry::Vacant(e), _) => {
                let table_id = self.next_id;
                e.insert(table_id);
                self.id_to_meta.insert(table_id, table_meta);
                self.next_id += 1;
                table_id
            }
        };
        Ok(CreateTableReply {
            table_id,
            table_id_seq: None,
            db_id,
            new_table: true,
            spec_vec: None,
            prev_table_id: None,
            orphan_table_name: None,
        })
    }
}

pub type TempTblMgrRef = Arc<Mutex<TempTblMgr>>;
