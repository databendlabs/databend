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
use std::sync::Mutex;

use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateTableMetaReq;

#[derive(Debug, Clone)]
pub struct TxnManager {
    state: TxnState,
    txn_buffer: TxnBuffer,
}

pub type TxnManagerRef = Arc<Mutex<TxnManager>>;

#[derive(Clone, Debug)]
pub enum TxnState {
    AutoCommit,
    Active,
    Fail,
}

#[derive(Debug, Clone)]
struct TxnBuffer {
    mutated_tables: HashMap<String, (UpdateTableMetaReq, TableInfo)>,
    stream_tables: HashMap<String, StreamSnapshot>,
}

#[derive(Debug, Clone)]
struct StreamSnapshot {
    pub stream: TableInfo,
    pub source: TableInfo,
}

impl TxnBuffer {
    fn new() -> Self {
        Self {
            mutated_tables: HashMap::new(),
            stream_tables: HashMap::new(),
        }
    }
    fn clear(&mut self) {
        self.mutated_tables.clear();
        self.stream_tables.clear();
    }
}

impl TxnManager {
    pub fn init() -> Self {
        TxnManager {
            state: TxnState::AutoCommit,
            txn_buffer: TxnBuffer::new(),
        }
    }

    pub fn begin(&mut self) {
        if let TxnState::AutoCommit = self.state {
            self.state = TxnState::Active
        }
    }

    pub fn clear(&mut self) {
        self.state = TxnState::AutoCommit;
        self.txn_buffer.clear();
    }

    pub fn set_fail(&mut self) {
        if let TxnState::Active = self.state {
            self.state = TxnState::Fail;
        }
    }

    pub fn is_fail(&self) -> bool {
        matches!(self.state, TxnState::Fail)
    }

    pub fn is_active(&self) -> bool {
        matches!(self.state, TxnState::Active)
    }

    pub fn state(&self) -> TxnState {
        self.state.clone()
    }

    pub fn add_mutated_table(&mut self, req: UpdateTableMetaReq, table_info: &TableInfo) {
        self.txn_buffer
            .mutated_tables
            .insert(table_info.desc.clone(), (req, table_info.clone()));
    }

    pub fn add_stream_table(&mut self, stream: TableInfo, source: TableInfo) {
        self.txn_buffer
            .stream_tables
            .insert(stream.desc.clone(), StreamSnapshot { stream, source });
    }

    pub fn get_stream_table_source(&self, stream_desc: &str) -> Option<TableInfo> {
        self.txn_buffer
            .stream_tables
            .get(stream_desc)
            .map(|snapshot| snapshot.source.clone())
    }

    pub fn get_table_from_buffer(
        &self,
        _tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Option<TableInfo> {
        let desc = format!("'{}'.'{}'", db_name, table_name);
        self.txn_buffer
            .mutated_tables
            .get(&desc)
            .map(|(req, table_info)| TableInfo {
                meta: req.new_table_meta.clone(),
                ..table_info.clone()
            })
            .or_else(|| {
                self.txn_buffer
                    .stream_tables
                    .get(&desc)
                    .cloned()
                    .map(|snapshot| snapshot.stream)
            })
    }

    pub fn get_table_from_buffer_by_id(&self, table_id: u64) -> Option<TableInfo> {
        self.txn_buffer
            .mutated_tables
            .values()
            .find(|(req, _)| req.table_id == table_id)
            .map(|(req, table_info)| TableInfo {
                meta: req.new_table_meta.clone(),
                ..table_info.clone()
            })
            .or_else(|| {
                self.txn_buffer
                    .stream_tables
                    .values()
                    .find(|ss| ss.stream.ident.table_id == table_id)
                    .cloned()
                    .map(|snapshot| snapshot.stream)
            })
    }

    pub fn reqs(&self) -> Vec<UpdateTableMetaReq> {
        self.txn_buffer
            .mutated_tables
            .values()
            .map(|(req, _)| req.clone())
            .collect()
    }
}
