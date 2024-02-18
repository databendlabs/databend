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

use databend_common_meta_app::schema::UpdateTableMetaReq;

pub struct TxnManager {
    state: TxnState,
    txn_buffer: TxnBuffer,
}

pub type TxnManagerRef = Arc<Mutex<TxnManager>>;

#[derive(Clone)]
pub enum TxnState {
    AutoCommit,
    Active,
    Fail,
}

struct TxnBuffer {
    table_metas: HashMap<u64, UpdateTableMetaReq>,
}

impl TxnBuffer {
    fn new() -> Self {
        Self {
            table_metas: HashMap::new(),
        }
    }
    fn refresh(&mut self) {
        self.table_metas.clear();
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

    pub fn commit(&mut self) {
        self.state = TxnState::AutoCommit;
        if let TxnState::Active = self.state {
            todo!("commit")
        }
        self.txn_buffer.refresh();
    }

    pub fn abort(&mut self) {
        self.state = TxnState::AutoCommit;
        self.txn_buffer.refresh();
    }

    pub fn set_fail(&mut self) {
        if let TxnState::Active = self.state {
            self.state = TxnState::Fail;
        }
    }

    pub fn is_fail(&self) -> bool {
        matches!(self.state, TxnState::Fail)
    }

    pub fn state(&self) -> TxnState {
        self.state.clone()
    }

    pub fn add_table_meta(&mut self, table_meta: UpdateTableMetaReq) {
        self.txn_buffer
            .table_metas
            .insert(table_meta.table_id, table_meta);
    }
}
