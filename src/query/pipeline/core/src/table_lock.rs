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

use std::any::Any;
use std::fmt::Display;
use std::fmt::Formatter;

pub trait TableLock: Sync + Send {
    fn level(&self) -> TableLockLevel;

    fn table_id(&self) -> u64;

    fn set_revision(&mut self, revision: u64);

    fn revision(&self) -> u64;

    fn watch_delete_key(&self, revision: u64) -> String;

    fn create_table_lock_req(&self, expire_secs: u64) -> Box<dyn TableLockReq>;

    fn extend_table_lock_req(&self, expire_secs: u64) -> Box<dyn TableLockReq>;

    fn delete_table_lock_req(&self) -> Box<dyn TableLockReq>;

    fn list_table_lock_req(&self) -> Box<dyn TableLockReq>;
}

pub enum TableLockLevel {
    Table,
}

impl Display for TableLockLevel {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TableLockLevel::Table => write!(f, "Table"),
        }
    }
}

pub trait TableLockReq: Send + Sync {
    fn as_any(&self) -> &dyn Any;

    fn clone_self(&self) -> Box<dyn TableLockReq>;
}

impl Clone for Box<dyn TableLockReq> {
    fn clone(&self) -> Self {
        self.clone_self()
    }
}
