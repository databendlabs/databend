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

use chrono::Utc;
use common_meta_app::schema::CreateTableLockRevReq;
use common_meta_app::schema::DeleteTableLockRevReq;
use common_meta_app::schema::ExtendTableLockRevReq;
use common_meta_app::schema::ListTableLockRevReq;
use common_pipeline_core::table_lock::TableLockReq;

#[derive(Clone)]
pub struct ListTableLockReq {
    pub table_id: u64,
}

impl TableLockReq for ListTableLockReq {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn TableLockReq> {
        Box::new(self.clone())
    }
}

impl From<&ListTableLockReq> for ListTableLockRevReq {
    fn from(value: &ListTableLockReq) -> Self {
        ListTableLockRevReq {
            table_id: value.table_id,
        }
    }
}

#[derive(Clone)]
pub struct CreateTableLockReq {
    pub table_id: u64,
    pub expire_secs: u64,
}

impl TableLockReq for CreateTableLockReq {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn TableLockReq> {
        Box::new(self.clone())
    }
}

impl From<&CreateTableLockReq> for CreateTableLockRevReq {
    fn from(value: &CreateTableLockReq) -> Self {
        CreateTableLockRevReq {
            table_id: value.table_id,
            expire_at: Utc::now().timestamp() as u64 + value.expire_secs,
        }
    }
}

#[derive(Clone)]
pub struct ExtendTableLockReq {
    pub table_id: u64,
    pub expire_secs: u64,
    pub revision: u64,
}

impl TableLockReq for ExtendTableLockReq {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn TableLockReq> {
        Box::new(self.clone())
    }
}

impl From<&ExtendTableLockReq> for ExtendTableLockRevReq {
    fn from(value: &ExtendTableLockReq) -> Self {
        ExtendTableLockRevReq {
            table_id: value.table_id,
            expire_at: Utc::now().timestamp() as u64 + value.expire_secs,
            revision: value.revision,
        }
    }
}

#[derive(Clone)]
pub struct DeleteTableLockReq {
    pub table_id: u64,
    pub revision: u64,
}

impl TableLockReq for DeleteTableLockReq {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_self(&self) -> Box<dyn TableLockReq> {
        Box::new(self.clone())
    }
}

impl From<&DeleteTableLockReq> for DeleteTableLockRevReq {
    fn from(value: &DeleteTableLockReq) -> Self {
        DeleteTableLockRevReq {
            table_id: value.table_id,
            revision: value.revision,
        }
    }
}
