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

use databend_common_meta_app::storage::S3StorageClass;

use crate::TempTblMgr;
use crate::TempTblMgrRef;
use crate::TxnManager;
use crate::TxnManagerRef;

#[derive(Clone, Debug)]
pub struct SessionState {
    /// Transaction manager for the session
    pub txn_mgr: TxnManagerRef,
    /// Temporary table manager for the session
    pub temp_tbl_mgr: TempTblMgrRef,
    /// S3 storage class configuration for Fuse tables (including external fuse tables)
    /// This setting applies to all table operations within this session that use S3 storage.
    /// Note: Only effective for AWS S3 and compatible storage that supports S3 storage classes.
    pub s3_storage_class: S3StorageClass,
}

impl Default for SessionState {
    fn default() -> Self {
        SessionState {
            txn_mgr: TxnManager::init(),
            temp_tbl_mgr: TempTblMgr::init(),
            s3_storage_class: S3StorageClass::default(),
        }
    }
}
