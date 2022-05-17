// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct TenantQuota {
    // The max database can be created in the tenant.
    pub max_databases: u32,

    // The max table per database can be created in the tenant.
    pub max_tables_per_database: u32,
}

impl TryFrom<Vec<u8>> for TenantQuota {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(quota) => Ok(quota),
            Err(err) => Err(ErrorCode::IllegalTenantQuotaFormat(format!(
                "Cannot deserialize tenant quota from bytes. cause {}",
                err
            ))),
        }
    }
}
