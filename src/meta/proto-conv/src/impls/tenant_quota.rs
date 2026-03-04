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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use databend_common_meta_app::tenant;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for tenant::TenantQuota {
    type PB = pb::TenantQuota;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TenantQuota) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            max_databases: p.max_databases,
            max_tables_per_database: p.max_tables_per_database,
            max_stages: p.max_stages,
            max_files_per_stage: p.max_files_per_stage,
            max_users: p.max_users,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TenantQuota, Incompatible> {
        let p = pb::TenantQuota {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            max_databases: self.max_databases,
            max_tables_per_database: self.max_tables_per_database,
            max_stages: self.max_stages,
            max_files_per_stage: self.max_files_per_stage,
            max_users: self.max_users,
        };
        Ok(p)
    }
}
