// Copyright 2021 Datafuse Labs.
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

//! This mod is the key point about `User` compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use std::collections::BTreeMap;
use std::collections::HashSet;

use common_datavalues as dv;
use common_meta_types as mt;
use common_protos::pb;
use enumflags2::BitFlags;
use num::FromPrimitive;

use crate::check_ver;
use crate::FromToProto;
use crate::Incompatible;

const USER_VER: u64 = 1;
const OLDEST_USER_COMPATIBLE_VER: u64 = 1;

impl FromToProto<pb::AuthInfo> for mt::AuthInfo {
    fn from_pb(p: pb::AuthInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, USER_VER, OLDEST_USER_COMPATIBLE_VER)?;

        match p.info {
            Some(pb::auth_info::Info::None(pb::auth_info::None {})) => Ok(mt::AuthInfo::None),
            Some(pb::auth_info::Info::Jwt(pb::auth_info::Jwt {})) => Ok(mt::AuthInfo::JWT),
            Some(pb::auth_info::Info::Password(pb::auth_info::Password {
                hash_value,
                hash_method,
            })) => Ok(mt::AuthInfo::Password {
                hash_value,
                hash_method: FromPrimitive::from_i32(hash_method).ok_or_else(|| Incompatible {
                    reason: format!("invalid PasswordHashMethod: {}", hash_method),
                })?,
            }),
            None => {
                return Err(Incompatible {
                    reason: format!("AuthInfo error"),
                });
            }
        }
    }

    fn to_pb(&self) -> Result<pb::AuthInfo, Incompatible> {
        let info = match self {
            mt::AuthInfo::None => Some(pb::auth_info::Info::None(pb::auth_info::None {})),
            mt::AuthInfo::JWT => Some(pb::auth_info::Info::Jwt(pb::auth_info::Jwt {})),
            mt::AuthInfo::Password {
                hash_value,
                hash_method,
            } => Some(pb::auth_info::Info::Password(pb::auth_info::Password {
                hash_value: hash_value.clone(),
                hash_method: *hash_method as i32,
            })),
        };
        Ok(pb::AuthInfo {
            ver: USER_VER,
            info,
        })
    }
}

impl FromToProto<pb::UserOption> for mt::UserOption {
    fn from_pb(p: pb::UserOption) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, USER_VER, OLDEST_USER_COMPATIBLE_VER)?;

        let flags = BitFlags::<mt::UserOptionFlag, u64>::from_bits(p.flags);
        match flags {
            Ok(flags) => return Ok(mt::UserOption::new(flags)),
            Err(e) => {
                return Err(Incompatible {
                    reason: format!("UserOptionFlag error: {}", e),
                });
            }
        }
    }

    fn to_pb(&self) -> Result<pb::UserOption, Incompatible> {
        Ok(pb::UserOption {
            ver: USER_VER,
            flags: self.flags().bits(),
        })
    }
}

impl FromToProto<pb::UserQuota> for mt::UserQuota {
    fn from_pb(p: pb::UserQuota) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, USER_VER, OLDEST_USER_COMPATIBLE_VER)?;

        Ok(Self {
            max_cpu: p.max_cpu,
            max_memory_in_bytes: p.max_memory_in_bytes,
            max_storage_in_bytes: p.max_storage_in_bytes,
        })
    }

    fn to_pb(&self) -> Result<pb::UserQuota, Incompatible> {
        Ok(pb::UserQuota {
            ver: USER_VER,
            max_cpu: self.max_cpu,
            max_memory_in_bytes: self.max_memory_in_bytes,
            max_storage_in_bytes: self.max_storage_in_bytes,
        })
    }
}

impl FromToProto<pb::GrantObject> for mt::GrantObject {
    fn from_pb(p: pb::GrantObject) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, USER_VER, OLDEST_USER_COMPATIBLE_VER)?;

        match p.object {
            Some(pb::grant_object::Object::Global(pb::grant_object::GrantGlobalObject {})) => {
                Ok(mt::GrantObject::Global)
            }
            Some(pb::grant_object::Object::Database(pb::grant_object::GrantDatabaseObject {
                db,
            })) => Ok(mt::GrantObject::Database(db)),
            Some(pb::grant_object::Object::Table(pb::grant_object::GrantTableObject {
                db,
                table,
            })) => Ok(mt::GrantObject::Table(db, table)),
            _ => Err(Incompatible {
                reason: format!("GrantObject error"),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::GrantObject, Incompatible> {
        let object = match self {
            mt::GrantObject::Global => Some(pb::grant_object::Object::Global(
                pb::grant_object::GrantGlobalObject {},
            )),
            mt::GrantObject::Database(db) => Some(pb::grant_object::Object::Database(
                pb::grant_object::GrantDatabaseObject { db: db.clone() },
            )),
            mt::GrantObject::Table(db, table) => Some(pb::grant_object::Object::Table(
                pb::grant_object::GrantTableObject {
                    db: db.clone(),
                    table: table.clone(),
                },
            )),
        };
        Ok(pb::GrantObject {
            ver: USER_VER,
            object,
        })
    }
}

impl FromToProto<pb::GrantEntry> for mt::GrantEntry {
    fn from_pb(p: pb::GrantEntry) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, USER_VER, OLDEST_USER_COMPATIBLE_VER)?;

        let privileges = BitFlags::<mt::UserPrivilegeType, u64>::from_bits(p.privileges);
        match privileges {
            Ok(privileges) => {
                return Ok(mt::GrantEntry::new(
                    mt::GrantObject::from_pb(p.object.ok_or_else(|| Incompatible {
                        reason: "GrantEntry.object can not be None".to_string(),
                    })?)?,
                    privileges,
                ))
            }
            Err(e) => {
                return Err(Incompatible {
                    reason: format!("UserPrivilegeType error: {}", e),
                });
            }
        }
    }

    fn to_pb(&self) -> Result<pb::GrantEntry, Incompatible> {
        Ok(pb::GrantEntry {
            ver: USER_VER,
            object: Some(self.object().to_pb()?),
            privileges: self.privileges().bits(),
        })
    }
}

impl FromToProto<pb::UserGrantSet> for mt::UserGrantSet {
    fn from_pb(p: pb::UserGrantSet) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, USER_VER, OLDEST_USER_COMPATIBLE_VER)?;

        let mut entries = Vec::new();
        for entry in p.entries.iter() {
            entries.push(mt::GrantEntry::from_pb(entry.clone())?);
        }
        let mut roles = HashSet::new();
        for role in p.roles.iter() {
            roles.insert(role.0.clone());
        }
        Ok(mt::UserGrantSet::new(entries, roles))
    }

    fn to_pb(&self) -> Result<pb::UserGrantSet, Incompatible> {
        let mut entries = Vec::new();
        for entry in self.entries().iter() {
            entries.push(entry.to_pb()?);
        }

        let mut roles = BTreeMap::new();
        for role in self.roles().iter() {
            roles.insert(role.clone(), true);
        }

        Ok(pb::UserGrantSet {
            ver: USER_VER,
            entries,
            roles,
        })
    }
}

/*
impl FromToProto<pb::UserSetting> for mt::UserSetting {
    fn from_pb(p: pb::UserSetting) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, USER_VER, OLDEST_USER_COMPATIBLE_VER)?;
        let dv = dv::DataTypeImpl::from_pb(p.data_type.ok_or_else(|| Incompatible {
            reason: "DataField.data_type can not be None".to_string(),
        })?)?;
        let v = mt::UserSetting::create(&p.name, dv);
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::UserSetting, Incompatible> {}
}
*/
