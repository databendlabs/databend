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

use std::collections::BTreeMap;
use std::collections::HashSet;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app as mt;
use databend_common_protos::pb;
use enumflags2::BitFlags;
use num::FromPrimitive;

use crate::FromProtoOptionExt;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::ToProtoOptionExt;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::principal::AuthInfo {
    type PB = pb::AuthInfo;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::AuthInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        match p.info {
            Some(pb::auth_info::Info::None(pb::auth_info::None {})) => {
                Ok(mt::principal::AuthInfo::None)
            }
            Some(pb::auth_info::Info::Jwt(pb::auth_info::Jwt {})) => {
                Ok(mt::principal::AuthInfo::JWT)
            }
            Some(pb::auth_info::Info::Password(pb::auth_info::Password {
                hash_value,
                hash_method,
                need_change,
            })) => Ok(mt::principal::AuthInfo::Password {
                hash_value,
                hash_method: FromPrimitive::from_i32(hash_method).ok_or_else(|| {
                    Incompatible::new(format!("invalid PasswordHashMethod: {}", hash_method))
                })?,
                need_change: need_change.unwrap_or_default(),
            }),
            None => Err(Incompatible::new("AuthInfo cannot be None".to_string())),
        }
    }

    fn to_pb(&self) -> Result<pb::AuthInfo, Incompatible> {
        let info = match self {
            mt::principal::AuthInfo::None => {
                Some(pb::auth_info::Info::None(pb::auth_info::None {}))
            }
            mt::principal::AuthInfo::JWT => Some(pb::auth_info::Info::Jwt(pb::auth_info::Jwt {})),
            mt::principal::AuthInfo::Password {
                hash_value,
                hash_method,
                need_change,
            } => Some(pb::auth_info::Info::Password(pb::auth_info::Password {
                hash_value: hash_value.clone(),
                hash_method: *hash_method as i32,
                need_change: Some(*need_change),
            })),
        };
        Ok(pb::AuthInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            info,
        })
    }
}

impl FromToProto for mt::principal::UserOption {
    type PB = pb::UserOption;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserOption) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        // ignore unknown flags
        let flags = BitFlags::<mt::principal::UserOptionFlag, u64>::from_bits_truncate(p.flags);

        Ok(mt::principal::UserOption::default()
            .with_flags(flags)
            .with_default_role(p.default_role)
            .with_default_warehouse(p.default_warehouse)
            .with_network_policy(p.network_policy)
            .with_password_policy(p.password_policy)
            .with_workload_group(p.workload_group)
            .with_disabled(p.disabled)
            .with_must_change_password(p.must_change_password))
    }

    fn to_pb(&self) -> Result<pb::UserOption, Incompatible> {
        Ok(pb::UserOption {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            flags: self.flags().bits(),
            default_role: self.default_role().cloned(),
            default_warehouse: self.default_warehouse().cloned(),
            network_policy: self.network_policy().cloned(),
            password_policy: self.password_policy().cloned(),
            workload_group: self.workload_group().cloned(),
            disabled: self.disabled().cloned(),
            must_change_password: self.must_change_password().cloned(),
        })
    }
}

impl FromToProto for mt::principal::UserQuota {
    type PB = pb::UserQuota;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserQuota) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            max_cpu: p.max_cpu,
            max_memory_in_bytes: p.max_memory_in_bytes,
            max_storage_in_bytes: p.max_storage_in_bytes,
        })
    }

    fn to_pb(&self) -> Result<pb::UserQuota, Incompatible> {
        Ok(pb::UserQuota {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            max_cpu: self.max_cpu,
            max_memory_in_bytes: self.max_memory_in_bytes,
            max_storage_in_bytes: self.max_storage_in_bytes,
        })
    }
}

impl FromToProto for mt::principal::GrantObject {
    type PB = pb::GrantObject;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::GrantObject) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let Some(object) = p.object else {
            return Err(Incompatible::new(format!(
                "Incompatible GrantObject type: Data contains an unrecognized variant for {} version",
                p.ver
            )));
        };

        match object {
            pb::grant_object::Object::Global(pb::grant_object::GrantGlobalObject {}) => {
                Ok(mt::principal::GrantObject::Global)
            }
            pb::grant_object::Object::Database(pb::grant_object::GrantDatabaseObject {
                catalog,
                db,
            }) => Ok(mt::principal::GrantObject::Database(catalog, db)),
            pb::grant_object::Object::Databasebyid(pb::grant_object::GrantDatabaseIdObject {
                catalog,
                db,
            }) => Ok(mt::principal::GrantObject::DatabaseById(catalog, db)),
            pb::grant_object::Object::Table(pb::grant_object::GrantTableObject {
                catalog,
                db,
                table,
            }) => Ok(mt::principal::GrantObject::Table(catalog, db, table)),
            pb::grant_object::Object::Tablebyid(pb::grant_object::GrantTableIdObject {
                catalog,
                db,
                table,
            }) => Ok(mt::principal::GrantObject::TableById(catalog, db, table)),
            pb::grant_object::Object::Udf(pb::grant_object::GrantUdfObject { udf }) => {
                Ok(mt::principal::GrantObject::UDF(udf))
            }
            pb::grant_object::Object::Stage(pb::grant_object::GrantStageObject { stage }) => {
                Ok(mt::principal::GrantObject::Stage(stage))
            }
            pb::grant_object::Object::Warehouse(pb::grant_object::GrantWarehouseObject {
                warehouse,
            }) => Ok(mt::principal::GrantObject::Warehouse(warehouse)),
            pb::grant_object::Object::Connection(pb::grant_object::GrantConnectionObject {
                connection,
            }) => Ok(mt::principal::GrantObject::Connection(connection)),
            pb::grant_object::Object::Sequence(pb::grant_object::GrantSequenceObject {
                sequence,
            }) => Ok(mt::principal::GrantObject::Sequence(sequence)),
            pb::grant_object::Object::Procedure(pb::grant_object::GrantProcedureObject {
                procedure_id,
            }) => Ok(mt::principal::GrantObject::Procedure(procedure_id)),
            pb::grant_object::Object::Maskingpolicy(
                pb::grant_object::GrantMaskingPolicyObject { policy_id },
            ) => Ok(mt::principal::GrantObject::MaskingPolicy(policy_id)),
            pb::grant_object::Object::RowAccessPolicy(
                pb::grant_object::GrantRowAccessPolicyObject { policy_id },
            ) => Ok(mt::principal::GrantObject::RowAccessPolicy(policy_id)),
        }
    }

    fn to_pb(&self) -> Result<pb::GrantObject, Incompatible> {
        let object = match self {
            mt::principal::GrantObject::Global => Some(pb::grant_object::Object::Global(
                pb::grant_object::GrantGlobalObject {},
            )),
            mt::principal::GrantObject::Database(catalog, db) => Some(
                pb::grant_object::Object::Database(pb::grant_object::GrantDatabaseObject {
                    catalog: catalog.clone(),
                    db: db.clone(),
                }),
            ),
            mt::principal::GrantObject::DatabaseById(catalog, db) => Some(
                pb::grant_object::Object::Databasebyid(pb::grant_object::GrantDatabaseIdObject {
                    catalog: catalog.clone(),
                    db: *db,
                }),
            ),
            mt::principal::GrantObject::Table(catalog, db, table) => Some(
                pb::grant_object::Object::Table(pb::grant_object::GrantTableObject {
                    catalog: catalog.clone(),
                    db: db.clone(),
                    table: table.clone(),
                }),
            ),
            mt::principal::GrantObject::TableById(catalog, db, table) => Some(
                pb::grant_object::Object::Tablebyid(pb::grant_object::GrantTableIdObject {
                    catalog: catalog.clone(),
                    db: *db,
                    table: *table,
                }),
            ),
            mt::principal::GrantObject::UDF(udf) => Some(pb::grant_object::Object::Udf(
                pb::grant_object::GrantUdfObject { udf: udf.clone() },
            )),
            mt::principal::GrantObject::Stage(stage) => Some(pb::grant_object::Object::Stage(
                pb::grant_object::GrantStageObject {
                    stage: stage.clone(),
                },
            )),
            mt::principal::GrantObject::Warehouse(w) => Some(pb::grant_object::Object::Warehouse(
                pb::grant_object::GrantWarehouseObject {
                    warehouse: w.clone(),
                },
            )),
            mt::principal::GrantObject::Connection(c) => Some(
                pb::grant_object::Object::Connection(pb::grant_object::GrantConnectionObject {
                    connection: c.clone(),
                }),
            ),
            mt::principal::GrantObject::Sequence(s) => Some(pb::grant_object::Object::Sequence(
                pb::grant_object::GrantSequenceObject {
                    sequence: s.clone(),
                },
            )),
            mt::principal::GrantObject::Procedure(p) => Some(pb::grant_object::Object::Procedure(
                pb::grant_object::GrantProcedureObject { procedure_id: *p },
            )),
            mt::principal::GrantObject::MaskingPolicy(policy_id) => {
                Some(pb::grant_object::Object::Maskingpolicy(
                    pb::grant_object::GrantMaskingPolicyObject {
                        policy_id: *policy_id,
                    },
                ))
            }
            mt::principal::GrantObject::RowAccessPolicy(policy_id) => {
                Some(pb::grant_object::Object::RowAccessPolicy(
                    pb::grant_object::GrantRowAccessPolicyObject {
                        policy_id: *policy_id,
                    },
                ))
            }
        };
        Ok(pb::GrantObject {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            object,
        })
    }
}

impl FromToProto for mt::principal::GrantEntry {
    type PB = pb::GrantEntry;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::GrantEntry) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        // Before https://github.com/datafuselabs/databend/releases/tag/v1.2.321-nightly
        // use from_bits deserialize privilege type, that maybe cause forward compat error.
        // Because old query may not contain new query's privilege type, so from_bits will return err, cause from_pb err.
        // https://docs.rs/enumflags2/0.7.7/enumflags2/struct.BitFlags.html#method.from_bits
        // https://docs.rs/enumflags2/0.7.7/enumflags2/struct.BitFlags.html#method.from_bits_truncate
        let privileges =
            BitFlags::<mt::principal::UserPrivilegeType, u64>::from_bits_truncate(p.privileges);
        Ok(mt::principal::GrantEntry::new(
            mt::principal::GrantObject::from_pb(p.object.ok_or_else(|| {
                Incompatible::new("GrantEntry.object can not be None".to_string())
            })?)?,
            privileges,
        ))
    }

    fn to_pb(&self) -> Result<pb::GrantEntry, Incompatible> {
        Ok(pb::GrantEntry {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            object: Some(self.object().to_pb()?),
            privileges: self.privileges().bits(),
        })
    }
}

impl FromToProto for mt::principal::UserGrantSet {
    type PB = pb::UserGrantSet;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserGrantSet) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut entries = Vec::new();
        for entry in p.entries.into_iter() {
            // If we add new GrantObject in new version
            // Rollback to old version, GrantEntry.object will be None
            // GrantEntry::from_pb will return err so user can not login in old version.
            // Silently dropping unrecognized grant entries and logging the error is
            // intentional: the node must still start and serve other users even when
            // some grant entries are unrecognizable (e.g. during a version rollback).
            match mt::principal::GrantEntry::from_pb(entry) {
                Ok(entry) => entries.push(entry),
                Err(e) => log::error!("GrantEntry::from_pb with error : {e}"),
            }
        }
        let mut roles = HashSet::new();
        for role in p.roles.iter() {
            roles.insert(role.0.clone());
        }
        Ok(mt::principal::UserGrantSet::new(entries, roles))
    }

    fn to_pb(&self) -> Result<pb::UserGrantSet, Incompatible> {
        let mut entries = Vec::new();
        for entry in self.entries().iter() {
            entries.push(entry.to_pb()?);
        }

        let roles = self
            .roles()
            .iter()
            .map(|role| (role.clone(), true))
            .collect::<BTreeMap<_, _>>();

        Ok(pb::UserGrantSet {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            entries,
            roles,
        })
    }
}

impl FromToProto for mt::principal::UserInfo {
    type PB = pb::UserInfo;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserInfo) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::principal::UserInfo {
            name: p.name.clone(),
            hostname: p.hostname.clone(),
            auth_info: mt::principal::AuthInfo::from_pb(p.auth_info.ok_or_else(|| {
                Incompatible::new(format!(
                    "USER {}: UserInfo.auth_info cannot be None",
                    &p.name
                ))
            })?)?,
            grants: mt::principal::UserGrantSet::from_pb(p.grants.ok_or_else(|| {
                Incompatible::new(format!("user {}: UserInfo.grants cannot be None", &p.name))
            })?)?,
            quota: mt::principal::UserQuota::from_pb(p.quota.ok_or_else(|| {
                Incompatible::new(format!("user {}: UserInfo.quota cannot be None", &p.name))
            })?)?,
            option: mt::principal::UserOption::from_pb(p.option.ok_or_else(|| {
                Incompatible::new(format!("user {}: UserInfo.option cannot be None", &p.name))
            })?)?,
            history_auth_infos: p
                .history_auth_infos
                .iter()
                .map(|a| mt::principal::AuthInfo::from_pb(a.clone()))
                .collect::<Result<Vec<mt::principal::AuthInfo>, Incompatible>>()?,
            password_fails: p
                .password_fails
                .iter()
                .map(|t| DateTime::<Utc>::from_pb(t.clone()))
                .collect::<Result<Vec<DateTime<Utc>>, Incompatible>>()?,
            password_update_on: p.password_update_on.from_pb_opt()?,
            lockout_time: p.lockout_time.from_pb_opt()?,
            created_on: p
                .created_on
                .map(FromToProto::from_pb)
                .transpose()?
                .unwrap_or_default(),
            update_on: p
                .update_on
                .map(FromToProto::from_pb)
                .transpose()?
                .unwrap_or_default(),
        })
    }

    fn to_pb(&self) -> Result<pb::UserInfo, Incompatible> {
        Ok(pb::UserInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            hostname: self.hostname.clone(),
            auth_info: Some(mt::principal::AuthInfo::to_pb(&self.auth_info)?),
            grants: Some(mt::principal::UserGrantSet::to_pb(&self.grants)?),
            quota: Some(mt::principal::UserQuota::to_pb(&self.quota)?),
            option: Some(mt::principal::UserOption::to_pb(&self.option)?),
            history_auth_infos: self
                .history_auth_infos
                .iter()
                .map(FromToProto::to_pb)
                .collect::<Result<Vec<pb::AuthInfo>, Incompatible>>()?,
            password_fails: self
                .password_fails
                .iter()
                .map(FromToProto::to_pb)
                .collect::<Result<Vec<String>, Incompatible>>()?,
            password_update_on: self.password_update_on.to_pb_opt()?,
            lockout_time: self.lockout_time.to_pb_opt()?,
            created_on: Some(self.created_on.to_pb()?),
            update_on: Some(self.update_on.to_pb()?),
        })
    }
}

impl FromToProto for mt::principal::UserIdentity {
    type PB = pb::UserIdentity;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::UserIdentity) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(mt::principal::UserIdentity {
            username: p.username.clone(),
            hostname: p.hostname,
        })
    }

    fn to_pb(&self) -> Result<pb::UserIdentity, Incompatible> {
        Ok(pb::UserIdentity {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            username: self.username.clone(),
            hostname: self.hostname.clone(),
        })
    }
}

impl FromToProto for mt::principal::NetworkPolicy {
    type PB = pb::NetworkPolicy;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::NetworkPolicy) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(mt::principal::NetworkPolicy {
            name: p.name.clone(),
            allowed_ip_list: p.allowed_ip_list.clone(),
            blocked_ip_list: p.blocked_ip_list.clone(),
            comment: p.comment,
            create_on: DateTime::<Utc>::from_pb(p.create_on)?,
            update_on: p.update_on.from_pb_opt()?,
        })
    }

    fn to_pb(&self) -> Result<pb::NetworkPolicy, Incompatible> {
        Ok(pb::NetworkPolicy {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            allowed_ip_list: self.allowed_ip_list.clone(),
            blocked_ip_list: self.blocked_ip_list.clone(),
            comment: self.comment.clone(),
            create_on: self.create_on.to_pb()?,
            update_on: self.update_on.to_pb_opt()?,
        })
    }
}

impl FromToProto for mt::principal::PasswordPolicy {
    type PB = pb::PasswordPolicy;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::PasswordPolicy) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(mt::principal::PasswordPolicy {
            name: p.name.clone(),
            min_length: p.min_length,
            max_length: p.max_length,
            min_upper_case_chars: p.min_upper_case_chars,
            min_lower_case_chars: p.min_lower_case_chars,
            min_numeric_chars: p.min_numeric_chars,
            min_special_chars: p.min_special_chars,
            min_age_days: p.min_age_days,
            max_age_days: p.max_age_days,
            max_retries: p.max_retries,
            lockout_time_mins: p.lockout_time_mins,
            history: p.history,
            comment: p.comment,
            create_on: DateTime::<Utc>::from_pb(p.create_on)?,
            update_on: p.update_on.from_pb_opt()?,
        })
    }

    fn to_pb(&self) -> Result<pb::PasswordPolicy, Incompatible> {
        Ok(pb::PasswordPolicy {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            min_length: self.min_length,
            max_length: self.max_length,
            min_upper_case_chars: self.min_upper_case_chars,
            min_lower_case_chars: self.min_lower_case_chars,
            min_numeric_chars: self.min_numeric_chars,
            min_special_chars: self.min_special_chars,
            min_age_days: self.min_age_days,
            max_age_days: self.max_age_days,
            max_retries: self.max_retries,
            lockout_time_mins: self.lockout_time_mins,
            history: self.history,
            comment: self.comment.clone(),
            create_on: self.create_on.to_pb()?,
            update_on: self.update_on.to_pb_opt()?,
        })
    }
}
