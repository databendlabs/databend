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

use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::share as mt;
use databend_common_protos::pb;
use enumflags2::BitFlags;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::ShareDatabase {
    type PB = pb::ShareDatabase;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::ShareDatabase) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let privileges =
            BitFlags::<mt::ShareGrantObjectPrivilege, u64>::from_bits_truncate(p.privileges);
        let v = Self {
            privileges,
            db_id: p.db_id,
            name: p.name,
            grant_on: DateTime::<Utc>::from_pb(p.grant_on)?,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::ShareDatabase, Incompatible> {
        let p = pb::ShareDatabase {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            privileges: self.privileges.bits(),
            db_id: self.db_id,
            name: self.name.clone(),
            grant_on: self.grant_on.to_pb()?,
        };
        Ok(p)
    }
}

impl FromToProto for mt::ShareTable {
    type PB = pb::ShareTable;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::ShareTable) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let privileges =
            BitFlags::<mt::ShareGrantObjectPrivilege, u64>::from_bits_truncate(p.privileges);
        let v = Self {
            privileges,
            name: p.name,
            db_id: p.db_id,
            table_id: p.table_id,
            grant_on: DateTime::<Utc>::from_pb(p.grant_on)?,
            engine: p.engine.clone(),
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::ShareTable, Incompatible> {
        let p = pb::ShareTable {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            privileges: self.privileges.bits(),
            name: self.name.clone(),
            db_id: self.db_id,
            table_id: self.table_id,
            grant_on: self.grant_on.to_pb()?,
            engine: self.engine.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::ShareMetaV2 {
    type PB = pb::ShareMetaV2;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::ShareMetaV2) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut reference_database = Vec::with_capacity(p.reference_database.len());
        for db in &p.reference_database {
            let db = mt::ShareDatabase::from_pb(db.clone())?;
            reference_database.push(db);
        }
        let mut table = Vec::with_capacity(p.table.len());
        for t in &p.table {
            table.push(mt::ShareTable::from_pb(t.clone())?);
        }
        let mut reference_table = Vec::with_capacity(p.reference_table.len());
        for t in &p.reference_table {
            reference_table.push(mt::ShareTable::from_pb(t.clone())?);
        }

        let v = Self {
            accounts: BTreeSet::from_iter(p.accounts.clone()),
            comment: p.comment.clone(),
            create_on: DateTime::<Utc>::from_pb(p.create_on)?,
            update_on: DateTime::<Utc>::from_pb(p.update_on)?,

            use_database: match p.use_database {
                Some(use_database) => Some(mt::ShareDatabase::from_pb(use_database)?),
                None => None,
            },
            reference_database,
            table,
            reference_table,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::ShareMetaV2, Incompatible> {
        let mut reference_database = Vec::with_capacity(self.reference_database.len());
        for v in &self.reference_database {
            reference_database.push(v.to_pb()?);
        }
        let mut table = Vec::with_capacity(self.table.len());
        for v in &self.table {
            table.push(v.to_pb()?);
        }
        let mut reference_table = Vec::with_capacity(self.reference_table.len());
        for v in &self.reference_table {
            reference_table.push(v.to_pb()?);
        }

        let p = pb::ShareMetaV2 {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            accounts: Vec::from_iter(self.accounts.clone()),
            comment: self.comment.clone(),
            create_on: self.create_on.to_pb()?,
            update_on: self.update_on.to_pb()?,

            use_database: match &self.use_database {
                Some(use_database) => Some(use_database.to_pb()?),
                None => None,
            },
            reference_database,
            table,
            reference_table,
        };
        Ok(p)
    }
}
