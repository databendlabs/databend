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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_meta_types as mt;
use common_meta_types::DatabaseIdent;
use common_meta_types::DatabaseNameIdent;
use common_protos::pb;

use crate::check_ver;
use crate::missing;
use crate::FromToProto;
use crate::Incompatible;
use crate::VER;

impl FromToProto<pb::DatabaseInfo> for mt::DatabaseInfo {
    fn from_pb(p: pb::DatabaseInfo) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let meta = match p.meta {
            None => {
                return Err(Incompatible {
                    reason: "DatabaseInfo.meta can not be None".to_string(),
                })
            }
            Some(x) => x,
        };

        let v = Self {
            ident: DatabaseIdent::from_pb(p.ident.ok_or_else(missing("DatabaseInfo.ident"))?)?,
            name_ident: DatabaseNameIdent::from_pb(
                p.name_ident
                    .ok_or_else(missing("DatabaseInfo.name_ident"))?,
            )?,
            meta: mt::DatabaseMeta::from_pb(meta)?,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseInfo, Incompatible> {
        let p = pb::DatabaseInfo {
            ver: VER,
            ident: Some(self.ident.to_pb()?),
            name_ident: Some(self.name_ident.to_pb()?),
            meta: Some(self.meta.to_pb()?),
        };
        Ok(p)
    }
}

impl FromToProto<pb::DatabaseNameIdent> for mt::DatabaseNameIdent {
    fn from_pb(p: pb::DatabaseNameIdent) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            tenant: p.tenant,
            db_name: p.db_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseNameIdent, Incompatible> {
        let p = pb::DatabaseNameIdent {
            ver: VER,
            tenant: self.tenant.clone(),
            db_name: self.db_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto<pb::DatabaseIdent> for mt::DatabaseIdent {
    fn from_pb(p: pb::DatabaseIdent) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            db_id: p.db_id,
            seq: p.seq,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseIdent, Incompatible> {
        let p = pb::DatabaseIdent {
            ver: VER,
            db_id: self.db_id,
            seq: self.seq,
        };
        Ok(p)
    }
}

impl FromToProto<pb::DatabaseMeta> for mt::DatabaseMeta {
    fn from_pb(p: pb::DatabaseMeta) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            engine: p.engine,
            engine_options: p.engine_options,
            options: p.options,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            comment: p.comment,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseMeta, Incompatible> {
        let p = pb::DatabaseMeta {
            ver: VER,
            engine: self.engine.clone(),
            engine_options: self.engine_options.clone(),
            options: self.options.clone(),
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb()?,
            comment: self.comment.clone(),
        };
        Ok(p)
    }
}
