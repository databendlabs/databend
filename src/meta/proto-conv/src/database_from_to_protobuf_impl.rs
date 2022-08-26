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

use std::collections::BTreeSet;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_meta_app::schema as mt;
use common_meta_app::share;
use common_protos::pb;

use crate::check_ver;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_COMPATIBLE_VER;
use crate::VER;

impl FromToProto for mt::DatabaseNameIdent {
    type PB = pb::DatabaseNameIdent;
    fn from_pb(p: pb::DatabaseNameIdent) -> Result<Self, Incompatible> {
        check_ver(p.ver, p.min_compatible)?;

        let v = Self {
            tenant: p.tenant,
            db_name: p.db_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseNameIdent, Incompatible> {
        let p = pb::DatabaseNameIdent {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            tenant: self.tenant.clone(),
            db_name: self.db_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::DatabaseMeta {
    type PB = pb::DatabaseMeta;
    fn from_pb(p: pb::DatabaseMeta) -> Result<Self, Incompatible> {
        check_ver(p.ver, p.min_compatible)?;

        let v = Self {
            engine: p.engine,
            engine_options: p.engine_options,
            options: p.options,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            drop_on: match p.drop_on {
                Some(drop_on) => Some(DateTime::<Utc>::from_pb(drop_on)?),
                None => None,
            },
            comment: p.comment,
            shared_by: BTreeSet::from_iter(p.shared_by.into_iter()),
            from_share: match p.from_share {
                Some(from_share) => Some(share::ShareNameIdent::from_pb(from_share)?),
                None => None,
            },
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseMeta, Incompatible> {
        let p = pb::DatabaseMeta {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            engine: self.engine.clone(),
            engine_options: self.engine_options.clone(),
            options: self.options.clone(),
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb()?,
            drop_on: match self.drop_on {
                Some(drop_on) => Some(drop_on.to_pb()?),
                None => None,
            },
            comment: self.comment.clone(),
            shared_by: Vec::from_iter(self.shared_by.clone().into_iter()),
            from_share: match &self.from_share {
                Some(from_share) => Some(from_share.to_pb()?),
                None => None,
            },
        };
        Ok(p)
    }
}

impl FromToProto for mt::DbIdList {
    type PB = pb::DbIdList;
    fn from_pb(p: pb::DbIdList) -> Result<Self, Incompatible> {
        check_ver(p.ver, p.min_compatible)?;

        let v = Self { id_list: p.ids };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DbIdList, Incompatible> {
        let p = pb::DbIdList {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            ids: self.id_list.clone(),
        };
        Ok(p)
    }
}
