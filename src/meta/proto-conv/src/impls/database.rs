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

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::schema as mt;
use databend_common_protos::pb;

use crate::FromProtoOptionExt;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::ToProtoOptionExt;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::DatabaseMeta {
    type PB = pb::DatabaseMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DatabaseMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            engine: p.engine,
            engine_options: p.engine_options,
            options: p.options,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            drop_on: p.drop_on.from_pb_opt()?,
            gc_in_progress: p.gc_in_progress,
            comment: p.comment,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DatabaseMeta, Incompatible> {
        let p = pb::DatabaseMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            engine: self.engine.clone(),
            engine_options: self.engine_options.clone(),
            options: self.options.clone(),
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb()?,
            drop_on: self.drop_on.to_pb_opt()?,
            gc_in_progress: self.gc_in_progress,
            comment: self.comment.clone(),
            shared_by: vec![],
            from_share: None,
            using_share_endpoint: None,
            from_share_db_id: None,
        };
        Ok(p)
    }
}

impl FromToProto for mt::ShareDbId {
    type PB = pb::ShareDbId;
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        0
    }

    fn from_pb(p: pb::ShareDbId) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.db_id {
            Some(pb::share_db_id::DbId::Usage(usage)) => Ok(mt::ShareDbId::Usage(usage.id)),
            Some(pb::share_db_id::DbId::Reference(reference)) => {
                Ok(mt::ShareDbId::Reference(reference.id))
            }
            None => Err(Incompatible::new("ShareDbId cannot be None".to_string())),
        }
    }

    fn to_pb(&self) -> Result<pb::ShareDbId, Incompatible> {
        match self {
            Self::Usage(id) => Ok(Self::PB {
                db_id: Some(pb::share_db_id::DbId::Usage(pb::ShareUsageDbId {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    id: *id,
                })),
            }),
            Self::Reference(id) => Ok(Self::PB {
                db_id: Some(pb::share_db_id::DbId::Reference(pb::ShareReferenceDbId {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    id: *id,
                })),
            }),
        }
    }
}

impl FromToProto for mt::DbIdList {
    type PB = pb::DbIdList;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DbIdList) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self { id_list: p.ids };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DbIdList, Incompatible> {
        let p = pb::DbIdList {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            ids: self.id_list.clone(),
        };
        Ok(p)
    }
}
