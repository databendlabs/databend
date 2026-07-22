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

//! Conversion between protobuf lineage structs and meta structs.

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::schema as mt;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::missing;
use crate::reader_check_msg;

impl FromToProto for mt::LineageDetail {
    type PB = pb::LineageDetail;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        Ok(Self {
            kind: lineage_kind_from_pb(p.kind),
            last_query_id: p.last_query_id,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            column_lineage: p
                .column_lineage
                .into_iter()
                .map(mt::LineageColumn::from_pb)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    fn to_pb(&self) -> Self::PB {
        pb::LineageDetail {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            last_query_id: self.last_query_id.clone(),
            kind: lineage_kind_to_pb(&self.kind),
            updated_on: self.updated_on.to_pb(),
            column_lineage: self.column_lineage.iter().map(|c| c.to_pb()).collect(),
        }
    }
}

fn lineage_kind_from_pb(kind: i32) -> mt::LineageKind {
    match kind {
        value if value == pb::lineage_detail::LineageKind::View as i32 => mt::LineageKind::View,
        value if value == pb::lineage_detail::LineageKind::MaterializedView as i32 => {
            mt::LineageKind::MaterializedView
        }
        value if value == pb::lineage_detail::LineageKind::Ctas as i32 => mt::LineageKind::Ctas,
        value if value == pb::lineage_detail::LineageKind::DataMovement as i32 => {
            mt::LineageKind::DataMovement
        }
        other => mt::LineageKind::Unknown(other),
    }
}

fn lineage_kind_to_pb(kind: &mt::LineageKind) -> i32 {
    match kind {
        mt::LineageKind::View => pb::lineage_detail::LineageKind::View as i32,
        mt::LineageKind::MaterializedView => {
            pb::lineage_detail::LineageKind::MaterializedView as i32
        }
        mt::LineageKind::Ctas => pb::lineage_detail::LineageKind::Ctas as i32,
        mt::LineageKind::DataMovement => pb::lineage_detail::LineageKind::DataMovement as i32,
        mt::LineageKind::Unknown(value) => *value,
    }
}

impl FromToProto for mt::LineageColumn {
    type PB = pb::LineageColumn;

    fn get_pb_ver(_p: &Self::PB) -> u64 {
        VER
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Self {
            upstream: p
                .upstream
                .map(mt::ColumnRef::from_pb)
                .transpose()?
                .ok_or_else(missing("LineageColumn.upstream"))?,
            downstream: p
                .downstream
                .map(mt::ColumnRef::from_pb)
                .transpose()?
                .ok_or_else(missing("LineageColumn.downstream"))?,
        })
    }

    fn to_pb(&self) -> Self::PB {
        pb::LineageColumn {
            upstream: Some(self.upstream.to_pb()),
            downstream: Some(self.downstream.to_pb()),
        }
    }
}

impl FromToProto for mt::ColumnRef {
    type PB = pb::ColumnRef;

    fn get_pb_ver(_p: &Self::PB) -> u64 {
        VER
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.identity.ok_or_else(missing("ColumnRef.identity"))? {
            pb::column_ref::Identity::Id(id) => Ok(Self::Id(id)),
            pb::column_ref::Identity::Name(name) => Ok(Self::Name(name)),
        }
    }

    fn to_pb(&self) -> Self::PB {
        let identity = match self {
            mt::ColumnRef::Id(id) => pb::column_ref::Identity::Id(*id),
            mt::ColumnRef::Name(name) => pb::column_ref::Identity::Name(name.clone()),
        };
        pb::ColumnRef {
            identity: Some(identity),
        }
    }
}
