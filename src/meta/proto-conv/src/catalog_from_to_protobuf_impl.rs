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

use common_meta_app::schema as mt;
use common_protos::pb;

use crate::check_ver;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_COMPATIBLE_VER;
use crate::VER;

impl FromToProto for mt::CatalogNameIdent {
    type PB = pb::CatalogNameIdent;
    fn from_pb(p: Self::PB) -> Result<Self, crate::Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        let v = Self {
            tenant: p.tenant,
            ctl_name: p.ctl_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::CatalogNameIdent, Incompatible> {
        let p = pb::CatalogNameIdent {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            tenant: self.tenant.clone(),
            ctl_name: self.ctl_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::CatalogMeta {
    type PB = pb::CatalogMeta;
    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        check_ver(p.ver, p.min_compatible)?;

        let v = Self {
            catalog_type: CatalogType::from_pb(p.catalog_type)?,
            options: p.options,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            dropped_on: match p.dropped_on {
                Some(dropped_on) => Some(DateTime::<Utc>::from_pb(dropped_on)?),
                None => None,
            },
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let p = pb::CatalogMeta {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            catalog_type: self.catalog_type.to_pb()?,
            options: self.options.clone(),
            created_on: self.created_on.to_pb()?,
            dropped_on: match self.dropped_on.clone() {
                Some(dropped_on) => Some(dropped_on.to_pb()?),
                None => None,
            },
        };
        Ok(p)
    }
}

impl FromToProto for mt::CatalogType {
    type PB = pb::CatalogType;

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        match p.ctl_type {
            0 => Ok(Self::Default),
            1 => Ok(Self::Hive),
            _ => Err(Incompatible),
        }
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let ctl_type = match self {
            CatalogType::Default => 0,
            CatalogType::Hive => 1,
        };
        Ok(PB {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            ctl_type,
        })
    }
}

impl FromToProto for mt::CatalogIdList {
    type PB = pb::CatalogIdList;
    fn from_pb(p: pb::DbIdList) -> Result<Self, Incompatible> {
        check_ver(p.ver, p.min_compatible)?;

        let v = Self { id_list: p.ids };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::CatalogIdList, Incompatible> {
        let p = pb::CatalogIdList {
            ver: VER,
            min_compatible: MIN_COMPATIBLE_VER,
            ids: self.id_list.clone(),
        };
        Ok(p)
    }
}
