// Copyright 2026 Datafuse Labs
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

use std::collections::BTreeSet;

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app::data_share as mt;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::DataShareMeta {
    type PB = pb::DataShareMeta;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(Self {
            provider: p.provider,
            name: p.name,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            comment: p.comment,
            accounts: p.accounts.into_iter().collect::<BTreeSet<_>>(),
            database: p.database.map(database_from_pb).transpose()?,
            tables: p
                .tables
                .into_iter()
                .map(|(name, grant)| Ok((name, table_from_pb(grant)?)))
                .collect::<Result<_, Incompatible>>()?,
            connection: p.connection,
        })
    }

    fn to_pb(&self) -> Self::PB {
        Self::PB {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            provider: self.provider.clone(),
            name: self.name.clone(),
            created_on: self.created_on.to_pb(),
            comment: self.comment.clone(),
            accounts: self.accounts.iter().cloned().collect(),
            database: self.database.as_ref().map(database_to_pb),
            tables: self
                .tables
                .iter()
                .map(|(name, grant)| (name.clone(), table_to_pb(grant)))
                .collect(),
            connection: self.connection.clone(),
        }
    }
}

fn database_from_pb(
    p: pb::DataShareDatabaseGrant,
) -> Result<mt::DataShareDatabaseGrant, Incompatible> {
    Ok(mt::DataShareDatabaseGrant {
        database: p.database,
        database_id: p.database_id,
        shared_on: DateTime::<Utc>::from_pb(p.shared_on)?,
    })
}

fn database_to_pb(grant: &mt::DataShareDatabaseGrant) -> pb::DataShareDatabaseGrant {
    pb::DataShareDatabaseGrant {
        database: grant.database.clone(),
        database_id: grant.database_id,
        shared_on: grant.shared_on.to_pb(),
    }
}

fn table_from_pb(p: pb::DataShareTableGrant) -> Result<mt::DataShareTableGrant, Incompatible> {
    Ok(mt::DataShareTableGrant {
        table_id: p.table_id,
        shared_on: DateTime::<Utc>::from_pb(p.shared_on)?,
    })
}

fn table_to_pb(grant: &mt::DataShareTableGrant) -> pb::DataShareTableGrant {
    pb::DataShareTableGrant {
        table_id: grant.table_id,
        shared_on: grant.shared_on.to_pb(),
    }
}
