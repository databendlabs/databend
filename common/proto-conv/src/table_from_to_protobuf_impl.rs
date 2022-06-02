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

use std::sync::Arc;

use common_datavalues as dv;
use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_meta_app::schema as mt;
use common_protos::pb;

use crate::check_ver;
use crate::FromToProto;
use crate::Incompatible;
use crate::VER;

impl FromToProto<pb::TableInfo> for mt::TableInfo {
    fn from_pb(p: pb::TableInfo) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let ident = match p.ident {
            None => {
                return Err(Incompatible {
                    reason: "TableInfo.ident can not be None".to_string(),
                })
            }
            Some(x) => x,
        };
        let v = Self {
            ident: mt::TableIdent::from_pb(ident)?,
            desc: p.desc,
            name: p.name,
            meta: mt::TableMeta::from_pb(p.meta.ok_or_else(|| Incompatible {
                reason: "TableInfo.meta can not be None".to_string(),
            })?)?,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableInfo, Incompatible> {
        let p = pb::TableInfo {
            ver: VER,
            ident: Some(self.ident.to_pb()?),
            desc: self.desc.clone(),
            name: self.name.clone(),
            meta: Some(self.meta.to_pb()?),
        };
        Ok(p)
    }
}

impl FromToProto<pb::TableNameIdent> for mt::TableNameIdent {
    fn from_pb(p: pb::TableNameIdent) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            tenant: p.tenant,
            db_name: p.db_name,
            table_name: p.table_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableNameIdent, Incompatible> {
        let p = pb::TableNameIdent {
            ver: VER,
            tenant: self.tenant.clone(),
            db_name: self.db_name.clone(),
            table_name: self.table_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto<pb::TableIdent> for mt::TableIdent {
    fn from_pb(p: pb::TableIdent) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            table_id: p.table_id,
            seq: p.seq,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableIdent, Incompatible> {
        let p = pb::TableIdent {
            ver: VER,
            table_id: self.table_id,
            seq: self.seq,
        };

        Ok(p)
    }
}

impl FromToProto<pb::TableMeta> for mt::TableMeta {
    fn from_pb(p: pb::TableMeta) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let schema = match p.schema {
            None => {
                return Err(Incompatible {
                    reason: "TableMeta.schema can not be None".to_string(),
                })
            }
            Some(x) => x,
        };

        let v = Self {
            schema: Arc::new(dv::DataSchema::from_pb(schema)?),
            engine: p.engine,
            engine_options: p.engine_options,
            options: p.options,
            cluster_key_meta: p
                .cluster_key_meta
                .map(mt::ClusterKeyMeta::from_pb)
                .transpose()?,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            drop_on: match p.drop_on {
                Some(drop_on) => Some(DateTime::<Utc>::from_pb(drop_on)?),
                None => None,
            },
            comment: p.comment,
            statistics: p
                .statistics
                .map(mt::TableStatistics::from_pb)
                .transpose()?
                .unwrap_or_default(),
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableMeta, Incompatible> {
        let p = pb::TableMeta {
            ver: VER,
            schema: Some(self.schema.to_pb()?),
            engine: self.engine.clone(),
            engine_options: self.engine_options.clone(),
            options: self.options.clone(),
            cluster_key_meta: self
                .cluster_key_meta
                .as_ref()
                .map(|x| x.to_pb())
                .transpose()?,
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb()?,
            drop_on: match self.drop_on {
                Some(drop_on) => Some(drop_on.to_pb()?),
                None => None,
            },
            comment: self.comment.clone(),
            statistics: Some(self.statistics.to_pb()?),
        };
        Ok(p)
    }
}

impl FromToProto<pb::TableStatistics> for mt::TableStatistics {
    fn from_pb(p: pb::TableStatistics) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            number_of_rows: p.number_of_rows,
            data_bytes: p.data_bytes,
            compressed_data_bytes: p.compressed_data_bytes,
            index_data_bytes: p.index_data_bytes,
        };

        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableStatistics, Incompatible> {
        let p = pb::TableStatistics {
            ver: VER,
            number_of_rows: self.number_of_rows,
            data_bytes: self.data_bytes,
            compressed_data_bytes: self.compressed_data_bytes,
            index_data_bytes: self.index_data_bytes,
        };
        Ok(p)
    }
}

impl FromToProto<pb::ClusterKeyMeta> for mt::ClusterKeyMeta {
    fn from_pb(p: pb::ClusterKeyMeta) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self {
            cluster_keys_vec: p.cluster_keys_vec,
            default_cluster_key_id: p.default_cluster_key_id,
        };

        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::ClusterKeyMeta, Incompatible> {
        let p = pb::ClusterKeyMeta {
            ver: VER,
            cluster_keys_vec: self.cluster_keys_vec.clone(),
            default_cluster_key_id: self.default_cluster_key_id,
        };
        Ok(p)
    }
}

impl FromToProto<pb::TableIdList> for mt::TableIdList {
    fn from_pb(p: pb::TableIdList) -> Result<Self, Incompatible> {
        check_ver(p.ver)?;

        let v = Self { id_list: p.ids };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableIdList, Incompatible> {
        let p = pb::TableIdList {
            ver: VER,
            ids: self.id_list.clone(),
        };
        Ok(p)
    }
}
