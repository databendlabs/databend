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
use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_base::non_empty::NonEmptyString;
use databend_common_expression as ex;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::schema::SecurityPolicyColumnMap;
use databend_common_meta_app::tenant::Tenant;
use databend_common_protos::pb;
use num::FromPrimitive;

use crate::FromProtoOptionExt;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::ToProtoOptionExt;
use crate::VER;
use crate::reader_check_msg;

impl FromToProto for mt::TableCopiedFileInfo {
    type PB = pb::TableCopiedFileInfo;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableCopiedFileInfo) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            etag: p.etag,
            content_length: p.content_length,
            last_modified: p.last_modified.from_pb_opt()?,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableCopiedFileInfo, Incompatible> {
        let p = pb::TableCopiedFileInfo {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            etag: self.etag.clone(),
            content_length: self.content_length,
            last_modified: self.last_modified.to_pb_opt()?,
        };
        Ok(p)
    }
}

impl FromToProto for mt::EmptyProto {
    type PB = pb::EmptyProto;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::EmptyProto) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {};
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::EmptyProto, Incompatible> {
        let p = pb::EmptyProto {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
        };
        Ok(p)
    }
}

impl FromToProto for mt::TableNameIdent {
    type PB = pb::TableNameIdent;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableNameIdent) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let non_empty = NonEmptyString::new(p.tenant.clone())
            .map_err(|_e| Incompatible::new("tenant is empty"))?;

        let tenant = Tenant::new_nonempty(non_empty);

        let v = Self {
            tenant,
            db_name: p.db_name,
            table_name: p.table_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableNameIdent, Incompatible> {
        let p = pb::TableNameIdent {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            tenant: self.tenant.tenant_name().to_string(),
            db_name: self.db_name.clone(),
            table_name: self.table_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::DBIdTableName {
    type PB = pb::DbIdTableName;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DbIdTableName) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            db_id: p.db_id,
            table_name: p.table_name,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DbIdTableName, Incompatible> {
        let p = pb::DbIdTableName {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            db_id: self.db_id,
            table_name: self.table_name.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::TableIdent {
    type PB = pb::TableIdent;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableIdent) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            table_id: p.table_id,
            seq: p.seq,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableIdent, Incompatible> {
        let p = pb::TableIdent {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            table_id: self.table_id,
            seq: self.seq,
        };

        Ok(p)
    }
}

impl FromToProto for mt::TableMeta {
    type PB = pb::TableMeta;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableMeta) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let schema = p
            .schema
            .ok_or_else(|| Incompatible::new("TableMeta.schema can not be None".to_string()))?;
        let virtual_schema = p.virtual_schema.from_pb_opt()?;

        let indexes = p
            .indexes
            .into_iter()
            .map(|(name, index)| Ok((name, mt::TableIndex::from_pb(index)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;

        let cluster_key_seq = if let Some(seq) = p.cluster_key_seq {
            seq
        } else if p.cluster_keys.is_empty() {
            0
        } else {
            p.cluster_keys.len() as u32 - 1
        };
        let constraints = p
            .constraints
            .into_iter()
            .map(|(name, constraint)| Ok((name, mt::Constraint::from_pb(constraint)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let refs = p
            .refs
            .into_iter()
            .map(|(name, snapshot_ref)| Ok((name, mt::SnapshotRef::from_pb(snapshot_ref)?)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let v = Self {
            schema: Arc::new(ex::TableSchema::from_pb(schema)?),
            engine: p.engine,
            engine_options: p.engine_options,
            storage_params: p.storage_params.from_pb_opt()?,
            part_prefix: p.part_prefix.unwrap_or("".to_string()),
            options: p.options,
            cluster_key: None,
            cluster_key_v2: p.cluster_key.map(|k| {
                let id = p.cluster_key_id.unwrap_or(cluster_key_seq);
                (id, k)
            }),
            cluster_key_seq,
            created_on: DateTime::<Utc>::from_pb(p.created_on)?,
            updated_on: DateTime::<Utc>::from_pb(p.updated_on)?,
            drop_on: p.drop_on.from_pb_opt()?,
            comment: p.comment,
            field_comments: p.field_comments,
            statistics: p
                .statistics
                .map(FromToProto::from_pb)
                .transpose()?
                .unwrap_or_default(),
            shared_by: BTreeSet::from_iter(p.shared_by),
            column_mask_policy: if p.column_mask_policy.is_empty() {
                None
            } else {
                Some(p.column_mask_policy)
            },
            row_access_policy: p.row_access_policy,
            row_access_policy_columns_ids: p
                .row_access_policy_columns_ids
                .map(FromToProto::from_pb)
                .transpose()?,
            column_mask_policy_columns_ids: p
                .column_mask_policy_columns_ids
                .into_iter()
                .map(|(k, v)| Ok((k, SecurityPolicyColumnMap::from_pb(v)?)))
                .collect::<Result<BTreeMap<_, _>, _>>()?,
            indexes,
            virtual_schema,
            constraints,
            refs,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableMeta, Incompatible> {
        let indexes = self
            .indexes
            .iter()
            .map(|(name, index)| Ok((name.clone(), index.to_pb()?)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let constraints = self
            .constraints
            .iter()
            .map(|(name, constraint)| Ok((name.clone(), constraint.to_pb()?)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let refs = self
            .refs
            .iter()
            .map(|(name, snapshot_ref)| Ok((name.clone(), snapshot_ref.to_pb()?)))
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let (cluster_key_id, cluster_key) = self.cluster_key_meta().unzip();
        let p = pb::TableMeta {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            schema: Some(self.schema.to_pb()?),
            engine: self.engine.clone(),
            engine_options: self.engine_options.clone(),
            storage_params: self.storage_params.to_pb_opt()?,
            part_prefix: if self.part_prefix.is_empty() {
                None
            } else {
                Some(self.part_prefix.clone())
            },
            options: self.options.clone(),
            cluster_key,
            cluster_key_id,
            // cluster_keys is deprecated.
            cluster_keys: vec![],
            cluster_key_seq: Some(self.cluster_key_seq),
            created_on: self.created_on.to_pb()?,
            updated_on: self.updated_on.to_pb()?,
            drop_on: self.drop_on.to_pb_opt()?,
            comment: self.comment.clone(),
            field_comments: self.field_comments.clone(),
            statistics: Some(self.statistics.to_pb()?),
            shared_by: Vec::from_iter(self.shared_by.clone()),
            column_mask_policy: self.column_mask_policy.clone().unwrap_or_default(),
            row_access_policy: self.row_access_policy.clone(),
            row_access_policy_columns_ids: self.row_access_policy_columns_ids.to_pb_opt()?,
            column_mask_policy_columns_ids: self
                .column_mask_policy_columns_ids
                .iter()
                .map(|(k, v)| Ok((*k, v.to_pb()?)))
                .collect::<Result<BTreeMap<_, _>, _>>()?,
            indexes,
            virtual_schema: self.virtual_schema.to_pb_opt()?,
            constraints,
            refs,
        };
        Ok(p)
    }
}

impl FromToProto for SecurityPolicyColumnMap {
    type PB = pb::RowAccessPolicyColumnMap;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            policy_id: p.policy_id,
            columns_ids: p.columns_ids.clone(),
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let p = pb::RowAccessPolicyColumnMap {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            policy_id: self.policy_id,
            columns_ids: self.columns_ids.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::Constraint {
    type PB = pb::Constraint;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let constraint = p.constraint.ok_or_else(|| {
            Incompatible::new("Invalid Constraint: .constraint can not be None".to_string())
        })?;
        Ok(match constraint {
            pb::constraint::Constraint::Check(expr) => mt::Constraint::Check(expr),
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        let constraint = match self {
            mt::Constraint::Check(expr) => pb::constraint::Constraint::Check(expr.clone()),
        };
        Ok(pb::Constraint {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            constraint: Some(constraint),
        })
    }
}

impl FromToProto for mt::TableStatistics {
    type PB = pb::TableStatistics;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableStatistics) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            number_of_rows: p.number_of_rows,
            data_bytes: p.data_bytes,
            compressed_data_bytes: p.compressed_data_bytes,
            index_data_bytes: p.index_data_bytes,
            number_of_segments: p.number_of_segments,
            number_of_blocks: p.number_of_blocks,
            bloom_index_size: p.bloom_index_size,
            ngram_index_size: p.ngram_index_size,
            inverted_index_size: p.inverted_index_size,
            vector_index_size: p.vector_index_size,
            virtual_column_size: p.virtual_column_size,
        };

        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableStatistics, Incompatible> {
        let p = pb::TableStatistics {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            number_of_rows: self.number_of_rows,
            data_bytes: self.data_bytes,
            compressed_data_bytes: self.compressed_data_bytes,
            index_data_bytes: self.index_data_bytes,
            number_of_segments: self.number_of_segments,
            number_of_blocks: self.number_of_blocks,
            bloom_index_size: self.bloom_index_size,
            ngram_index_size: self.ngram_index_size,
            inverted_index_size: self.inverted_index_size,
            vector_index_size: self.vector_index_size,
            virtual_column_size: self.virtual_column_size,
        };
        Ok(p)
    }
}

impl FromToProto for mt::TableIdList {
    type PB = pb::TableIdList;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableIdList) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self { id_list: p.ids };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableIdList, Incompatible> {
        let p = pb::TableIdList {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            ids: self.id_list.clone(),
        };
        Ok(p)
    }
}

impl FromToProto for mt::TableIndex {
    type PB = pb::TableIndex;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::TableIndex) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let v = Self {
            index_type: FromPrimitive::from_i32(p.index_type)
                .ok_or_else(|| Incompatible::new(format!("invalid IndexType: {}", p.index_type)))?,
            name: p.name,
            column_ids: p.column_ids,
            sync_creation: p.sync_creation,
            version: p.version.clone(),
            options: p.options.clone(),
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::TableIndex, Incompatible> {
        let p = pb::TableIndex {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name.clone(),
            column_ids: self.column_ids.clone(),
            sync_creation: self.sync_creation,
            version: self.version.clone(),
            options: self.options.clone(),
            index_type: self.index_type.clone() as i32,
        };
        Ok(p)
    }
}

impl FromToProto for mt::SnapshotRef {
    type PB = pb::SnapshotRef;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::SnapshotRef) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        let v = Self {
            id: p.id,
            expire_at: p.expire_at.from_pb_opt()?,
            typ: FromPrimitive::from_i32(p.typ)
                .ok_or_else(|| Incompatible::new(format!("invalid RefType: {}", p.typ)))?,
            loc: p.loc,
        };
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::SnapshotRef, Incompatible> {
        let p = pb::SnapshotRef {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            id: self.id,
            expire_at: self.expire_at.to_pb_opt()?,
            typ: self.typ as i32,
            loc: self.loc.clone(),
        };
        Ok(p)
    }
}
