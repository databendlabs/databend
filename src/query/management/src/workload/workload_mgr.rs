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

use std::collections::HashMap;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::base::escape_for_key;
use databend_common_base::runtime::workload_group::QuotaValue;
use databend_common_base::runtime::workload_group::WorkloadGroup;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_store::MetaStore;
use databend_meta_kvapi::kvapi::KVApi;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::SeqV;
use databend_meta_types::TxnCondition;
use databend_meta_types::TxnOp;
use databend_meta_types::TxnRequest;

use crate::errors::meta_service_error;
use crate::workload::workload_api::WorkloadApi;
pub static WORKLOAD_META_KEY_PREFIX: &str = "__fd_workloads";

pub struct WorkloadMgr {
    metastore: MetaStore,
    workload_key_prefix: String,
    workload_index_prefix: String,
}

impl WorkloadMgr {
    pub fn create(metastore: MetaStore, tenant: &str) -> Result<Self> {
        Ok(WorkloadMgr {
            metastore,
            workload_key_prefix: format!(
                "{}/{}/workload_groups",
                WORKLOAD_META_KEY_PREFIX,
                escape_for_key(tenant)?
            ),
            workload_index_prefix: format!(
                "{}/{}/index_workload_groups",
                WORKLOAD_META_KEY_PREFIX,
                escape_for_key(tenant)?
            ),
        })
    }

    pub async fn get_id_by_name(&self, name: &str) -> Result<String> {
        let index_key = format!("{}/{}", self.workload_index_prefix, escape_for_key(name)?);

        let Some(seq) = self
            .metastore
            .get_kv(&index_key)
            .await
            .map_err(meta_service_error)?
        else {
            return Err(ErrorCode::UnknownWorkload(format!(
                "Unknown workload {}",
                name
            )));
        };

        Ok(unsafe { String::from_utf8_unchecked(seq.data) })
    }

    async fn get_seq_by_name(&self, name: &str) -> Result<SeqV<WorkloadGroup>> {
        let id = self.get_id_by_name(name).await?;

        let Some(seq) = self.get_seq_by_id(&id).await? else {
            return Err(ErrorCode::UnknownWorkload(format!(
                "Unknown workload {}",
                name
            )));
        };

        Ok(seq)
    }

    async fn get_seq_by_id(&self, id: &str) -> Result<Option<SeqV<WorkloadGroup>>> {
        let workload_key = format!("{}/{}", self.workload_key_prefix, id);

        let Some(seq) = self
            .metastore
            .get_kv(&workload_key)
            .await
            .map_err(meta_service_error)?
        else {
            return Ok(None);
        };

        Ok(Some(SeqV::new(
            seq.seq,
            serde_json::from_slice::<WorkloadGroup>(&seq.data)?,
        )))
    }
}

#[async_trait::async_trait]
impl WorkloadApi for WorkloadMgr {
    async fn create(&self, mut group: WorkloadGroup) -> Result<WorkloadGroup> {
        if group.name.is_empty() {
            return Err(ErrorCode::InvalidWorkload("Workload group name is empty."));
        }

        if !group.id.is_empty() {
            return Err(ErrorCode::InvalidWorkload(
                "Workload group id is not empty.",
            ));
        }

        let group_id = GlobalUniq::unique();
        let mut create_workload = TxnRequest::default();

        group.id = group_id.clone();
        let escape_name = escape_for_key(&group.name)?;
        let workload_key = format!("{}/{}", self.workload_key_prefix, group_id);
        let workload_index_key = format!("{}/{}", self.workload_index_prefix, escape_name);

        create_workload
            .condition
            .push(TxnCondition::eq_seq(workload_index_key.clone(), 0));
        create_workload
            .if_then
            .push(TxnOp::put(workload_index_key, group_id.into_bytes()));
        create_workload
            .if_then
            .push(TxnOp::put(workload_key, serde_json::to_vec(&group)?));

        match self
            .metastore
            .transaction(create_workload)
            .await
            .map_err(meta_service_error)?
        {
            res if res.success => Ok(group),
            _res => Err(ErrorCode::AlreadyExistsWorkload(format!(
                "The workload {} already exits.",
                group.name
            ))),
        }
    }

    async fn drop(&self, name: String) -> Result<()> {
        let workload_id = self.get_id_by_name(&name).await?;

        let escape_name = escape_for_key(&name)?;
        let workload_key = format!("{}/{}", self.workload_key_prefix, workload_id);
        let workload_index_key = format!("{}/{}", self.workload_index_prefix, escape_name);

        let mut drop_workload = TxnRequest::default();
        drop_workload.condition.push(TxnCondition::eq_value(
            workload_index_key.clone(),
            workload_id.into_bytes(),
        ));
        drop_workload.if_then.push(TxnOp::delete(workload_key));
        drop_workload
            .if_then
            .push(TxnOp::delete(workload_index_key));

        match self
            .metastore
            .transaction(drop_workload)
            .await
            .map_err(meta_service_error)?
        {
            res if res.success => Ok(()),
            _res => Err(ErrorCode::UnknownWorkload(format!(
                "Unknown workload {}",
                name
            ))),
        }
    }

    async fn rename(&self, old_name: String, new_name: String) -> Result<()> {
        let mut workload = self.get_by_name(&old_name).await?;
        workload.name = new_name.clone();

        let workload_key = format!("{}/{}", self.workload_key_prefix, workload.id);

        let escape_name = escape_for_key(&old_name)?;
        let old_workload_index_key = format!("{}/{}", self.workload_index_prefix, escape_name);

        let escape_name = escape_for_key(&new_name)?;
        let new_workload_index_key = format!("{}/{}", self.workload_index_prefix, escape_name);

        let mut rename_workload = TxnRequest::default();
        rename_workload
            .condition
            .push(TxnCondition::eq_seq(new_workload_index_key.clone(), 0));
        rename_workload.condition.push(TxnCondition::eq_value(
            old_workload_index_key.clone(),
            workload.id.clone().into_bytes(),
        ));

        rename_workload
            .if_then
            .push(TxnOp::put(workload_key, serde_json::to_vec(&workload)?));
        rename_workload
            .if_then
            .push(TxnOp::delete(old_workload_index_key));
        rename_workload.if_then.push(TxnOp::put(
            new_workload_index_key,
            workload.id.clone().into_bytes(),
        ));

        match self
            .metastore
            .transaction(rename_workload)
            .await
            .map_err(meta_service_error)?
        {
            res if res.success => Ok(()),
            _res => Err(ErrorCode::InvalidWorkload(format!(
                "Unknown workload {} or workload {} already exists",
                old_name, new_name
            ))),
        }
    }

    async fn set_quotas(&self, name: String, quotas: HashMap<String, QuotaValue>) -> Result<()> {
        for _index in 0..5 {
            let workload = self.get_seq_by_name(&name).await?;
            let seq = workload.seq;
            let mut workload = workload.data;

            for (key, value) in &quotas {
                workload.quotas.insert(key.clone(), value.clone());
            }

            let workload_key = format!("{}/{}", self.workload_key_prefix, workload.id);
            let mut alter_workload = TxnRequest::default();
            alter_workload
                .condition
                .push(TxnCondition::eq_seq(workload_key.clone(), seq));
            alter_workload
                .if_then
                .push(TxnOp::put(workload_key, serde_json::to_vec(&workload)?));

            if self
                .metastore
                .transaction(alter_workload)
                .await
                .map_err(meta_service_error)?
                .success
            {
                return Ok(());
            }
        }

        Err(ErrorCode::WorkloadOperateConflict(
            "Workload operate conflict(tried 5 times).",
        ))
    }

    async fn unset_quotas(&self, name: String, quotas: Vec<String>) -> Result<()> {
        for _index in 0..5 {
            let workload = self.get_seq_by_name(&name).await?;
            let seq = workload.seq;
            let mut workload = workload.data;

            for quota_name in &quotas {
                if workload.quotas.remove(quota_name).is_none() {
                    return Err(ErrorCode::UnknownWorkloadQuotas(format!(
                        "Unknown workload group quota name {} in {}",
                        quota_name, name
                    )));
                }
            }

            let workload_key = format!("{}/{}", self.workload_key_prefix, workload.id);
            let mut alter_workload = TxnRequest::default();
            alter_workload
                .condition
                .push(TxnCondition::eq_seq(workload_key.clone(), seq));
            alter_workload
                .if_then
                .push(TxnOp::put(workload_key, serde_json::to_vec(&workload)?));

            if self
                .metastore
                .transaction(alter_workload)
                .await
                .map_err(meta_service_error)?
                .success
            {
                return Ok(());
            }
        }

        Err(ErrorCode::WorkloadOperateConflict(
            "Workload operate conflict(tried 5 times).",
        ))
    }

    async fn get_all(&self) -> Result<Vec<WorkloadGroup>> {
        let list_reply = self
            .metastore
            .list_kv_collect(ListOptions::unlimited(&format!(
                "{}/",
                self.workload_key_prefix
            )))
            .await
            .map_err(meta_service_error)?;

        let mut workload_groups = Vec::with_capacity(list_reply.len());

        for (_key, seq) in list_reply {
            workload_groups.push(serde_json::from_slice::<WorkloadGroup>(&seq.data)?);
        }

        Ok(workload_groups)
    }

    async fn get_by_id(&self, id: &str) -> Result<WorkloadGroup> {
        let Some(seq) = self.get_seq_by_id(id).await? else {
            return Err(ErrorCode::UnknownWorkload(format!(
                "Unknown workload id {}",
                id
            )));
        };

        Ok(seq.data)
    }

    async fn get_by_name(&self, name: &str) -> Result<WorkloadGroup> {
        let seq_value = self.get_seq_by_name(name).await?;
        Ok(seq_value.data)
    }
}
