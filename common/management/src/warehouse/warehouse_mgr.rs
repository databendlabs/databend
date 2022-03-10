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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_meta_api::KVApi;
use common_meta_types::IntoSeqV;
use common_meta_types::MatchSeq;
use common_meta_types::MatchSeqExt;
use common_meta_types::OkOrExist;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::UpsertKVAction;
use common_meta_types::WarehouseInfo;

use crate::warehouse::warehouse_api::WarehouseApi;

static WAREHOUSE_API_KEY_PREFIX: &str = "__fd_warehouses";

pub struct WarehouseMgr {
    kv_api: Arc<dyn KVApi>,
    warehouse_prefix: String,
    #[allow(dead_code)]
    tenant: String,
}

impl WarehouseMgr {
    pub fn create(kv_api: Arc<dyn KVApi>, tenant: &str) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while warehouse mgr create)",
            ));
        }

        Ok(WarehouseMgr {
            kv_api,
            warehouse_prefix: format!("{}/{}", WAREHOUSE_API_KEY_PREFIX, tenant),
            tenant: tenant.into(),
        })
    }
}

#[async_trait::async_trait]
impl WarehouseApi for WarehouseMgr {
    async fn create_warehouse(&self, warehouse_info: WarehouseInfo) -> Result<u64> {
        let match_seq = MatchSeq::Exact(0);
        let name = warehouse_info.meta.warehouse_name.clone();
        let key = format!("{}/{}", self.warehouse_prefix, name.clone());
        let value = serde_json::to_vec(&warehouse_info)?;
        let kv_api = self.kv_api.clone();
        let upsert_kv = kv_api.upsert_kv(UpsertKVAction::new(
            &key,
            match_seq,
            Operation::Update(value),
            None,
        ));
        let res = upsert_kv.await?.into_add_result()?;
        match res.res {
            OkOrExist::Ok(v) => Ok(v.seq),
            OkOrExist::Exists(v) => Err(ErrorCode::WarehouseAlreadyExists(format!(
                "Warehouse already exists, seq [{}]",
                v.seq
            ))),
        }
    }
    async fn get_warehouse(&self, name: &str, seq: Option<u64>) -> Result<SeqV<WarehouseInfo>> {
        let key = format!("{}/{}", self.warehouse_prefix, name);
        let kv_api = self.kv_api.clone();
        let res = kv_api.get_kv(&key).await?;
        let seq_value =
            res.ok_or_else(|| ErrorCode::UnknownWarehouse(format!("unknown warehouse {}", key)))?;

        match MatchSeq::from(seq).match_seq(&seq_value) {
            Ok(_) => Ok(seq_value.into_seqv()?),
            Err(_) => Err(ErrorCode::UnknownWarehouse(format!(
                "unknown warehouse {}",
                key
            ))),
        }
    }

    async fn get_warehouses(&self) -> Result<Vec<SeqV<WarehouseInfo>>> {
        let warehuose_prefx = self.warehouse_prefix.clone();
        let values = self.kv_api.prefix_list_kv(warehuose_prefx.as_str()).await?;
        let mut r = vec![];
        for (_key, val) in values {
            let u = serde_json::from_slice::<WarehouseInfo>(&val.data)
                .map_err_to_code(ErrorCode::IllegalWarehouseInfoFormat, || "")?;

            r.push(SeqV::new(val.seq, u));
        }
        Ok(r)
    }

    async fn update_warehouse_size(&self, name: &str, size: &str, seq: Option<u64>) -> Result<u64> {
        let warehouse_val_seq = self.get_warehouse(name, seq);
        let warehouse_info = warehouse_val_seq.await?.data;
        let mut new_warehouse_info = warehouse_info.clone();
        new_warehouse_info.meta.size = size.into();
        let key = format!("{}/{}", self.warehouse_prefix, name);
        let value = serde_json::to_vec(&new_warehouse_info)?;
        let match_seq = match seq {
            None => MatchSeq::GE(1),
            Some(s) => MatchSeq::Exact(s),
        };
        let res = self
            .kv_api
            .upsert_kv(UpsertKVAction::new(
                &key,
                match_seq,
                Operation::Update(value),
                None,
            ))
            .await?;
        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownWarehouse(format!(
                "unknown warehouse, or seq not match {}",
                name
            ))),
        }
    }

    async fn drop_warehouse(&self, name: &str, seq: Option<u64>) -> Result<()> {
        let key = format!("{}/{}", self.warehouse_prefix, name);
        let res = self
            .kv_api
            .upsert_kv(UpsertKVAction::new(
                &key,
                seq.into(),
                Operation::Delete,
                None,
            ))
            .await?;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownWarehouse(format!(
                "unknown warehouse {}",
                name
            )))
        }
    }
}
