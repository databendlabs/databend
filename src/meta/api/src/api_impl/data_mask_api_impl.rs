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

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::data_mask::CreateDatamaskReply;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskId;
use databend_common_meta_app::data_mask::DataMaskIdIdent;
use databend_common_meta_app::data_mask::DataMaskIdToNameIdent;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DataMaskNameIdentRaw;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
use databend_common_meta_app::data_mask::MaskPolicyTableIdListIdent;
use databend_common_meta_app::data_mask::MaskpolicyTableIdList;
use databend_common_meta_app::data_mask::data_mask_name_ident;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::row_access_policy::RowAccessPolicyNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::ExistError;
use databend_meta_kvapi::kvapi;
use databend_meta_kvapi::kvapi::DirName;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_types::MetaError;
use databend_meta_types::SeqV;
use databend_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use super::data_mask_api::DatamaskApi;
use super::errors::MaskingPolicyError;
use crate::fetch_id;
use crate::kv_pb_api::KVPbApi;
use crate::meta_txn_error::MetaTxnError;
use crate::txn_backoff::txn_backoff;
use crate::txn_condition_util::txn_cond_eq_keys_with_prefix;
use crate::txn_condition_util::txn_cond_eq_seq;
use crate::txn_core_util::send_txn;
use crate::txn_core_util::txn_delete_exact;
use crate::txn_op_builder_util::txn_del;
use crate::txn_op_builder_util::txn_put_pb_with_ttl;

/// DatamaskApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls DatamaskApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> DatamaskApi for KV {
    async fn create_data_mask(
        &self,
        req: CreateDatamaskReq,
    ) -> Result<Result<CreateDatamaskReply, ExistError<data_mask_name_ident::Resource>>, MetaError>
    {
        debug!(req :? =(&req); "DatamaskApi: {}", func_name!());

        let name_ident = &req.name;

        let row_access_name_ident = RowAccessPolicyNameIdent::new(
            name_ident.tenant().clone(),
            name_ident.data_mask_name().to_string(),
        );

        let masking_policy_id = fetch_id(self, IdGenerator::data_mask_id()).await?;

        let mut txn = TxnRequest::default();

        // Create data mask by inserting these record:
        // name -> id
        // id -> policy
        // data mask name -> data mask table id list

        let id = DataMaskId::new(masking_policy_id);
        let id_ident = DataMaskIdIdent::new_generic(name_ident.tenant(), id);
        let id_to_name_ident = DataMaskIdToNameIdent::new_generic(name_ident.tenant(), id);
        let name_raw = DataMaskNameIdentRaw::from(name_ident.clone());
        let id_list_key = MaskPolicyTableIdListIdent::new_from(name_ident.clone());

        debug!(
            id :? =(&id_ident),
            name_key :? =(name_ident);
            "new datamask id"
        );

        {
            let meta: DatamaskMeta = req.data_mask_meta.clone();
            let id_list = MaskpolicyTableIdList::default();
            txn.condition.push(txn_cond_eq_seq(name_ident, 0));
            txn.condition
                .push(txn_cond_eq_seq(&row_access_name_ident, 0));

            txn.if_then.extend(vec![
                txn_put_pb_with_ttl(name_ident, &id, None)?, // name -> masking_policy_id
                txn_put_pb_with_ttl(&id_ident, &meta, None)?, // id -> meta
                txn_put_pb_with_ttl(&id_to_name_ident, &name_raw, None)?, // id -> name
                // TODO: Tentative retention for compatibility MaskPolicyTableIdListIdent related logic. It can be directly deleted later
                txn_put_pb_with_ttl(&id_list_key, &id_list, None)?, // data mask name -> id_list
            ]);
        }

        let (succ, _responses) = send_txn(self, txn).await?;

        debug!(
            name :? =(name_ident),
            id :? =(&id_ident),
            succ = succ;
            "create_data_mask"
        );

        if succ {
            Ok(Ok(CreateDatamaskReply {
                id: masking_policy_id,
            }))
        } else {
            Ok(Err(
                name_ident.exist_error(format!("{} already exists", req.name))
            ))
        }
    }

    async fn drop_data_mask(
        &self,
        name_ident: &DataMaskNameIdent,
    ) -> Result<
        Result<Option<(SeqV<DataMaskId>, SeqV<DatamaskMeta>)>, MaskingPolicyError>,
        MetaTxnError,
    > {
        debug!(name_ident :? =(name_ident); "DatamaskApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Check if policy exists
            let res = self.get_id_and_value(name_ident).await?;
            let Some((seq_id, seq_meta)) = res else {
                return Ok(Ok(None));
            };

            let policy_id = *seq_id.data;
            let tenant = name_ident.tenant();
            let table_policy_ref_prefix = DirName::new(MaskPolicyTableIdIdent::new_generic(
                tenant.clone(),
                MaskPolicyIdTableId {
                    policy_id,
                    table_id: 0,
                },
            ));

            // List all table-policy references
            let table_policy_refs = self
                .list_pb_vec(ListOptions::unlimited(&table_policy_ref_prefix))
                .await?;

            // Policy is in use - cannot drop
            if !table_policy_refs.is_empty() {
                return Ok(Err(MaskingPolicyError::policy_in_use(
                    name_ident.data_mask_name().to_string(),
                )));
            }

            // No references - drop the policy
            let id_ident = seq_id.data.into_t_ident(tenant.clone());
            let id_to_name_ident =
                DataMaskIdToNameIdent::new_generic(tenant, DataMaskId::new(policy_id));
            let mut txn = TxnRequest::default();

            // Ensure no new references were created
            txn.condition
                .push(txn_cond_eq_keys_with_prefix(&table_policy_ref_prefix, 0));

            txn_delete_exact(&mut txn, name_ident, seq_id.seq);
            txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);
            txn.if_then.push(txn_del(&id_to_name_ident));

            // TODO: Tentative retention for compatibility. Can be deleted later.
            clear_table_column_mask_policy(self, name_ident, &mut txn).await?;

            let (succ, _responses) = send_txn(self, txn).await?;
            if succ {
                return Ok(Ok(Some((seq_id, seq_meta))));
            }
            // Transaction failed, retry
        }
    }

    async fn get_data_mask(
        &self,
        name_ident: &DataMaskNameIdent,
    ) -> Result<Option<SeqV<DatamaskMeta>>, MetaError> {
        debug!(req :? =(&name_ident); "DatamaskApi: {}", func_name!());

        let res = self.get_id_and_value(name_ident).await?;

        Ok(res.map(|(_, seq_meta)| seq_meta))
    }

    async fn get_data_mask_id(
        &self,
        name_ident: &DataMaskNameIdent,
    ) -> Result<Option<SeqV<DataMaskId>>, MetaError> {
        self.get_pb(name_ident).await
    }

    async fn get_data_mask_name_by_id(
        &self,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<Option<String>, MetaError> {
        let ident = DataMaskIdToNameIdent::new_generic(tenant.clone(), DataMaskId::new(policy_id));
        let seq_meta = self.get_pb(&ident).await?;

        debug!(ident :% =(&ident); "get_data_mask_name_by_id");

        Ok(seq_meta.map(|s| s.data.data_mask_name().to_string()))
    }

    async fn get_data_mask_by_id(
        &self,
        tenant: &Tenant,
        policy_id: u64,
    ) -> Result<Option<SeqV<DatamaskMeta>>, MetaError> {
        debug!(req :? =(policy_id); "DatamaskApi: {}", func_name!());

        let id = DataMaskId::new(policy_id);
        let id_ident = DataMaskIdIdent::new_generic(tenant, id);

        let res = self.get_pb(&id_ident).await?;
        Ok(res)
    }
}

async fn clear_table_column_mask_policy(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_ident: &DataMaskNameIdent,
    txn: &mut TxnRequest,
) -> Result<(), MetaError> {
    let id_list_key = MaskPolicyTableIdListIdent::new_from(name_ident.clone());

    let seq_id_list = kv_api.get_pb(&id_list_key).await?;

    let Some(seq_id_list) = seq_id_list else {
        return Ok(());
    };

    txn_delete_exact(txn, &id_list_key, seq_id_list.seq);
    Ok(())
}
