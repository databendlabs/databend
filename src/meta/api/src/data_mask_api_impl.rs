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

use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::data_mask::CreateDatamaskReply;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskId;
use databend_common_meta_app::data_mask::DataMaskIdIdent;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::MaskPolicyTableIdListIdent;
use databend_common_meta_app::data_mask::MaskpolicyTableIdList;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use log::debug;

use crate::data_mask_api::DatamaskApi;
use crate::fetch_id;
use crate::get_pb_value;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::send_txn;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_eq_seq;
use crate::util::txn_delete_exact;
use crate::util::txn_op_put_pb;
use crate::util::txn_replace_exact;

/// DatamaskApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls DatamaskApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> DatamaskApi for KV {
    async fn create_data_mask(
        &self,
        req: CreateDatamaskReq,
    ) -> Result<CreateDatamaskReply, KVAppError> {
        debug!(req :? =(&req); "DatamaskApi: {}", func_name!());

        let name_ident = &req.name;

        let mut trials = txn_backoff(None, func_name!());
        let id = loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let res = self.get_id_and_value(name_ident).await?;
            debug!(res :? = res, name_key :? =(name_ident); "create_data_mask");

            let mut curr_seq = 0;

            if let Some((seq_id, seq_meta)) = res {
                match req.create_option {
                    CreateOption::Create => {
                        return Err(AppError::DatamaskAlreadyExists(
                            name_ident.exist_error(func_name!()),
                        )
                        .into());
                    }
                    CreateOption::CreateIfNotExists => {
                        return Ok(CreateDatamaskReply { id: *seq_id.data });
                    }
                    CreateOption::CreateOrReplace => {
                        let id_ident = seq_id.data.into_t_ident(name_ident.tenant());

                        txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);

                        clear_table_column_mask_policy(self, name_ident, &mut txn).await?;

                        curr_seq = seq_id.seq;
                    }
                };
            }

            // Create data mask by inserting these record:
            // name -> id
            // id -> policy
            // data mask name -> data mask table id list

            let id = fetch_id(self, IdGenerator::data_mask_id()).await?;

            let id = DataMaskId::new(id);
            let id_ident = DataMaskIdIdent::new_generic(name_ident.tenant(), id);
            let id_list_key = MaskPolicyTableIdListIdent::new_from(name_ident.clone());

            debug!(
                id :? =(&id_ident),
                name_key :? =(name_ident);
                "new datamask id"
            );

            {
                let meta: DatamaskMeta = req.data_mask_meta.clone();
                let id_list = MaskpolicyTableIdList::default();
                txn.condition.push(txn_cond_eq_seq(name_ident, curr_seq));
                txn.if_then.extend(vec![
                    txn_op_put_pb(name_ident, &id)?,        // name -> db_id
                    txn_op_put_pb(&id_ident, &meta)?,       // id -> meta
                    txn_op_put_pb(&id_list_key, &id_list)?, // data mask name -> id_list
                ]);

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(name_ident),
                    id :? =(&id_ident),
                    succ = succ;
                    "create_data_mask"
                );

                if succ {
                    break id;
                }
            }
        };

        Ok(CreateDatamaskReply { id: *id })
    }

    async fn drop_data_mask(
        &self,
        name_ident: &DataMaskNameIdent,
    ) -> Result<Option<(SeqV<DataMaskId>, SeqV<DatamaskMeta>)>, KVAppError> {
        debug!(name_ident :? =(name_ident); "DatamaskApi: {}", func_name!());

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let res = self.get_id_and_value(name_ident).await?;
            debug!(res :? = res, name_key :? =(name_ident); "{}", func_name!());

            let Some((seq_id, seq_meta)) = res else {
                return Ok(None);
            };

            let id_ident = seq_id.data.into_t_ident(name_ident.tenant());

            txn_delete_exact(&mut txn, name_ident, seq_id.seq);
            txn_delete_exact(&mut txn, &id_ident, seq_meta.seq);

            clear_table_column_mask_policy(self, name_ident, &mut txn).await?;

            let (succ, _responses) = send_txn(self, txn).await?;
            debug!(succ = succ;"{}", func_name!());

            if succ {
                return Ok(Some((seq_id, seq_meta)));
            }
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

    // remove mask policy from table meta
    for table_id in seq_id_list.data.id_list.into_iter() {
        let tbid = TableId { table_id };

        let (tb_meta_seq, table_meta_opt): (_, Option<TableMeta>) =
            get_pb_value(kv_api, &tbid).await?;

        let Some(mut table_meta) = table_meta_opt else {
            continue;
        };

        if let Some(column_mask_policy) = table_meta.column_mask_policy {
            let new_column_mask_policy = column_mask_policy
                .into_iter()
                .filter(|(_, name)| name != name_ident.name())
                .collect();

            table_meta.column_mask_policy = Some(new_column_mask_policy);

            txn_replace_exact(txn, &tbid, tb_meta_seq, &table_meta)?;
        }
    }

    Ok(())
}
