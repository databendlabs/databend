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

use std::fmt::Display;

use common_meta_app::app_error::AppError;
use common_meta_app::app_error::DatamaskAlreadyExists;
use common_meta_app::app_error::UnknownDatamask;
use common_meta_app::data_mask::CreateDatamaskReply;
use common_meta_app::data_mask::CreateDatamaskReq;
use common_meta_app::data_mask::DatamaskId;
use common_meta_app::data_mask::DatamaskMeta;
use common_meta_app::data_mask::DatamaskNameIdent;
use common_meta_app::data_mask::DropDatamaskReply;
use common_meta_app::data_mask::DropDatamaskReq;
use common_meta_app::data_mask::GetDatamaskReply;
use common_meta_app::data_mask::GetDatamaskReq;
use common_meta_app::data_mask::MaskpolicyTableIdList;
use common_meta_app::data_mask::MaskpolicyTableIdListKey;
use common_meta_app::schema::TableId;
use common_meta_app::schema::TableMeta;
use common_meta_kvapi::kvapi;
use common_meta_types::ConditionResult::Eq;
use common_meta_types::MetaError;
use common_meta_types::TxnCondition;
use common_meta_types::TxnOp;
use common_meta_types::TxnRequest;
use common_tracing::func_name;
use log::as_debug;
use log::debug;

use crate::data_mask_api::DatamaskApi;
use crate::fetch_id;
use crate::get_pb_value;
use crate::get_u64_value;
use crate::id_generator::IdGenerator;
use crate::kv_app_error::KVAppError;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_put;
use crate::util::txn_trials;

/// DatamaskApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls DatamaskApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError>> DatamaskApi for KV {
    async fn create_data_mask(
        &self,
        req: CreateDatamaskReq,
    ) -> Result<CreateDatamaskReply, KVAppError> {
        debug!(req = as_debug!(&req); "DatamaskApi: {}", func_name!());

        let name_key = &req.name;

        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);
        let id = loop {
            trials.next().unwrap()?;
            // Get db mask by name to ensure absence
            let (seq, id) = get_u64_value(self, name_key).await?;
            debug!(seq = seq, id = id, name_key = as_debug!(name_key); "create_data_mask");

            if seq > 0 {
                return if req.if_not_exists {
                    Ok(CreateDatamaskReply { id })
                } else {
                    Err(KVAppError::AppError(AppError::DatamaskAlreadyExists(
                        DatamaskAlreadyExists::new(
                            &name_key.name,
                            format!("create data mask: {}", req.name),
                        ),
                    )))
                };
            }

            // Create data mask by inserting these record:
            // name -> id
            // id -> policy
            // data mask name -> data mask table id list

            let id = fetch_id(self, IdGenerator::data_mask_id()).await?;
            let id_key = DatamaskId { id };
            let id_list_key = MaskpolicyTableIdListKey {
                tenant: name_key.tenant.clone(),
                name: name_key.name.clone(),
            };

            debug!(
                id = as_debug!(&id_key),
                name_key = as_debug!(name_key);
                "new datamask id"
            );

            {
                let meta: DatamaskMeta = req.clone().into();
                let id_list = MaskpolicyTableIdList::default();
                let condition = vec![txn_cond_seq(name_key, Eq, 0)];
                let if_then = vec![
                    txn_op_put(name_key, serialize_u64(id)?), // name -> db_id
                    txn_op_put(&id_key, serialize_struct(&meta)?), // id -> meta
                    txn_op_put(&id_list_key, serialize_struct(&id_list)?), /* data mask name -> id_list */
                ];

                let txn_req = TxnRequest {
                    condition,
                    if_then,
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name = as_debug!(name_key),
                    id = as_debug!(&id_key),
                    succ = succ;
                    "create_data_mask"
                );

                if succ {
                    break id;
                }
            }
        };

        Ok(CreateDatamaskReply { id })
    }

    async fn drop_data_mask(&self, req: DropDatamaskReq) -> Result<DropDatamaskReply, KVAppError> {
        debug!(req = as_debug!(&req); "DatamaskApi: {}", func_name!());

        let name_key = &req.name;
        let ctx = &func_name!();
        let mut trials = txn_trials(None, ctx);

        loop {
            trials.next().unwrap()?;

            let result =
                get_data_mask_or_err(self, name_key, format!("drop_data_mask: {}", name_key)).await;

            let (id_seq, id, data_mask_seq, _) = match result {
                Ok((id_seq, id, data_mask_seq, meta)) => (id_seq, id, data_mask_seq, meta),
                Err(err) => {
                    if let KVAppError::AppError(AppError::UnknownDatamask(_)) = err {
                        if req.if_exists {
                            return Ok(DropDatamaskReply {});
                        }
                    }

                    return Err(err);
                }
            };
            let id_key = DatamaskId { id };
            let mut condition = vec![
                txn_cond_seq(name_key, Eq, id_seq),
                txn_cond_seq(&id_key, Eq, data_mask_seq),
            ];
            let mut if_then = vec![txn_op_del(name_key), txn_op_del(&id_key)];

            clear_table_column_mask_policy(self, name_key, &mut condition, &mut if_then).await?;

            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                name = as_debug!(name_key),
                id = as_debug!(&DatamaskId { id }),
                succ = succ;
                "drop_data_mask"
            );

            if succ {
                break;
            }
        }

        Ok(DropDatamaskReply {})
    }

    async fn get_data_mask(&self, req: GetDatamaskReq) -> Result<GetDatamaskReply, KVAppError> {
        debug!(req = as_debug!(&req); "DatamaskApi: {}", func_name!());

        let name_key = &req.name;

        let (_id_seq, _id, _data_mask_seq, policy) =
            get_data_mask_or_err(self, name_key, format!("drop_data_mask: {}", name_key)).await?;

        Ok(GetDatamaskReply { policy })
    }
}

/// Returns (id_seq, id, data_mask_seq, data_mask)
async fn get_data_mask_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &DatamaskNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, DatamaskMeta), KVAppError> {
    let (id_seq, id) = get_u64_value(kv_api, name_key).await?;
    data_mask_has_to_exist(id_seq, name_key, &msg)?;

    let id_key = DatamaskId { id };

    let (data_mask_seq, data_mask) = get_pb_value(kv_api, &id_key).await?;
    data_mask_has_to_exist(data_mask_seq, name_key, msg)?;

    Ok((
        id_seq,
        id,
        data_mask_seq,
        // Safe unwrap(): data_mask_seq > 0 implies data_mask is not None.
        data_mask.unwrap(),
    ))
}

/// Return OK if a db_id or db_meta exists by checking the seq.
///
/// Otherwise returns UnknownDatamask error
pub fn data_mask_has_to_exist(
    seq: u64,
    name_ident: &DatamaskNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, name_ident = as_debug!(name_ident); "data mask does not exist");

        Err(KVAppError::AppError(AppError::UnknownDatamask(
            UnknownDatamask::new(&name_ident.name, format!("{}: {}", msg, name_ident)),
        )))
    } else {
        Ok(())
    }
}

async fn clear_table_column_mask_policy(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_ident: &DatamaskNameIdent,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    let id_list_key = MaskpolicyTableIdListKey {
        tenant: name_ident.tenant.clone(),
        name: name_ident.name.clone(),
    };
    let (id_list_seq, id_list_opt): (_, Option<MaskpolicyTableIdList>) =
        get_pb_value(kv_api, &id_list_key).await?;
    if let Some(id_list) = id_list_opt {
        condition.push(txn_cond_seq(&id_list_key, Eq, id_list_seq));
        if_then.push(txn_op_del(&id_list_key));

        // remove mask policy from table meta
        for table_id in id_list.id_list.into_iter() {
            let tbid = TableId { table_id };

            let (tb_meta_seq, table_meta_opt): (_, Option<TableMeta>) =
                get_pb_value(kv_api, &tbid).await?;
            if let Some(mut table_meta) = table_meta_opt {
                if let Some(column_mask_policy) = table_meta.column_mask_policy {
                    let new_column_mask_policy = column_mask_policy
                        .into_iter()
                        .filter(|(_, name)| name != &name_ident.name)
                        .collect();

                    table_meta.column_mask_policy = Some(new_column_mask_policy);

                    condition.push(txn_cond_seq(&tbid, Eq, tb_meta_seq));
                    if_then.push(txn_op_put(&tbid, serialize_struct(&table_meta)?));
                }
            }
        }
    }

    Ok(())
}
