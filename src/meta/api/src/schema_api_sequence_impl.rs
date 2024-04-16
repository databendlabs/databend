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

use chrono::Utc;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::OutofSequenceRange;
use databend_common_meta_app::app_error::SequenceAlreadyExists;
use databend_common_meta_app::app_error::UnknownSequence;
use databend_common_meta_app::app_error::WrongSequenceCount;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceId;
use databend_common_meta_app::schema::SequenceMeta;
use databend_common_meta_app::schema::SequenceNameIdent;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::ConditionResult::Eq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use log::debug;
use minitrace::func_name;

use crate::fetch_id;
use crate::get_pb_value;
use crate::get_u64_value;
use crate::kv_app_error::KVAppError;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_put;

pub async fn do_create_sequence(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    req: CreateSequenceReq,
) -> Result<CreateSequenceReply, KVAppError> {
    debug!(req :? =(&req); "SchemaApi: {}", func_name!());

    let name_key = &req.name_ident;

    let mut trials = txn_backoff(None, func_name!());
    let id = loop {
        trials.next().unwrap()?.await;

        // Get db mask by name to ensure absence
        let (seq, id) = get_u64_value(kv_api, name_key).await?;
        debug!(seq = seq, id = id, name_key :? =(name_key); "create_sequence");

        let mut condition = vec![];
        let mut if_then = vec![];

        if seq > 0 {
            match req.create_option {
                CreateOption::Create => {
                    return Err(KVAppError::AppError(AppError::SequenceAlreadyExists(
                        SequenceAlreadyExists::new(
                            name_key.sequence_name.clone(),
                            format!("create sequence: {:?}", name_key),
                        ),
                    )));
                }
                CreateOption::CreateIfNotExists => return Ok(CreateSequenceReply { id }),
                CreateOption::CreateOrReplace => {
                    construct_drop_sequence_operations(
                        kv_api,
                        name_key,
                        false,
                        false,
                        func_name!(),
                        &mut condition,
                        &mut if_then,
                    )
                    .await?;
                }
            };
        };

        let id = fetch_id(kv_api, IdGenerator::sequence_id()).await?;
        let id_key = SequenceId { id };

        debug!(
            id :? =(&id_key),
            name_key :? =(name_key);
            "new sequence id"
        );

        {
            let meta: SequenceMeta = req.clone().into();
            condition.push(txn_cond_seq(name_key, Eq, seq));
            if_then.extend(vec![
                txn_op_put(name_key, serialize_u64(id)?), // name -> db_id
                txn_op_put(&id_key, serialize_struct(&meta)?), // id -> meta
            ]);

            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(kv_api, txn_req).await?;

            debug!(
                name :? =(name_key),
                id :? =(&id_key),
                succ = succ;
                "create_sequence"
            );

            if succ {
                break id;
            }
        }
    };

    Ok(CreateSequenceReply { id })
}

pub async fn do_get_sequence(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    req: GetSequenceReq,
) -> Result<GetSequenceReply, KVAppError> {
    debug!(req :? =(&req); "SchemaApi: {}", func_name!());
    let name_key = &req.name_ident;

    let result = get_sequence_or_err(
        kv_api,
        name_key,
        format!("get_sequence_next_values: {:?}", name_key),
    )
    .await;

    let (_id_seq, id, _sequence_seq, sequence_meta) = match result {
        Ok((id_seq, id, sequence_seq, meta)) => (id_seq, id, sequence_seq, meta),
        Err(err) => {
            return Err(err);
        }
    };

    Ok(GetSequenceReply {
        id,
        meta: sequence_meta,
    })
}

pub async fn do_get_sequence_next_value(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    req: GetSequenceNextValueReq,
) -> Result<GetSequenceNextValueReply, KVAppError> {
    debug!(req :? =(&req); "SchemaApi: {}", func_name!());

    let name_key = &req.name_ident;
    if req.count == 0 {
        return Err(KVAppError::AppError(AppError::WrongSequenceCount(
            WrongSequenceCount::new(name_key.sequence_name.clone()),
        )));
    }

    let mut trials = txn_backoff(None, func_name!());
    loop {
        trials.next().unwrap()?.await;
        let result = get_sequence_or_err(
            kv_api,
            name_key,
            format!("get_sequence_next_values: {:?}", name_key),
        )
        .await;

        let (id_seq, id, sequence_seq, mut sequence_meta) = match result {
            Ok((id_seq, id, sequence_seq, meta)) => (id_seq, id, sequence_seq, meta),
            Err(err) => {
                return Err(err);
            }
        };
        let start = sequence_meta.current;
        let count = req.count;
        if u64::MAX - sequence_meta.current < count {
            return Err(KVAppError::AppError(AppError::OutofSequenceRange(
                OutofSequenceRange::new(
                    name_key.sequence_name.clone(),
                    format!(
                        "{}: {:?}",
                        format!("current: {}, count: {}", sequence_meta.current, count),
                        name_key
                    ),
                ),
            )));
        }

        sequence_meta.current += count;
        sequence_meta.update_on = Utc::now();
        let id_key = SequenceId { id };
        let condition = vec![
            txn_cond_seq(name_key, Eq, id_seq),
            txn_cond_seq(&id_key, Eq, sequence_seq),
        ];
        let if_then = vec![
            txn_op_put(name_key, serialize_u64(id)?), // name -> db_id
            txn_op_put(&id_key, serialize_struct(&sequence_meta)?), // id -> meta
        ];

        debug!(
            current :? =(&sequence_meta.current),
            name_key :? =(name_key);
            "get_sequence_next_values"
        );

        let txn_req = TxnRequest {
            condition,
            if_then,
            else_then: vec![],
        };

        let (succ, _responses) = send_txn(kv_api, txn_req).await?;

        debug!(
            current :? =(&sequence_meta.current),
            name_key :? =(name_key),
            succ = succ;
            "get_sequence_next_values"
        );
        if succ {
            return Ok(GetSequenceNextValueReply {
                id,
                start,
                step: sequence_meta.step,
                end: sequence_meta.current - 1,
            });
        }
    }
}

pub async fn do_drop_sequence(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    req: DropSequenceReq,
) -> Result<DropSequenceReply, KVAppError> {
    debug!(req :? =(&req); "SchemaApi: {}", func_name!());

    let name_key = &req.name_ident;

    let mut trials = txn_backoff(None, func_name!());
    loop {
        trials.next().unwrap()?.await;

        let mut condition = vec![];
        let mut if_then = vec![];

        construct_drop_sequence_operations(
            kv_api,
            name_key,
            req.if_exists,
            true,
            func_name!(),
            &mut condition,
            &mut if_then,
        )
        .await?;

        let txn_req = TxnRequest {
            condition,
            if_then,
            else_then: vec![],
        };

        let (succ, _responses) = send_txn(kv_api, txn_req).await?;

        debug!(
            name :? =(name_key),
            succ = succ;
            "drop_sequence"
        );

        if succ {
            break;
        }
    }
    Ok(DropSequenceReply {})
}

/// Return OK if a db_id or sequence_meta exists by checking the seq.
///
/// Otherwise returns UnknownDatamask error
fn sequence_has_to_exist(
    seq: u64,
    name_ident: &SequenceNameIdent,
    msg: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        debug!(seq = seq, name_ident :? =(name_ident); "sequence does not exist");

        Err(KVAppError::AppError(AppError::UnknownSequence(
            UnknownSequence::new(
                name_ident.sequence_name.clone(),
                format!("{}: {:?}", msg, name_ident),
            ),
        )))
    } else {
        Ok(())
    }
}

/// Returns (id_seq, id, seq, sequence_meta)
async fn get_sequence_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &SequenceNameIdent,
    msg: impl Display,
) -> Result<(u64, u64, u64, SequenceMeta), KVAppError> {
    let (id_seq, id) = get_u64_value(kv_api, name_key).await?;
    sequence_has_to_exist(id_seq, name_key, &msg)?;

    let id_key = SequenceId { id };

    let (sequence_seq, sequence_meta) = get_pb_value(kv_api, &id_key).await?;
    sequence_has_to_exist(sequence_seq, name_key, msg)?;

    Ok((id_seq, id, sequence_seq, sequence_meta.unwrap()))
}

async fn construct_drop_sequence_operations(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &SequenceNameIdent,
    drop_if_exists: bool,
    if_delete: bool,
    ctx: &str,
    condition: &mut Vec<TxnCondition>,
    if_then: &mut Vec<TxnOp>,
) -> Result<(), KVAppError> {
    let result =
        get_sequence_or_err(kv_api, name_key, format!("drop_sequence: {:?}", name_key)).await;

    let (id_seq, id, sequence_seq, _) = match result {
        Ok((id_seq, id, sequence_seq, meta)) => (id_seq, id, sequence_seq, meta),
        Err(err) => {
            if let KVAppError::AppError(AppError::UnknownSequence(_)) = err {
                if drop_if_exists {
                    return Ok(());
                }
            }

            return Err(err);
        }
    };
    let id_key = SequenceId { id };

    condition.push(txn_cond_seq(&id_key, Eq, sequence_seq));
    if_then.push(txn_op_del(&id_key));

    if if_delete {
        condition.push(txn_cond_seq(name_key, Eq, id_seq));
        if_then.push(txn_op_del(name_key));
    }

    debug!(
        name :? =(name_key),
        id :? =(&SequenceId { id }),
        ctx = ctx;
        "construct_drop_sequence_operations"
    );

    Ok(())
}
