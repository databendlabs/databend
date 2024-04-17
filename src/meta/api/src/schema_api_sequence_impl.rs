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
use databend_common_meta_app::app_error::CreateSequenceError;
use databend_common_meta_app::app_error::OutofSequenceRange;
use databend_common_meta_app::app_error::SequenceAlreadyExists;
use databend_common_meta_app::app_error::SequenceError;
use databend_common_meta_app::app_error::UnknownSequence;
use databend_common_meta_app::app_error::WrongSequenceCount;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceMeta;
use databend_common_meta_app::schema::SequenceNameIdent;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::ConditionResult::Eq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::TxnRequest;
use log::debug;
use minitrace::func_name;

use crate::get_pb_value;
use crate::kv_app_error::KVAppError;
use crate::send_txn;
use crate::serialize_struct;
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
    let meta: SequenceMeta = req.clone().into();

    let mut trials = txn_backoff(None, func_name!());
    let mut match_seq = MatchSeq::Exact(0);
    loop {
        trials.next().unwrap()?.await;
        let reply = kv_api
            .upsert_kv(UpsertKVReq::new(
                &name_key.to_string_key(),
                match_seq,
                Operation::Update(serialize_struct(&meta)?),
                None,
            ))
            .await?;

        debug!(
            name :? =(name_key),
            prev :? = (reply.prev),
            is_changed = reply.is_changed();
            "create_sequence"
        );

        if !reply.is_changed() {
            match req.create_option {
                CreateOption::Create => {
                    return Err(KVAppError::AppError(AppError::SequenceError(
                        SequenceError::SequenceAlreadyExists(SequenceAlreadyExists::new(
                            name_key.sequence_name.clone(),
                            format!("create sequence: {:?}", name_key),
                        )),
                    )));
                }
                CreateOption::CreateIfNotExists => break,
                CreateOption::CreateOrReplace => {
                    if let Some(prev) = reply.prev {
                        match_seq = MatchSeq::Exact(prev.seq);
                    } else {
                        return Err(KVAppError::AppError(AppError::SequenceError(
                            SequenceError::CreateSequenceError(CreateSequenceError::new(
                                name_key.sequence_name.clone(),
                                format!("replace sequence: {:?} but has no prev seq", name_key),
                            )),
                        )));
                    }
                }
            };
        } else {
            break;
        }
    }
    Ok(CreateSequenceReply {})
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

    let (_sequence_seq, sequence_meta) = match result {
        Ok((sequence_seq, meta)) => (sequence_seq, meta),
        Err(err) => {
            return Err(err);
        }
    };

    Ok(GetSequenceReply {
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
        return Err(KVAppError::AppError(AppError::SequenceError(
            SequenceError::WrongSequenceCount(WrongSequenceCount::new(
                name_key.sequence_name.clone(),
            )),
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

        let (sequence_seq, mut sequence_meta) = match result {
            Ok((sequence_seq, meta)) => (sequence_seq, meta),
            Err(err) => {
                return Err(err);
            }
        };
        let start = sequence_meta.current;
        let count = req.count;
        if u64::MAX - sequence_meta.current < count {
            return Err(KVAppError::AppError(AppError::SequenceError(
                SequenceError::OutofSequenceRange(OutofSequenceRange::new(
                    name_key.sequence_name.clone(),
                    format!(
                        "{:?}: current: {}, count: {}",
                        name_key, sequence_meta.current, count
                    ),
                )),
            )));
        }

        // update meta
        sequence_meta.current += count;
        sequence_meta.update_on = Utc::now();

        let condition = vec![txn_cond_seq(name_key, Eq, sequence_seq)];
        let if_then = vec![
            txn_op_put(name_key, serialize_struct(&sequence_meta)?), // name -> meta
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

        let result =
            get_sequence_or_err(kv_api, name_key, format!("drop_sequence: {:?}", name_key)).await;

        let (sequence_seq, _sequence_meta) = match result {
            Ok((sequence_seq, meta)) => (sequence_seq, meta),
            Err(err) => {
                if req.if_exists {
                    return Ok(DropSequenceReply {});
                }
                return Err(err);
            }
        };

        let condition = vec![txn_cond_seq(name_key, Eq, sequence_seq)];
        let if_then = vec![txn_op_del(name_key)];

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

/// Returns (seq, sequence_meta)
async fn get_sequence_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &SequenceNameIdent,
    msg: impl Display,
) -> Result<(u64, SequenceMeta), KVAppError> {
    let (sequence_seq, sequence_meta) = get_pb_value(kv_api, name_key).await?;

    if sequence_seq == 0 {
        debug!(seq = sequence_seq, name_ident :? =(name_key); "sequence does not exist");

        Err(KVAppError::AppError(AppError::SequenceError(
            SequenceError::UnknownSequence(UnknownSequence::new(
                name_key.sequence_name.clone(),
                format!("{}: {:?}", msg, name_key),
            )),
        )))
    } else {
        Ok((sequence_seq, sequence_meta.unwrap()))
    }
}
