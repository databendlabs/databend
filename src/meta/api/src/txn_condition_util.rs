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

//! Transaction condition utilities extracted from util.rs

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_types::txn_condition::Target;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::TxnCondition;

/// Build a TxnCondition that compares the seq of a record.
pub fn txn_cond_eq_seq(key: &impl kvapi::Key, seq: u64) -> TxnCondition {
    TxnCondition::eq_seq(key.to_string_key(), seq)
}

/// Build a TxnCondition that compares the seq of a record.
pub fn txn_cond_seq(key: &impl kvapi::Key, op: ConditionResult, seq: u64) -> TxnCondition {
    TxnCondition {
        key: key.to_string_key(),
        expected: op as i32,
        target: Some(Target::Seq(seq)),
    }
}
