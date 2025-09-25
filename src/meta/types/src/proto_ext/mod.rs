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

//! Extend protobuf generated code with some useful methods.

mod boolean_expression_ext;
mod conditional_operation_ext;
mod fetch_add_u64;
mod fetch_add_u64_response_ext;
mod put_sequential_ext;
mod raft_types_ext;
mod seq_v_ext;
mod snapshot_chunk_request_ext;
mod snapshot_v004_ext;
mod stream_item_ext;
mod transfer_leader_request_ext;
mod txn_condition_ext;
mod txn_get_request_ext;
mod txn_get_response_ext;
mod txn_op_ext;
mod txn_op_response_ext;
mod txn_reply_ext;
mod txn_request_ext;
mod watch_ext;

mod txn_delete_by_prefix_request_ext;
mod txn_delete_by_prefix_response_ext;
mod txn_delete_request_ext;
mod txn_delete_response_ext;
mod txn_put_request_ext;
mod txn_put_response_ext;
mod vote_request_ext;
mod vote_response_ext;
