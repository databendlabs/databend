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

mod expire_key;
mod expire_value;
mod kv_meta;
mod state_machine_api;
pub mod time_util;
mod user_key;

pub use expire_key::ExpireKey;
pub use expire_value::ExpireValue;
pub use kv_meta::KVMeta;
pub use map_api::SeqValue;
pub use state_machine_api::StateMachineApi;
pub use user_key::UserKey;

pub type SeqV<T = Vec<u8>> = map_api::SeqV<KVMeta, T>;

pub type MetaValue<T = Vec<u8>> = (Option<KVMeta>, T);
