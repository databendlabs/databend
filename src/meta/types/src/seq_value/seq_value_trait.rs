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

use crate::seq_value::kv_meta::KVMeta;
use crate::EvalExpireTime;

pub trait SeqValue<V = Vec<u8>> {
    fn seq(&self) -> u64;
    fn value(&self) -> Option<&V>;
    fn into_value(self) -> Option<V>;
    fn meta(&self) -> Option<&KVMeta>;

    /// Return the expire time in millisecond since 1970.
    fn get_expire_at_ms(&self) -> Option<u64> {
        if let Some(meta) = self.meta() {
            meta.get_expire_at_ms()
        } else {
            None
        }
    }

    /// Evaluate and returns the absolute expire time in millisecond since 1970.
    fn eval_expire_at_ms(&self) -> u64 {
        self.meta().eval_expire_at_ms()
    }

    /// Return true if the record is expired.
    fn is_expired(&self, now_ms: u64) -> bool {
        self.eval_expire_at_ms() < now_ms
    }
}
