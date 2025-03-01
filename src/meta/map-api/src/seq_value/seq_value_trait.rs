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

use crate::expirable::Expirable;

pub trait SeqValue<M, V = Vec<u8>> {
    fn seq(&self) -> u64;
    fn value(&self) -> Option<&V>;
    fn into_value(self) -> Option<V>;
    fn meta(&self) -> Option<&M>;

    fn unpack(self) -> (u64, Option<V>)
    where Self: Sized {
        (self.seq(), self.into_value())
    }

    /// Return the expire time in millisecond since 1970.
    fn expires_at_ms_opt(&self) -> Option<u64>
    where M: Expirable {
        let meta = self.meta()?;
        meta.expires_at_ms_opt()
    }

    /// Evaluate and returns the absolute expire time in millisecond since 1970.
    fn expires_at_ms(&self) -> u64
    where M: Expirable {
        self.meta().expires_at_ms()
    }

    /// Return true if the record is expired.
    fn is_expired(&self, now_ms: u64) -> bool
    where M: Expirable {
        self.expires_at_ms() < now_ms
    }
}
