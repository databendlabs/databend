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

/// Evaluate and returns the absolute expire time.
pub trait EvalExpireTime {
    /// Evaluate and returns the absolute expire time in millisecond since 1970.
    ///
    /// If there is no expire time, return u64::MAX.
    fn eval_expire_at_ms(&self) -> u64;
}

impl<T> EvalExpireTime for &T
where T: EvalExpireTime
{
    fn eval_expire_at_ms(&self) -> u64 {
        EvalExpireTime::eval_expire_at_ms(*self)
    }
}

impl<T> EvalExpireTime for Option<T>
where T: EvalExpireTime
{
    fn eval_expire_at_ms(&self) -> u64 {
        self.as_ref()
            .map(|m| m.eval_expire_at_ms())
            .unwrap_or(u64::MAX)
    }
}
