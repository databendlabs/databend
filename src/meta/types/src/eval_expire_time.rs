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

/// A trait for evaluating and returning the absolute expiration time.
pub trait Expirable {
    /// Evaluates and returns the absolute expiration time in milliseconds since the Unix epoch (January 1, 1970).
    ///
    /// If there is no expiration time, it returns `u64::MAX`.
    fn expires_at_ms(&self) -> u64;
}

impl<T> Expirable for &T
where T: Expirable
{
    fn expires_at_ms(&self) -> u64 {
        Expirable::expires_at_ms(*self)
    }
}

impl<T> Expirable for Option<T>
where T: Expirable
{
    fn expires_at_ms(&self) -> u64 {
        self.as_ref().map(|m| m.expires_at_ms()).unwrap_or(u64::MAX)
    }
}
