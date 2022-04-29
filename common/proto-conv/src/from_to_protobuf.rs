// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

/// Defines API to convert from/to protobuf meta type.
pub trait FromToProto<PB> {
    fn from_pb(p: PB) -> Result<Self, Incompatible>
    where Self: Sized;

    fn to_pb(&self) -> Result<PB, Incompatible>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Incompatible: {reason}")]
pub struct Incompatible {
    pub reason: String,
}

impl<T, PB> FromToProto<PB> for Arc<T>
where T: FromToProto<PB>
{
    fn from_pb(p: PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Arc::new(T::from_pb(p)?))
    }

    fn to_pb(&self) -> Result<PB, Incompatible> {
        let s = self.as_ref();
        s.to_pb()
    }
}
