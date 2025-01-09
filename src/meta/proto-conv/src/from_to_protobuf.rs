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

use std::fmt;
use std::sync::Arc;

/// Defines API to convert from/to protobuf meta type.
pub trait FromToProto {
    /// The corresponding protobuf defined type.
    type PB: prost::Message + Default;

    /// Get the version encoded in a protobuf message.
    fn get_pb_ver(p: &Self::PB) -> u64;

    /// Convert to rust type from protobuf type.
    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized;

    /// Convert from rust type to protobuf type.
    fn to_pb(&self) -> Result<Self::PB, Incompatible>;
}

/// Defines API to convert from/to protobuf Enumeration.
pub trait FromToProtoEnum {
    /// The corresponding protobuf defined type.
    type PBEnum;

    /// Convert to rust type from protobuf enum type.
    fn from_pb_enum(p: Self::PBEnum) -> Result<Self, Incompatible>
    where Self: Sized;

    /// Convert from rust type to protobuf type.
    fn to_pb_enum(&self) -> Result<Self::PBEnum, Incompatible>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct Incompatible {
    pub context: String,
    pub reason: String,
}

impl fmt::Display for Incompatible {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.reason)?;

        if !self.context.is_empty() {
            write!(f, "; when:({})", self.context)?;
        }
        Ok(())
    }
}

impl Incompatible {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            context: "".into(),
            reason: reason.into(),
        }
    }

    /// Set the context of the error.
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = context.into();
        self
    }
}

impl<T> FromToProto for Arc<T>
where T: FromToProto
{
    type PB = T::PB;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        T::get_pb_ver(p)
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Arc::new(T::from_pb(p)?))
    }

    fn to_pb(&self) -> Result<T::PB, Incompatible> {
        let s = self.as_ref();
        s.to_pb()
    }
}
