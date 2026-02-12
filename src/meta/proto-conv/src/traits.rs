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

use std::sync::Arc;

use crate::Incompatible;

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

impl<T> FromToProto for Box<T>
where T: FromToProto
{
    type PB = T::PB;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        T::get_pb_ver(p)
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Box::new(T::from_pb(p)?))
    }

    fn to_pb(&self) -> Result<T::PB, Incompatible> {
        self.as_ref().to_pb()
    }
}

pub trait ToProtoOptionExt {
    type PB;
    fn to_pb_opt(&self) -> Result<Option<Self::PB>, Incompatible>;
}

impl<T: FromToProto> ToProtoOptionExt for Option<T> {
    type PB = T::PB;
    fn to_pb_opt(&self) -> Result<Option<T::PB>, Incompatible> {
        self.as_ref().map(FromToProto::to_pb).transpose()
    }
}

#[allow(clippy::wrong_self_convention)]
pub trait FromProtoOptionExt<T: FromToProto> {
    fn from_pb_opt(self) -> Result<Option<T>, Incompatible>;
}

impl<T: FromToProto> FromProtoOptionExt<T> for Option<T::PB> {
    fn from_pb_opt(self) -> Result<Option<T>, Incompatible> {
        self.map(FromToProto::from_pb).transpose()
    }
}
