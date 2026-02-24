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
use std::ops::Deref;
use std::ops::DerefMut;

use databend_common_meta_app as mt;
use databend_common_meta_app::tenant_key::resource::TenantResource;
use databend_common_protos::pb;

use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for mt::id_generator::IdGeneratorValue {
    type PB = pb::EmptyProto;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(_p: pb::EmptyProto) -> Result<Self, Incompatible> {
        Ok(Self)
    }

    fn to_pb(&self) -> Result<pb::EmptyProto, Incompatible> {
        Ok(pb::EmptyProto {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
        })
    }
}

impl<T> FromToProto for mt::primitive::Id<T>
where
    T: fmt::Debug + Clone + Default + Send + Sync,
    T: From<u64> + Deref<Target = u64> + DerefMut<Target = u64>,
{
    type PB = Self;

    /// Id is actually json encoded and does not have a version.
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        VER
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(p)
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(self.clone())
    }
}

impl<R> FromToProto for mt::data_id::DataId<R>
where R: TenantResource + Sync + Send
{
    type PB = Self;

    /// Id is actually json encoded and does not have a version.
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        VER
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(p)
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(*self)
    }
}
