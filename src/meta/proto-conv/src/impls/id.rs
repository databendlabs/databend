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

    fn to_pb(&self) -> pb::EmptyProto {
        pb::EmptyProto {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
        }
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

    fn to_pb(&self) -> Self::PB {
        *self
    }
}

impl FromToProto for mt::schema::DatabaseId {
    type PB = mt::value_id::ValueId<Self>;

    /// DatabaseId is actually json encoded and does not have a version.
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        VER
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Self::new(*p))
    }

    fn to_pb(&self) -> Self::PB {
        mt::value_id::ValueId::new(self.db_id)
    }
}

impl FromToProto for mt::schema::TableId {
    type PB = mt::value_id::ValueId<Self>;

    /// TableId is actually json encoded and does not have a version.
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        VER
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(Self::new(*p))
    }

    fn to_pb(&self) -> Self::PB {
        mt::value_id::ValueId::new(self.table_id)
    }
}

impl<T> FromToProto for mt::value_id::ValueId<T>
where T: Sync + Send
{
    type PB = Self;

    /// ValueId is actually json encoded and does not have a version.
    fn get_pb_ver(_p: &Self::PB) -> u64 {
        VER
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        Ok(p)
    }

    fn to_pb(&self) -> Self::PB {
        *self
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use prost::Message;

    use super::*;

    fn assert_json_u64_round_trip<T>(value: T, id: u64)
    where T: FromToProto + Debug + PartialEq {
        let pb = value.to_pb();
        let buf = pb.encode_to_vec();

        assert_eq!(buf, id.to_string().as_bytes());

        let decoded_pb = T::PB::decode(buf.as_ref()).unwrap();
        let decoded = T::from_pb(decoded_pb).unwrap();
        assert_eq!(decoded, value);
    }

    #[test]
    fn test_database_id_and_table_id_use_json_u64_proto_encoding() {
        for id in [
            0,
            1,
            u8::MAX as u64,
            u16::MAX as u64,
            u32::MAX as u64,
            i64::MAX as u64,
            u64::MAX - 1,
            u64::MAX,
        ] {
            assert_json_u64_round_trip(mt::schema::DatabaseId::new(id), id);
            assert_json_u64_round_trip(mt::schema::TableId::new(id), id);
        }
    }
}
