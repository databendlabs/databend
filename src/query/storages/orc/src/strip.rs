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

use std::fmt::Debug;

use databend_common_expression::BlockMetaInfo;
use orc_rust::stripe::Stripe;
#[derive(Debug)]
pub struct StripeInMemory {
    pub path: String,
    pub stripe: Stripe,
}

impl serde::Serialize for StripeInMemory {
    fn serialize<S>(&self, _: S) -> databend_common_exception::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize StripeInMemory")
    }
}

impl<'de> serde::Deserialize<'de> for StripeInMemory {
    fn deserialize<D>(_: D) -> databend_common_exception::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize StripeInMemory")
    }
}

#[typetag::serde(name = "orc_stripe")]
impl BlockMetaInfo for StripeInMemory {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("StripeInMemory as BlockMetaInfo is not expected to be compared.")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unreachable!("StripeInMemory as BlockMetaInfo is not expected to be cloned.")
    }
}
