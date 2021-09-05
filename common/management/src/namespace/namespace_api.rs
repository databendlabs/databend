// Copyright 2020 Datafuse Labs.
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
//

use std::convert::TryFrom;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_metatypes::SeqValue;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Namespace {
    pub id: String,
}

#[async_trait]
pub trait NamespaceApi {
    // Create a new tenant's namespace.
    async fn add_namespace(&mut self, tenant: String, namespace: Namespace) -> Result<u64>;

    // Get the tenant's namespace.
    async fn get_namespace(
        &mut self,
        tenant: String,
        namespace_id: String,
        seq: Option<u64>,
    ) -> Result<SeqValue<Namespace>>;

    // Get the tenant's all namespaces.
    async fn get_all_namespaces(
        &mut self,
        tenant: String,
        seq: Option<u64>,
    ) -> Result<Vec<SeqValue<Namespace>>>;

    // Check tenant's namespace exists or not.
    async fn exists_namespace(
        &mut self,
        tenant: String,
        namespace_id: String,
        seq: Option<u64>,
    ) -> Result<bool>;

    // Update the tenant's namespace.
    async fn update_namespace(
        &mut self,
        tenant: String,
        namespace: Namespace,
        seq: Option<u64>,
    ) -> Result<Option<u64>>;

    // Drop the tenant's one namespace by id.
    async fn drop_namespace(
        &mut self,
        tenant: String,
        namespace_id: String,
        seq: Option<u64>,
    ) -> Result<()>;
}

impl TryFrom<Vec<u8>> for Namespace {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(user_info) => Ok(user_info),
            Err(serialize_error) => Err(ErrorCode::IllegalUserInfoFormat(format!(
                "Cannot deserialize namespace from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}
