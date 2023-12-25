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

use databend_common_exception::Result;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;

#[async_trait::async_trait]
pub trait ConnectionApi: Sync + Send {
    // Add a connection info to /tenant/connection-name.
    async fn add_connection(&self, connection: UserDefinedConnection) -> Result<u64>;

    async fn get_connection(
        &self,
        name: &str,
        seq: MatchSeq,
    ) -> Result<SeqV<UserDefinedConnection>>;

    // Get all the connections for a tenant.
    async fn get_connections(&self) -> Result<Vec<UserDefinedConnection>>;

    // Drop the tenant's connection by name.
    async fn drop_connection(&self, name: &str, seq: MatchSeq) -> Result<()>;
}
