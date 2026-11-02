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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_app::principal::connection_ident;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_meta_client::types::MatchSeq;

use crate::UserApiProvider;
use crate::meta_service_error;

/// user connection operations.
impl UserApiProvider {
    // Add a new connection. Returns `true` if the connection did not exist before.
    #[async_backtrace::framed]
    pub async fn add_connection(
        &self,
        tenant: &Tenant,
        connection: UserDefinedConnection,
        create_option: &CreateOption,
    ) -> Result<bool> {
        let connection_api_provider = self.connection_api(tenant);
        let created = connection_api_provider
            .add(connection, create_option)
            .await?;
        Ok(created)
    }

    // Get one connection from by tenant.
    #[async_backtrace::framed]
    pub async fn get_connection(
        &self,
        tenant: &Tenant,
        connection_name: &str,
    ) -> Result<UserDefinedConnection> {
        let connection_api_provider = self.connection_api(tenant);
        let get_connection = connection_api_provider.get(connection_name, MatchSeq::GE(0));
        Ok(get_connection.await?.data)
    }

    // Get the tenant all connection list.
    #[async_backtrace::framed]
    pub async fn get_connections(&self, tenant: &Tenant) -> Result<Vec<UserDefinedConnection>> {
        let connection_api_provider = self.connection_api(tenant);
        let get_connections = connection_api_provider.list(None);

        match get_connections.await {
            Err(e) => Err(meta_service_error(e).add_message_back(" (while get connection)")),
            Ok(seq_connections_info) => Ok(seq_connections_info),
        }
    }

    // Drop a connection by name.
    #[async_backtrace::framed]
    pub async fn drop_connection(
        &self,
        tenant: &Tenant,
        name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let connection_api_provider = self.connection_api(tenant);
        let dropped = connection_api_provider
            .remove_with_ownership(name, MatchSeq::GE(1), OwnershipObject::Connection {
                name: name.to_string(),
            })
            .await?;
        if dropped || if_exists {
            Ok(())
        } else {
            let error = UnknownError::<connection_ident::Resource>::new_match_seq(
                name.to_string(),
                MatchSeq::GE(1),
                "NotFound when remove, seq of existing record: 0",
            );
            Err(ErrorCode::from(error).add_message_back(" (while drop connection)"))
        }
    }
}
