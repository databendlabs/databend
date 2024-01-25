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
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_types::MatchSeq;

use crate::UserApiProvider;

/// user connection operations.
impl UserApiProvider {
    // Add a new connection.
    #[async_backtrace::framed]
    pub async fn add_connection(
        &self,
        tenant: &str,
        connection: UserDefinedConnection,
        if_not_exists: bool,
    ) -> Result<u64> {
        let connection_api_provider = self.get_connection_api_client(tenant)?;
        let add_connection = connection_api_provider.add_connection(connection);
        match add_connection.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_not_exists && e.code() == ErrorCode::CONNECTION_ALREADY_EXISTS {
                    Ok(u64::MIN)
                } else {
                    Err(e)
                }
            }
        }
    }

    // Get one connection from by tenant.
    #[async_backtrace::framed]
    pub async fn get_connection(
        &self,
        tenant: &str,
        connection_name: &str,
    ) -> Result<UserDefinedConnection> {
        let connection_api_provider = self.get_connection_api_client(tenant)?;
        let get_connection =
            connection_api_provider.get_connection(connection_name, MatchSeq::GE(0));
        Ok(get_connection.await?.data)
    }

    // Get the tenant all connection list.
    #[async_backtrace::framed]
    pub async fn get_connections(&self, tenant: &str) -> Result<Vec<UserDefinedConnection>> {
        let connection_api_provider = self.get_connection_api_client(tenant)?;
        let get_connections = connection_api_provider.get_connections();

        match get_connections.await {
            Err(e) => Err(e.add_message_back(" (while get connection)")),
            Ok(seq_connections_info) => Ok(seq_connections_info),
        }
    }

    // Drop a connection by name.
    #[async_backtrace::framed]
    pub async fn drop_connection(&self, tenant: &str, name: &str, if_exists: bool) -> Result<()> {
        let connection_api_provider = self.get_connection_api_client(tenant)?;
        let drop_connection = connection_api_provider.drop_connection(name, MatchSeq::GE(1));
        match drop_connection.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_exists && e.code() == ErrorCode::UNKNOWN_CONNECTION {
                    Ok(())
                } else {
                    Err(e.add_message_back(" (while drop connection)"))
                }
            }
        }
    }
}
