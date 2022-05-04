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

use std::convert::Infallible;
use std::fmt::Debug;
use std::string::FromUtf8Error;
use std::time::Duration;

use common_base::escape_for_key;
use common_exception::ErrorCode;
use common_grpc::RpcClientTlsConfig;
use common_meta_types::DatabaseNameIdent;
use common_meta_types::DatabaseTenantIdIdent;
use common_tracing::tracing;

use crate::MetaGrpcClient;
use crate::MetaGrpcClientConf;

/// Metasrv client base on only KVApi.
///
/// It implement transactional operation on the client side, through `KVApi::transaction()`.
///
/// TODO(xp): consider remove the `impl MetaAPI for MetaGrpcClient` when this is done.
pub struct MetaClientOnKV {
    pub(crate) inner: MetaGrpcClient,
}

impl MetaClientOnKV {
    pub async fn try_new(conf: &MetaGrpcClientConf) -> Result<MetaClientOnKV, Infallible> {
        Ok(Self {
            inner: MetaGrpcClient::try_new(conf).await?,
        })
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub async fn try_create(
        addr: &str,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        conf: Option<RpcClientTlsConfig>,
    ) -> Result<Self, ErrorCode> {
        Ok(Self {
            inner: MetaGrpcClient::try_create(addr, username, password, timeout, conf).await?,
        })
    }
}

/// Key for database id generator
#[derive(Debug)]
pub struct DatabaseIdGen {}

/// Key for table id generator
#[derive(Debug)]
pub struct TableIdGen {}

/// Convert to a string as a key used in KVApi
pub trait ToKVMetaKey: Debug {
    fn to_key(&self) -> Result<String, FromUtf8Error>;
}

impl ToKVMetaKey for DatabaseNameIdent {
    fn to_key(&self) -> Result<String, FromUtf8Error> {
        let k = format!(
            "__fd_database/{}/by_name/{}",
            escape_for_key(&self.tenant)?,
            escape_for_key(&self.db_name)?,
        );
        Ok(k)
    }
}

impl ToKVMetaKey for DatabaseTenantIdIdent {
    fn to_key(&self) -> Result<String, FromUtf8Error> {
        let k = format!(
            "__fd_database/{}/by_id/{}",
            escape_for_key(&self.tenant)?,
            self.db_id,
        );
        Ok(k)
    }
}

impl ToKVMetaKey for DatabaseIdGen {
    fn to_key(&self) -> Result<String, FromUtf8Error> {
        let k = "__fd_id_gen/database_id".to_string();
        Ok(k)
    }
}

impl ToKVMetaKey for TableIdGen {
    fn to_key(&self) -> Result<String, FromUtf8Error> {
        let k = "__fd_id_gen/table_id".to_string();
        Ok(k)
    }
}
