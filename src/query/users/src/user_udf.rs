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
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_types::MatchSeq;

use crate::UserApiProvider;

/// UDF operations.
impl UserApiProvider {
    // Add a new UDF.
    #[async_backtrace::framed]
    pub async fn add_udf(
        &self,
        tenant: &str,
        info: UserDefinedFunction,
        if_not_exists: bool,
    ) -> Result<u64> {
        let udf_api_client = self.get_udf_api_client(tenant)?;
        let add_udf = udf_api_client.add_udf(info);
        match add_udf.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_not_exists && e.code() == ErrorCode::UDF_ALREADY_EXISTS {
                    Ok(u64::MIN)
                } else {
                    Err(e)
                }
            }
        }
    }

    // Update a UDF.
    #[async_backtrace::framed]
    pub async fn update_udf(&self, tenant: &str, info: UserDefinedFunction) -> Result<u64> {
        let udf_api_client = self.get_udf_api_client(tenant)?;
        let update_udf = udf_api_client.update_udf(info, MatchSeq::GE(1));
        match update_udf.await {
            Ok(res) => Ok(res),
            Err(e) => Err(e.add_message_back("(while update UDF).")),
        }
    }

    // Get a UDF by name.
    #[async_backtrace::framed]
    pub async fn get_udf(&self, tenant: &str, udf_name: &str) -> Result<UserDefinedFunction> {
        let udf_api_client = self.get_udf_api_client(tenant)?;
        let get_udf = udf_api_client.get_udf(udf_name, MatchSeq::GE(0));
        Ok(get_udf.await?.data)
    }

    #[async_backtrace::framed]
    pub async fn exists_udf(&self, tenant: &str, udf_name: &str) -> Result<bool> {
        match self.get_udf(tenant, udf_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UNKNOWN_U_D_F {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    // Get all UDFs for the tenant.
    #[async_backtrace::framed]
    pub async fn get_udfs(&self, tenant: &str) -> Result<Vec<UserDefinedFunction>> {
        let udf_api_client = self.get_udf_api_client(tenant)?;
        let get_udfs = udf_api_client.get_udfs();

        match get_udfs.await {
            Err(e) => Err(e.add_message_back("(while get UDFs).")),
            Ok(seq_udfs_info) => Ok(seq_udfs_info),
        }
    }

    // Drop a UDF by name.
    #[async_backtrace::framed]
    pub async fn drop_udf(&self, tenant: &str, udf_name: &str, if_exists: bool) -> Result<()> {
        let udf_api_client = self.get_udf_api_client(tenant)?;
        let drop_udf = udf_api_client.drop_udf(udf_name, MatchSeq::GE(1));
        match drop_udf.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(e.add_message_back("(while drop UDF)"))
                }
            }
        }
    }
}
