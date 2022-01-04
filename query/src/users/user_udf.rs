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

use common_exception::Result;
use common_functions::udfs::UDFFactory;
use common_meta_types::UserDefinedFunction;

use crate::users::UserApiProvider;

/// UDF operations.
impl UserApiProvider {
    // Add a new UDF.
    pub async fn add_udf(&self, tenant: &str, info: UserDefinedFunction) -> Result<u64> {
        let udf_api_client = self.get_udf_api_client(tenant);
        let add_udf = udf_api_client.add_udf(info);
        match add_udf.await {
            Ok(res) => Ok(res),
            Err(failure) => Err(failure.add_message_back("(while add UDF).")),
        }
    }

    // Update a UDF.
    pub async fn update_udf(&self, tenant: &str, info: UserDefinedFunction) -> Result<u64> {
        let udf_api_client = self.get_udf_api_client(tenant);
        let update_udf = udf_api_client.update_udf(info, None);
        match update_udf.await {
            Ok(res) => Ok(res),
            Err(failure) => Err(failure.add_message_back("(while update UDF).")),
        }
    }

    // Get a UDF by name.
    pub async fn get_udf(&self, tenant: &str, udf_name: &str) -> Result<UserDefinedFunction> {
        let udf_api_client = self.get_udf_api_client(tenant);
        let get_udf = udf_api_client.get_udf(udf_name, None);
        Ok(get_udf.await?.data)
    }

    // Get all UDFs for the tenant.
    pub async fn get_udfs(&self, tenant: &str) -> Result<Vec<UserDefinedFunction>> {
        let udf_api_client = self.get_udf_api_client(tenant);
        let get_udfs = udf_api_client.get_udfs();

        match get_udfs.await {
            Err(failure) => Err(failure.add_message_back("(while get UDFs).")),
            Ok(seq_udfs_info) => Ok(seq_udfs_info),
        }
    }

    // Drop a UDF by name.
    pub async fn drop_udf(&self, tenant: &str, udf_name: &str, if_exist: bool) -> Result<()> {
        let udf_api_client = self.get_udf_api_client(tenant);
        let drop_udf = udf_api_client.drop_udf(udf_name, None);
        match drop_udf.await {
            Ok(res) => Ok(res),
            Err(failure) => {
                if if_exist {
                    Ok(())
                } else {
                    Err(failure.add_message_back("(while drop UDF)"))
                }
            }
        }
    }

    pub async fn load_udfs(&self, tenant: &str) -> Result<()> {
        let udfs = self.get_udf_api_client(tenant).get_udfs().await?;

        for udf in udfs.iter() {
            UDFFactory::register(
                tenant,
                udf.name.as_str(),
                &udf.parameters,
                udf.definition.as_str(),
            )?;
        }

        Ok(())
    }
}
