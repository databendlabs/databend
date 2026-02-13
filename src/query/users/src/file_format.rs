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
use databend_common_meta_api::crud::CrudError;
use databend_common_meta_app::principal::UserDefinedFileFormat;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_types::MatchSeq;

use crate::UserApiProvider;
use crate::meta_service_error;

/// user file_format operations.
impl UserApiProvider {
    // Add a new file_format.
    #[async_backtrace::framed]
    pub async fn add_file_format(
        &self,
        tenant: &Tenant,
        file_format_options: UserDefinedFileFormat,
        create_option: &CreateOption,
    ) -> Result<()> {
        let file_format_api_provider = self.file_format_api(tenant);
        file_format_api_provider
            .add(file_format_options, create_option)
            .await?;
        Ok(())
    }

    // Get one file_format from by tenant.
    #[async_backtrace::framed]
    pub async fn get_file_format(
        &self,
        tenant: &Tenant,
        file_format_name: &str,
    ) -> Result<UserDefinedFileFormat> {
        let file_format_api_provider = self.file_format_api(tenant);
        let get_file_format = file_format_api_provider.get(file_format_name, MatchSeq::GE(0));
        Ok(get_file_format.await?.data)
    }

    // Get the tenant all file_format list.
    #[async_backtrace::framed]
    pub async fn get_file_formats(&self, tenant: &Tenant) -> Result<Vec<UserDefinedFileFormat>> {
        let file_format_api_provider = self.file_format_api(tenant);
        let get_file_formats = file_format_api_provider.list(None);

        match get_file_formats.await {
            Err(e) => Err(meta_service_error(e).add_message_back(" (while get file_format)")),
            Ok(seq_file_formats_info) => Ok(seq_file_formats_info),
        }
    }

    // Drop a file_format by name.
    #[async_backtrace::framed]
    pub async fn drop_file_format(
        &self,
        tenant: &Tenant,
        name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let file_format_api_provider = self.file_format_api(tenant);
        let drop_file_format = file_format_api_provider.remove(name, MatchSeq::GE(1));
        match drop_file_format.await {
            Ok(res) => Ok(res),
            Err(CrudError::Business(_unknown_error)) if if_exists => Ok(()),
            Err(e) => {
                let e = ErrorCode::from(e);
                Err(e.add_message_back(" (while drop file_format)"))
            }
        }
    }
}
