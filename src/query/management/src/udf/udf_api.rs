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
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::SeqV;

use crate::errors::TenantError;

#[derive(Clone, Debug, thiserror::Error)]
pub enum UdfError {
    #[error("TenantError: '{0}'")]
    TenantError(#[from] TenantError),

    #[error("UDF not found: '{tenant}/{name}'")]
    NotFound { tenant: String, name: String },

    #[error("MetaService error: {0}")]
    MetaError(#[from] MetaError),
}

impl From<UdfError> for ErrorCode {
    fn from(value: UdfError) -> Self {
        let s = value.to_string();
        match value {
            UdfError::TenantError(e) => ErrorCode::from(e),
            UdfError::NotFound { .. } => ErrorCode::UnknownUDF(s),
            UdfError::MetaError(meta_err) => ErrorCode::from(meta_err),
        }
    }
}

#[async_trait::async_trait]
pub trait UdfApi: Sync + Send {
    // Add a UDF to /tenant/udf-name.
    async fn add_udf(
        &self,
        udf: UserDefinedFunction,
        create_option: &CreateOption,
    ) -> Result<(), ErrorCode>;

    // Update a UDF to /tenant/udf-name.
    async fn update_udf(&self, udf: UserDefinedFunction, seq: MatchSeq) -> Result<u64, ErrorCode>;

    // Get UDF by name.
    async fn get_udf(&self, udf_name: &str) -> Result<SeqV<UserDefinedFunction>, ErrorCode>;

    // Get all the UDFs for a tenant.
    async fn get_udfs(&self) -> Result<Vec<UserDefinedFunction>, ErrorCode>;

    /// Drop the tenant's UDF by name, return the dropped one or None if nothing is dropped.
    async fn drop_udf(
        &self,
        udf_name: &str,
        seq: MatchSeq,
    ) -> Result<Option<SeqV<UserDefinedFunction>>, MetaError>;
}
