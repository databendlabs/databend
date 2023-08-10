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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::number::UInt32Type;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::ValueType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::Value;
use common_meta_app::principal::UserOptionFlag;
use common_meta_app::tenant::TenantQuota;
use common_meta_types::MatchSeq;
use common_procedures::ProcedureFeatures;
use common_procedures::ProcedureSignature;
use common_users::UserApiProvider;

use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct TenantQuotaProcedure {
    sig: Box<dyn ProcedureSignature>,
}

impl TenantQuotaProcedure {
    pub fn try_create(sig: Box<dyn ProcedureSignature>) -> Result<Box<dyn Procedure>> {
        Ok(TenantQuotaProcedure { sig }.into_procedure())
    }
}

impl ProcedureSignature for TenantQuotaProcedure {
    fn name(&self) -> &str {
        self.sig.name()
    }

    fn features(&self) -> ProcedureFeatures {
        self.sig.features()
    }

    fn schema(&self) -> Arc<DataSchema> {
        self.sig.schema()
    }
}

#[async_trait::async_trait]
impl OneBlockProcedure for TenantQuotaProcedure {
    /// args:
    /// tenant_id: string
    /// max_databases: u32
    /// max_tables_per_database: u32
    /// max_stages: u32
    /// max_files_per_stage: u32
    #[async_backtrace::framed]
    async fn all_data(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let mut tenant = ctx.get_tenant();
        if !args.is_empty() {
            let user_info = ctx.get_current_user()?;
            if !user_info.has_option_flag(UserOptionFlag::TenantSetting) {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Access denied: '{}' requires user {} option flag",
                    self.name(),
                    UserOptionFlag::TenantSetting
                )));
            }
            tenant = args[0].clone();
        }
        let quota_api = UserApiProvider::instance().get_tenant_quota_api_client(&tenant)?;
        let res = quota_api.get_quota(MatchSeq::GE(0)).await?;
        let mut quota = res.data;

        if args.len() <= 1 {
            return self.to_block(&quota);
        };

        quota.max_databases = args[1].parse::<u32>()?;
        if let Some(max_tables) = args.get(2) {
            quota.max_tables_per_database = max_tables.parse::<u32>()?;
        };
        if let Some(max_stages) = args.get(3) {
            quota.max_stages = max_stages.parse::<u32>()?;
        };
        if let Some(max_files_per_stage) = args.get(4) {
            quota.max_files_per_stage = max_files_per_stage.parse::<u32>()?
        };

        quota_api
            .set_quota(&quota, MatchSeq::Exact(res.seq))
            .await?;

        self.to_block(&quota)
    }
}

impl TenantQuotaProcedure {
    fn to_block(&self, quota: &TenantQuota) -> Result<DataBlock> {
        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt32),
                    Value::Scalar(UInt32Type::upcast_scalar(quota.max_databases)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt32),
                    Value::Scalar(UInt32Type::upcast_scalar(quota.max_tables_per_database)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt32),
                    Value::Scalar(UInt32Type::upcast_scalar(quota.max_stages)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt32),
                    Value::Scalar(UInt32Type::upcast_scalar(quota.max_files_per_stage)),
                ),
            ],
            1,
        ))
    }
}
