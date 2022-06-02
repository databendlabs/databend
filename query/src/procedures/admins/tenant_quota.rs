// Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_meta_types::TenantQuota;
use common_meta_types::UserOptionFlag;

use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;

pub struct TenantQuotaProcedure;

impl TenantQuotaProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(Box::new(TenantQuotaProcedure {}))
    }
}

#[async_trait::async_trait]
impl Procedure for TenantQuotaProcedure {
    fn name(&self) -> &str {
        "TENANT_QUOTA"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
            .variadic_arguments(1, 5)
            .management_mode_required(true)
            .user_option_flag(UserOptionFlag::TenantSetting)
    }

    /// args:
    /// tenant_id: string
    /// max_databases: u32
    /// max_tables_per_database: u32
    /// max_stages: u32
    /// max_files_per_stage: u32
    async fn inner_eval(&self, ctx: Arc<QueryContext>, args: Vec<String>) -> Result<DataBlock> {
        let tenant = args[0].clone();
        let quota_api = ctx
            .get_user_manager()
            .get_tenant_quota_api_client(&tenant)?;
        let res = quota_api.get_quota(None).await?;
        let mut quota = res.data;

        if args.len() == 1 {
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

        quota_api.set_quota(&quota, Some(res.seq)).await?;

        self.to_block(&quota)
    }

    fn schema(&self) -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("max_databases", u32::to_data_type()),
            DataField::new("max_tables_per_database", u32::to_data_type()),
            DataField::new("max_stages", u32::to_data_type()),
            DataField::new("max_files_per_stage", u32::to_data_type()),
        ])
    }
}

impl TenantQuotaProcedure {
    fn to_block(&self, quota: &TenantQuota) -> Result<DataBlock> {
        Ok(DataBlock::create(self.schema(), vec![
            Series::from_data(vec![quota.max_databases]),
            Series::from_data(vec![quota.max_tables_per_database]),
            Series::from_data(vec![quota.max_stages]),
            Series::from_data(vec![quota.max_files_per_stage]),
        ]))
    }
}
