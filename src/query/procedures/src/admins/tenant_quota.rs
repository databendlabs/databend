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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;

use crate::ProcedureFeatures;
use crate::ProcedureSignature;

pub struct TenantQuotaProcedureSig {}

impl TenantQuotaProcedureSig {
    pub fn try_create() -> Result<Box<dyn ProcedureSignature>> {
        Ok(Box::new(TenantQuotaProcedureSig {}))
    }
}

impl ProcedureSignature for TenantQuotaProcedureSig {
    fn name(&self) -> &str {
        "TENANT_QUOTA"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
            .variadic_arguments(0, 5)
            .management_mode_required(true)
    }

    fn schema(&self) -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("max_databases", DataType::Number(NumberDataType::UInt32)),
            DataField::new(
                "max_tables_per_database",
                DataType::Number(NumberDataType::UInt32),
            ),
            DataField::new("max_stages", DataType::Number(NumberDataType::UInt32)),
            DataField::new(
                "max_files_per_stage",
                DataType::Number(NumberDataType::UInt32),
            ),
        ])
    }
}
