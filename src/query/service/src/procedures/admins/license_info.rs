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

use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRefExt;
use common_expression::Scalar;
use common_expression::Value;
use common_license::license::LicenseInfo;
use common_license::license_manager::get_license_manager;
use humantime::Duration as HumanDuration;
use jwt_simple::claims::JWTClaims;
use jwt_simple::prelude::Clock;

use crate::procedures::OneBlockProcedure;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFeatures;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct LicenseInfoProcedure;

impl LicenseInfoProcedure {
    pub fn try_create() -> Result<Box<dyn Procedure>> {
        Ok(LicenseInfoProcedure {}.into_procedure())
    }
}

#[async_trait::async_trait]
impl OneBlockProcedure for LicenseInfoProcedure {
    fn name(&self) -> &str {
        "LICENSE_INFO"
    }

    fn features(&self) -> ProcedureFeatures {
        ProcedureFeatures::default()
            .variadic_arguments(0, 1)
            .management_mode_required(false)
    }

    #[async_backtrace::framed]
    async fn all_data(&self, ctx: Arc<QueryContext>, _args: Vec<String>) -> Result<DataBlock> {
        let settings = ctx.get_settings();
        // sync global changes on distributed node cluster.
        settings.load_global_changes().await?;

        let license = settings
            .get_enterprise_license()
            .map_err_to_code(ErrorCode::LicenseKeyInvalid, || {
                format!("failed to get license for {}", ctx.get_tenant())
            })?;

        get_license_manager().manager.check_enterprise_enabled(
            &settings,
            ctx.get_tenant(),
            "license_info".to_string(),
        )?;

        let info = get_license_manager()
            .manager
            .parse_license(license.as_str())
            .map_err_to_code(ErrorCode::LicenseKeyInvalid, || {
                format!("current license invalid for {}", ctx.get_tenant())
            })?;
        self.to_block(&info)
    }

    fn schema(&self) -> Arc<DataSchema> {
        DataSchemaRefExt::create(vec![
            DataField::new("license_issuer", DataType::String),
            DataField::new("license_type", DataType::String),
            DataField::new("organization", DataType::String),
            DataField::new("issued_at", DataType::Timestamp),
            DataField::new("expire_at", DataType::Timestamp),
            // formatted string calculate the available time from now to expiry of license
            DataField::new("available_time_until_expiry", DataType::String),
        ])
    }
}

impl LicenseInfoProcedure {
    fn to_block(&self, info: &JWTClaims<LicenseInfo>) -> Result<DataBlock> {
        let now = Clock::now_since_epoch();
        let available_time = info.expires_at.unwrap_or_default().sub(now).as_micros();
        let human_readable_available_time =
            HumanDuration::from(Duration::from_micros(available_time)).to_string();
        Ok(DataBlock::new(
            vec![
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(
                        info.issuer
                            .clone()
                            .unwrap_or("".to_string())
                            .into_bytes()
                            .to_vec(),
                    )),
                },
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(
                        info.custom
                            .r#type
                            .clone()
                            .unwrap_or("".to_string())
                            .into_bytes()
                            .to_vec(),
                    )),
                },
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(
                        info.custom
                            .org
                            .clone()
                            .unwrap_or("".to_string())
                            .into_bytes()
                            .to_vec(),
                    )),
                },
                BlockEntry {
                    data_type: DataType::Timestamp,
                    value: Value::Scalar(Scalar::Timestamp(
                        info.issued_at.unwrap_or_default().as_micros() as i64,
                    )),
                },
                BlockEntry {
                    data_type: DataType::Timestamp,
                    value: Value::Scalar(Scalar::Timestamp(
                        info.expires_at.unwrap_or_default().as_micros() as i64,
                    )),
                },
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Scalar(Scalar::String(
                        human_readable_available_time.into_bytes().to_vec(),
                    )),
                },
            ],
            1,
        ))
    }
}
