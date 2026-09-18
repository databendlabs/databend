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

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
pub use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::timestamp::check_timestamp;
use databend_common_license::license::Feature;
use databend_common_license::license::LicenseClaims;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_storages_factory::Table;
use humantime::Duration as HumanDuration;

use crate::sessions::TableContext;

pub struct LicenseInfoTable {
    table_info: TableInfo,
}

impl LicenseInfoTable {
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("license_issuer", TableDataType::String),
            TableField::new("license_type", TableDataType::String),
            TableField::new("organization", TableDataType::String),
            TableField::new("issued_at", TableDataType::Timestamp),
            TableField::new("expire_at", TableDataType::Timestamp),
            // formatted string calculate the available time from now to expiry of license
            TableField::new("available_time_until_expiry", TableDataType::String),
            TableField::new("features", TableDataType::String),
            // JWT nbf is independent of issued_at; append to preserve existing column positions.
            TableField::new("valid_from", TableDataType::Timestamp.wrap_nullable()),
        ])
    }

    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        _table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("license_info"),
            meta: TableMeta {
                schema: Self::schema(),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(LicenseInfoTable { table_info }))
    }
}

#[async_trait::async_trait]
impl Table for LicenseInfoTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        // dummy statistics
        Ok((PartStatistics::new_exact(1, 1, 1, 1), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(|output| LicenseInfoSource::create(ctx.clone(), output), 1)?;
        Ok(())
    }
}

struct LicenseInfoSource {
    done: bool,
    ctx: Arc<dyn TableContext>,
}

impl LicenseInfoSource {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, LicenseInfoSource {
            ctx,
            done: false,
        })
    }

    fn to_block(info: &LicenseClaims, now: Duration) -> Result<DataBlock> {
        let issued_at = license_timestamp_micros(info.issued_at.unwrap_or_default())?;
        let expires_at = license_timestamp_micros(info.expires_at.unwrap_or_default())?;
        // Missing nbf means no explicit not-before constraint, not the issuance time or epoch.
        let valid_from = info
            .invalid_before
            .map(license_timestamp_micros)
            .transpose()?;
        let available_time =
            Duration::from_secs(info.expires_at.unwrap_or_default()).saturating_sub(now);
        // Preserve the microsecond precision of the previous license output.
        let available_time = Duration::new(
            available_time.as_secs(),
            available_time.subsec_micros() * 1_000,
        );
        let human_readable_available_time = HumanDuration::from(available_time).to_string();

        let feature_str = info.custom.display_features();
        Ok(DataBlock::new(
            vec![
                BlockEntry::new_const_column_arg::<StringType>(
                    info.issuer.clone().unwrap_or("".to_string()),
                    1,
                ),
                BlockEntry::new_const_column_arg::<StringType>(
                    info.custom.r#type.clone().unwrap_or("".to_string()),
                    1,
                ),
                BlockEntry::new_const_column_arg::<StringType>(
                    info.custom.org.clone().unwrap_or("".to_string()),
                    1,
                ),
                BlockEntry::new_const_column_arg::<TimestampType>(issued_at, 1),
                BlockEntry::new_const_column_arg::<TimestampType>(expires_at, 1),
                BlockEntry::new_const_column_arg::<StringType>(human_readable_available_time, 1),
                BlockEntry::new_const_column_arg::<StringType>(feature_str.to_string(), 1),
                BlockEntry::new_const_column_arg::<NullableType<TimestampType>>(valid_from, 1),
            ],
            1,
        ))
    }
}

#[async_trait::async_trait]
impl AsyncSource for LicenseInfoSource {
    const NAME: &'static str = "license_info";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let settings = self.ctx.get_settings();
        // sync global changes on distributed node cluster.
        settings.load_changes().await?;
        let license = settings.get_enterprise_license(self.ctx.get_version());

        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(license.clone(), Feature::LicenseInfo)?;

        let info = LicenseManagerSwitch::instance()
            .parse_license(license.as_str())
            .map_err_to_code(ErrorCode::LicenseKeyInvalid, || {
                format!(
                    "current license invalid for {}",
                    self.ctx.get_tenant().display()
                )
            })?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err_to_code(
                ErrorCode::LicenseKeyInvalid,
                || "System clock is before the Unix epoch",
            )?;
        Ok(Some(Self::to_block(&info, now)?))
    }
}

fn license_timestamp_micros(seconds: u64) -> Result<i64> {
    let micros = seconds
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| ErrorCode::LicenseKeyInvalid("License timestamp is out of range"))?;
    check_timestamp(micros).map_err(ErrorCode::LicenseKeyInvalid)
}

impl TableFunction for LicenseInfoTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
