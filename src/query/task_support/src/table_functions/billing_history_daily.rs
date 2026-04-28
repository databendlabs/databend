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

use chrono::DateTime;
use chrono::NaiveDate;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::GetBillingHistoryDailyRequest;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType::UInt64;
use databend_common_expression::types::StringType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_storages_factory::Table;

pub struct BillingHistoryDailyTable {
    table_info: TableInfo,
    args_parsed: BillingHistoryDailyArgsParsed,
    table_args: TableArgs,
}

impl BillingHistoryDailyTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = BillingHistoryDailyArgsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("billing_history_daily"),
            meta: TableMeta {
                schema: infer_table_schema(&billing_history_daily_schema())
                    .expect("failed to infer billing_history_daily schema"),
                engine: String::from(table_func_name),
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            args_parsed,
            table_args,
        }))
    }
}

#[async_trait::async_trait]
impl Table for BillingHistoryDailyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::new_exact(1, 1, 1, 1), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| {
                BillingHistoryDailySource::create(ctx.clone(), output, self.args_parsed.clone())
            },
            1,
        )?;
        Ok(())
    }
}

struct BillingHistoryDailySource {
    is_finished: bool,
    args_parsed: BillingHistoryDailyArgsParsed,
    ctx: Arc<dyn TableContext>,
}

impl BillingHistoryDailySource {
    fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: BillingHistoryDailyArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            ctx,
            args_parsed,
            is_finished: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for BillingHistoryDailySource {
    const NAME: &'static str = "billing_history_daily";

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }
        self.is_finished = true;

        if GlobalConfig::instance()
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot view billing_history_daily without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }

        let cloud_api = CloudControlApiProvider::instance();
        let tenant = self.ctx.get_tenant();
        let query_id = self.ctx.get_id();
        let user = self
            .ctx
            .get_current_user()?
            .identity()
            .display()
            .to_string();

        let cfg = build_client_config(
            tenant.tenant_name().to_string(),
            user.clone(),
            query_id.clone(),
            cloud_api.get_timeout(),
        );
        let req = GetBillingHistoryDailyRequest {
            tenant_id: tenant.tenant_name().to_string(),
            billing_month: self.args_parsed.month.clone(),
            sql_user: user,
            query_id,
        };
        let resp = cloud_api
            .get_task_client()
            .get_billing_history_daily(make_request(req, cfg))
            .await?;
        Ok(Some(parse_billing_history_daily_to_datablock(resp)))
    }
}

impl TableFunction for BillingHistoryDailyTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[derive(Clone, Debug)]
struct BillingHistoryDailyArgsParsed {
    month: String,
}

impl BillingHistoryDailyArgsParsed {
    fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_named("billing_history_daily")?;
        if args.len() != 1 {
            return Err(ErrorCode::BadArguments(
                "billing_history_daily requires exactly one named argument: month".to_string(),
            ));
        }

        let month = args
            .get("month")
            .and_then(|v| v.as_string().cloned())
            .ok_or_else(|| {
                ErrorCode::BadArguments(
                    "billing_history_daily requires named string argument: month".to_string(),
                )
            })?;

        validate_month(&month)?;
        Ok(Self { month })
    }
}

fn validate_month(month: &str) -> Result<()> {
    NaiveDate::parse_from_str(&format!("{month}-01"), "%Y-%m-%d").map_err(|_| {
        ErrorCode::BadArguments("invalid month format, expected YYYY-MM".to_string())
    })?;
    Ok(())
}

fn billing_history_daily_schema() -> DataSchemaRef {
    Arc::new(DataSchema::new(vec![
        DataField::new("date", DataType::String),
        DataField::new("total_amount", DataType::String),
        DataField::new("warehouse_amount", DataType::String),
        DataField::new("storage_amount", DataType::String),
        DataField::new("cloud_service_amount", DataType::String),
        DataField::new("storage_total_bytes", DataType::String),
        DataField::new("storage_day_price_per_tb", DataType::String),
        DataField::new("storage_read_requests", DataType::Number(UInt64)),
        DataField::new("storage_kilo_read_request_price", DataType::String),
        DataField::new("storage_write_requests", DataType::Number(UInt64)),
        DataField::new("storage_kilo_write_request_price", DataType::String),
        DataField::new("cloud_service_api_requests", DataType::String),
        DataField::new("cloud_service_kilo_api_requests_price", DataType::String),
    ]))
}

fn parse_billing_history_daily_to_datablock(
    resp: databend_common_cloud_control::pb::GetBillingHistoryDailyResponse,
) -> DataBlock {
    let mut date = Vec::with_capacity(resp.rows.len());
    let mut total_amount = Vec::with_capacity(resp.rows.len());
    let mut warehouse_amount = Vec::with_capacity(resp.rows.len());
    let mut storage_amount = Vec::with_capacity(resp.rows.len());
    let mut cloud_service_amount = Vec::with_capacity(resp.rows.len());
    let mut storage_total_bytes = Vec::with_capacity(resp.rows.len());
    let mut storage_day_price_per_tb = Vec::with_capacity(resp.rows.len());
    let mut storage_read_requests = Vec::with_capacity(resp.rows.len());
    let mut storage_kilo_read_request_price = Vec::with_capacity(resp.rows.len());
    let mut storage_write_requests = Vec::with_capacity(resp.rows.len());
    let mut storage_kilo_write_request_price = Vec::with_capacity(resp.rows.len());
    let mut cloud_service_api_requests = Vec::with_capacity(resp.rows.len());
    let mut cloud_service_kilo_api_requests_price = Vec::with_capacity(resp.rows.len());

    for row in resp.rows {
        let storage_usage = row.storage_usage.unwrap_or_default();
        let cloud_service_usage = row.cloud_service_usage.unwrap_or_default();

        date.push(row.date);
        total_amount.push(row.total_amount);
        warehouse_amount.push(row.warehouse_amount);
        storage_amount.push(row.storage_amount);
        cloud_service_amount.push(row.cloud_service_amount);
        storage_total_bytes.push(storage_usage.total_bytes);
        storage_day_price_per_tb.push(storage_usage.day_price_per_tb);
        storage_read_requests.push(storage_usage.read_requests);
        storage_kilo_read_request_price.push(storage_usage.kilo_read_request_price);
        storage_write_requests.push(storage_usage.write_requests);
        storage_kilo_write_request_price.push(storage_usage.kilo_write_request_price);
        cloud_service_api_requests.push(cloud_service_usage.api_requests);
        cloud_service_kilo_api_requests_price.push(cloud_service_usage.kilo_api_requests_price);
    }

    DataBlock::new_from_columns(vec![
        StringType::from_data(date),
        StringType::from_data(total_amount),
        StringType::from_data(warehouse_amount),
        StringType::from_data(storage_amount),
        StringType::from_data(cloud_service_amount),
        StringType::from_data(storage_total_bytes),
        StringType::from_data(storage_day_price_per_tb),
        databend_common_expression::types::UInt64Type::from_data(storage_read_requests),
        StringType::from_data(storage_kilo_read_request_price),
        databend_common_expression::types::UInt64Type::from_data(storage_write_requests),
        StringType::from_data(storage_kilo_write_request_price),
        StringType::from_data(cloud_service_api_requests),
        StringType::from_data(cloud_service_kilo_api_requests_price),
    ])
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_catalog::table_args::TableArgs;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    #[test]
    fn test_parse_args_accepts_named_month() {
        let args = TableArgs::new_named(HashMap::from([(
            "month".to_string(),
            Scalar::String("2026-03".to_string()),
        )]));

        let parsed = BillingHistoryDailyArgsParsed::parse(&args).unwrap();
        assert_eq!(parsed.month, "2026-03");
    }

    #[test]
    fn test_parse_args_rejects_non_named_or_invalid_month() {
        let positioned = TableArgs::new_positioned(vec![Scalar::String("2026-03".to_string())]);
        assert!(BillingHistoryDailyArgsParsed::parse(&positioned).is_err());

        let missing = TableArgs::new_named(HashMap::new());
        assert!(BillingHistoryDailyArgsParsed::parse(&missing).is_err());

        let invalid = TableArgs::new_named(HashMap::from([(
            "month".to_string(),
            Scalar::String("202603".to_string()),
        )]));
        assert!(BillingHistoryDailyArgsParsed::parse(&invalid).is_err());
    }

    #[test]
    fn test_parse_daily_response_to_datablock() {
        let block = parse_billing_history_daily_to_datablock(
            databend_common_cloud_control::pb::GetBillingHistoryDailyResponse {
                rows: vec![databend_common_cloud_control::pb::BillingHistoryDailyRow {
                    date: "2026-03-02".to_string(),
                    total_amount: "2.465".to_string(),
                    warehouse_amount: "1.390".to_string(),
                    storage_amount: "1.067".to_string(),
                    cloud_service_amount: "0.008".to_string(),
                    storage_usage: Some(
                        databend_common_cloud_control::pb::BillingHistoryDailyStorageUsage {
                            total_bytes: "1580580967006".to_string(),
                            day_price_per_tb: "0.7419354838709677".to_string(),
                            read_requests: 10,
                            kilo_read_request_price: "0.0004".to_string(),
                            write_requests: 20,
                            kilo_write_request_price: "0.005".to_string(),
                        },
                    ),
                    cloud_service_usage: Some(
                        databend_common_cloud_control::pb::BillingHistoryDailyCloudServiceUsage {
                            api_requests: "78".to_string(),
                            kilo_api_requests_price: "0.1".to_string(),
                        },
                    ),
                }],
                error: None,
            },
        );

        assert_eq!(block.num_rows(), 1);
        assert_eq!(block.num_columns(), 13);

        assert_eq!(
            block.get_by_offset(0).index(0).unwrap(),
            ScalarRef::String("2026-03-02")
        );
        assert_eq!(
            block.get_by_offset(1).index(0).unwrap(),
            ScalarRef::String("2.465")
        );
        assert_eq!(
            block.get_by_offset(7).index(0).unwrap(),
            ScalarRef::Number(NumberScalar::UInt64(10))
        );
        assert_eq!(
            block.get_by_offset(9).index(0).unwrap(),
            ScalarRef::Number(NumberScalar::UInt64(20))
        );
        assert_eq!(
            block.get_by_offset(11).index(0).unwrap(),
            ScalarRef::String("78")
        );
    }
}
