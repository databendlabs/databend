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
use databend_common_cloud_control::pb::BillingError as PbBillingError;
use databend_common_cloud_control::pb::GetBillingUsageDailyRequest;
use databend_common_cloud_control::pb::GetBillingUsageDailyResponse;
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
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_storages_factory::Table;
use serde_json::Value;

pub struct BillingUsageDailyTable {
    table_info: TableInfo,
    args_parsed: BillingUsageDailyArgsParsed,
    table_args: TableArgs,
}

impl BillingUsageDailyTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = BillingUsageDailyArgsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("billing_usage_daily"),
            meta: TableMeta {
                schema: infer_table_schema(&billing_usage_daily_schema())
                    .expect("failed to infer billing_usage_daily schema"),
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
impl Table for BillingUsageDailyTable {
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
            |output| BillingUsageDailySource::create(ctx.clone(), output, self.args_parsed.clone()),
            1,
        )?;
        Ok(())
    }
}

struct BillingUsageDailySource {
    is_finished: bool,
    args_parsed: BillingUsageDailyArgsParsed,
    ctx: Arc<dyn TableContext>,
}

impl BillingUsageDailySource {
    fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: BillingUsageDailyArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            ctx,
            args_parsed,
            is_finished: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for BillingUsageDailySource {
    const NAME: &'static str = "billing_usage_daily";

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
                "cannot view billing_usage_daily without cloud control enabled, please set cloud_control_grpc_server_address in config",
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
        let req = GetBillingUsageDailyRequest {
            tenant_id: tenant.tenant_name().to_string(),
            billing_month: self.args_parsed.month.clone(),
            sql_user: user,
            query_id,
        };
        let resp = cloud_api
            .get_billing_client()
            .get_billing_usage_daily(make_request(req, cfg))
            .await?;
        Ok(Some(parse_billing_usage_daily_response(resp)?))
    }
}

impl TableFunction for BillingUsageDailyTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[derive(Clone, Debug)]
struct BillingUsageDailyArgsParsed {
    month: String,
}

impl BillingUsageDailyArgsParsed {
    fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_named("billing_usage_daily")?;
        if args.len() != 1 {
            return Err(ErrorCode::BadArguments(
                "billing_usage_daily requires exactly one named argument: month".to_string(),
            ));
        }

        let month = args
            .get("month")
            .and_then(|v| v.as_string().cloned())
            .ok_or_else(|| {
                ErrorCode::BadArguments(
                    "billing_usage_daily requires named string argument: month".to_string(),
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

fn billing_usage_daily_schema() -> DataSchemaRef {
    Arc::new(DataSchema::new(vec![
        DataField::new("usage_date", DataType::String),
        DataField::new("usage_type", DataType::String),
        DataField::new("service_type", DataType::String),
        DataField::new("resource_name", DataType::String),
        DataField::new("usage", DataType::String),
        DataField::new("usage_unit", DataType::String),
        DataField::new("rate", DataType::String),
        DataField::new("rate_unit", DataType::String),
        DataField::new("usage_in_currency", DataType::String),
        DataField::new("currency", DataType::String),
        DataField::new("tags", DataType::Variant),
        DataField::new("details", DataType::Variant),
    ]))
}

fn parse_billing_usage_daily_response(resp: GetBillingUsageDailyResponse) -> Result<DataBlock> {
    if let Some(error) = resp.error.as_ref() {
        return Err(billing_error_to_error_code(
            "get_billing_usage_daily",
            error,
        ));
    }

    Ok(parse_billing_usage_daily_to_datablock(resp))
}

fn billing_error_to_error_code(operation: &str, error: &PbBillingError) -> ErrorCode {
    ErrorCode::CloudControlConnectError(format!(
        "cloud control {operation} failed: {} (kind: {}, code: {})",
        error.message, error.kind, error.code
    ))
}

fn parse_billing_usage_daily_to_datablock(resp: GetBillingUsageDailyResponse) -> DataBlock {
    let mut usage_date = Vec::with_capacity(resp.rows.len());
    let mut usage_type = Vec::with_capacity(resp.rows.len());
    let mut service_type = Vec::with_capacity(resp.rows.len());
    let mut resource_name = Vec::with_capacity(resp.rows.len());
    let mut usage = Vec::with_capacity(resp.rows.len());
    let mut usage_unit = Vec::with_capacity(resp.rows.len());
    let mut rate = Vec::with_capacity(resp.rows.len());
    let mut rate_unit = Vec::with_capacity(resp.rows.len());
    let mut usage_in_currency = Vec::with_capacity(resp.rows.len());
    let mut currency = Vec::with_capacity(resp.rows.len());
    let mut tags = Vec::with_capacity(resp.rows.len());
    let mut details = Vec::with_capacity(resp.rows.len());

    for row in resp.rows {
        usage_date.push(row.usage_date);
        usage_type.push(row.usage_type);
        service_type.push(row.service_type);
        resource_name.push(row.resource_name);
        usage.push(row.usage);
        usage_unit.push(row.usage_unit);
        rate.push(row.rate);
        rate_unit.push(row.rate_unit);
        usage_in_currency.push(row.usage_in_currency);
        currency.push(row.currency);
        tags.push(serde_json::to_vec(&row.tags).unwrap_or_else(|_| b"{}".to_vec()));
        details.push(json_text_to_variant(&row.details));
    }

    DataBlock::new_from_columns(vec![
        StringType::from_data(usage_date),
        StringType::from_data(usage_type),
        StringType::from_data(service_type),
        StringType::from_data(resource_name),
        StringType::from_data(usage),
        StringType::from_data(usage_unit),
        StringType::from_data(rate),
        StringType::from_data(rate_unit),
        StringType::from_data(usage_in_currency),
        StringType::from_data(currency),
        VariantType::from_data(tags),
        VariantType::from_data(details),
    ])
}

fn json_text_to_variant(raw: &str) -> Vec<u8> {
    if raw.trim().is_empty() {
        return b"{}".to_vec();
    }

    match serde_json::from_str::<Value>(raw) {
        Ok(value) => serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec()),
        Err(_) => {
            serde_json::to_vec(&Value::String(raw.to_string())).unwrap_or_else(|_| b"\"\"".to_vec())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;

    use databend_common_catalog::table_args::TableArgs;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;

    use super::*;

    #[test]
    fn test_parse_args_accepts_named_month() {
        let args = TableArgs::new_named(HashMap::from([(
            "month".to_string(),
            Scalar::String("2026-03".to_string()),
        )]));

        let parsed = BillingUsageDailyArgsParsed::parse(&args).unwrap();
        assert_eq!(parsed.month, "2026-03");
    }

    #[test]
    fn test_parse_args_rejects_non_named_or_invalid_month() {
        let positioned = TableArgs::new_positioned(vec![Scalar::String("2026-03".to_string())]);
        assert!(BillingUsageDailyArgsParsed::parse(&positioned).is_err());

        let missing = TableArgs::new_named(HashMap::new());
        assert!(BillingUsageDailyArgsParsed::parse(&missing).is_err());

        let invalid = TableArgs::new_named(HashMap::from([(
            "month".to_string(),
            Scalar::String("202603".to_string()),
        )]));
        assert!(BillingUsageDailyArgsParsed::parse(&invalid).is_err());
    }

    #[test]
    fn test_parse_usage_daily_response_to_datablock() {
        let expected_tags =
            serde_json::to_vec(&HashMap::from([("env".to_string(), "test".to_string())])).unwrap();
        let expected_details = serde_json::to_vec(&serde_json::json!({
            "cluster_name": "cl-00000",
            "max_clusters": 1,
            "size": "XSmall",
        }))
        .unwrap();

        let block = parse_billing_usage_daily_to_datablock(GetBillingUsageDailyResponse {
            rows: vec![databend_common_cloud_control::pb::BillingUsageDailyRow {
                usage_date: "2026-03-02".to_string(),
                usage_type: "compute".to_string(),
                service_type: "WAREHOUSE_METERING".to_string(),
                resource_name: "default".to_string(),
                usage: "2653".to_string(),
                usage_unit: "second".to_string(),
                rate: "".to_string(),
                rate_unit: "second".to_string(),
                usage_in_currency: "0.737".to_string(),
                currency: "¥".to_string(),
                tags: BTreeMap::from([("env".to_string(), "test".to_string())]),
                details: "{\"cluster_name\":\"cl-00000\",\"max_clusters\":1,\"size\":\"XSmall\"}"
                    .to_string(),
            }],
            error: None,
        });

        assert_eq!(block.num_rows(), 1);
        assert_eq!(block.num_columns(), 12);

        assert_eq!(
            block.get_by_offset(0).index(0).unwrap(),
            ScalarRef::String("2026-03-02")
        );
        assert_eq!(
            block.get_by_offset(1).index(0).unwrap(),
            ScalarRef::String("compute")
        );
        assert_eq!(
            block.get_by_offset(8).index(0).unwrap(),
            ScalarRef::String("0.737")
        );
        assert_eq!(
            block.get_by_offset(9).index(0).unwrap(),
            ScalarRef::String("¥")
        );

        match block.get_by_offset(10).index(0).unwrap() {
            ScalarRef::Variant(bytes) => assert_eq!(bytes, expected_tags.as_slice()),
            other => panic!("unexpected scalar type for tags: {other:?}"),
        }

        match block.get_by_offset(11).index(0).unwrap() {
            ScalarRef::Variant(bytes) => assert_eq!(bytes, expected_details.as_slice()),
            other => panic!("unexpected scalar type for details: {other:?}"),
        }
    }

    #[test]
    fn test_parse_usage_daily_response_preserves_invalid_details_as_string_variant() {
        let block = parse_billing_usage_daily_to_datablock(GetBillingUsageDailyResponse {
            rows: vec![databend_common_cloud_control::pb::BillingUsageDailyRow {
                usage_date: "2026-03-02".to_string(),
                usage_type: "storage".to_string(),
                service_type: "STORAGE".to_string(),
                resource_name: String::new(),
                usage: "1580580967006".to_string(),
                usage_unit: "byte".to_string(),
                rate: "".to_string(),
                rate_unit: "tb_day".to_string(),
                usage_in_currency: "1.067".to_string(),
                currency: "$".to_string(),
                tags: BTreeMap::new(),
                details: "not-json".to_string(),
            }],
            error: None,
        });

        let expected = serde_json::to_vec(&Value::String("not-json".to_string())).unwrap();
        match block.get_by_offset(11).index(0).unwrap() {
            ScalarRef::Variant(bytes) => assert_eq!(bytes, expected.as_slice()),
            other => panic!("unexpected scalar type for details: {other:?}"),
        }
    }

    #[test]
    fn test_parse_usage_daily_response_surfaces_task_error() {
        let err = parse_billing_usage_daily_response(GetBillingUsageDailyResponse {
            rows: vec![],
            error: Some(PbBillingError {
                kind: "Internal".to_string(),
                message: "billing usage unavailable".to_string(),
                code: 500,
            }),
        })
        .expect_err("billing error should be returned");

        assert!(err.message().contains("get_billing_usage_daily"));
        assert!(err.message().contains("billing usage unavailable"));
        assert!(err.message().contains("Internal"));
        assert!(err.message().contains("500"));
    }
}
