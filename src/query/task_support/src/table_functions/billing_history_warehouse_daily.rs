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
use databend_common_cloud_control::pb::GetBillingHistoryWarehouseDailyRequest;
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

pub struct BillingHistoryWarehouseDailyTable {
    table_info: TableInfo,
    args_parsed: BillingHistoryWarehouseDailyArgsParsed,
    table_args: TableArgs,
}

impl BillingHistoryWarehouseDailyTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = BillingHistoryWarehouseDailyArgsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("billing_history_warehouse_daily"),
            meta: TableMeta {
                schema: infer_table_schema(&billing_history_warehouse_daily_schema())
                    .expect("failed to infer billing_history_warehouse_daily schema"),
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
impl Table for BillingHistoryWarehouseDailyTable {
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
                BillingHistoryWarehouseDailySource::create(
                    ctx.clone(),
                    output,
                    self.args_parsed.clone(),
                )
            },
            1,
        )?;
        Ok(())
    }
}

struct BillingHistoryWarehouseDailySource {
    is_finished: bool,
    args_parsed: BillingHistoryWarehouseDailyArgsParsed,
    ctx: Arc<dyn TableContext>,
}

impl BillingHistoryWarehouseDailySource {
    fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: BillingHistoryWarehouseDailyArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output, Self {
            ctx,
            args_parsed,
            is_finished: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for BillingHistoryWarehouseDailySource {
    const NAME: &'static str = "billing_history_warehouse_daily";

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
                "cannot view billing_history_warehouse_daily without cloud control enabled, please set cloud_control_grpc_server_address in config",
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
        let req = GetBillingHistoryWarehouseDailyRequest {
            tenant_id: tenant.tenant_name().to_string(),
            billing_month: self.args_parsed.month.clone(),
            sql_user: user,
            query_id,
        };
        let resp = cloud_api
            .get_task_client()
            .get_billing_history_warehouse_daily(make_request(req, cfg))
            .await?;
        Ok(Some(parse_billing_history_warehouse_daily_to_datablock(
            resp,
        )))
    }
}

impl TableFunction for BillingHistoryWarehouseDailyTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

#[derive(Clone, Debug)]
struct BillingHistoryWarehouseDailyArgsParsed {
    month: String,
}

impl BillingHistoryWarehouseDailyArgsParsed {
    fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_named("billing_history_warehouse_daily")?;
        if args.len() != 1 {
            return Err(ErrorCode::BadArguments(
                "billing_history_warehouse_daily requires exactly one named argument: month"
                    .to_string(),
            ));
        }

        let month = args
            .get("month")
            .and_then(|v| v.as_string().cloned())
            .ok_or_else(|| {
                ErrorCode::BadArguments(
                    "billing_history_warehouse_daily requires named string argument: month"
                        .to_string(),
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

fn billing_history_warehouse_daily_schema() -> DataSchemaRef {
    Arc::new(DataSchema::new(vec![
        DataField::new("date", DataType::String),
        DataField::new("warehouse_name", DataType::String),
        DataField::new("cluster_name", DataType::String),
        DataField::new("max_clusters", DataType::Number(UInt64)),
        DataField::new("size", DataType::String),
        DataField::new("seconds", DataType::Number(UInt64)),
        DataField::new("price_per_second", DataType::String),
        DataField::new("credits", DataType::String),
        DataField::new("tags", DataType::Variant),
    ]))
}

fn parse_billing_history_warehouse_daily_to_datablock(
    resp: databend_common_cloud_control::pb::GetBillingHistoryWarehouseDailyResponse,
) -> DataBlock {
    let mut date = Vec::with_capacity(resp.rows.len());
    let mut warehouse_name = Vec::with_capacity(resp.rows.len());
    let mut cluster_name = Vec::with_capacity(resp.rows.len());
    let mut max_clusters = Vec::with_capacity(resp.rows.len());
    let mut size = Vec::with_capacity(resp.rows.len());
    let mut seconds = Vec::with_capacity(resp.rows.len());
    let mut price_per_second = Vec::with_capacity(resp.rows.len());
    let mut credits = Vec::with_capacity(resp.rows.len());
    let mut tags = Vec::with_capacity(resp.rows.len());

    for row in resp.rows {
        date.push(row.date);
        warehouse_name.push(row.warehouse_name);
        cluster_name.push(row.cluster_name);
        max_clusters.push(row.max_clusters);
        size.push(row.size);
        seconds.push(row.seconds);
        price_per_second.push(row.price_per_second);
        credits.push(row.credits);
        tags.push(serde_json::to_vec(&row.tags).unwrap_or_else(|_| b"{}".to_vec()));
    }

    DataBlock::new_from_columns(vec![
        StringType::from_data(date),
        StringType::from_data(warehouse_name),
        StringType::from_data(cluster_name),
        databend_common_expression::types::UInt64Type::from_data(max_clusters),
        StringType::from_data(size),
        databend_common_expression::types::UInt64Type::from_data(seconds),
        StringType::from_data(price_per_second),
        StringType::from_data(credits),
        VariantType::from_data(tags),
    ])
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
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

        let parsed = BillingHistoryWarehouseDailyArgsParsed::parse(&args).unwrap();
        assert_eq!(parsed.month, "2026-03");
    }

    #[test]
    fn test_parse_args_rejects_non_named_or_invalid_month() {
        let positioned = TableArgs::new_positioned(vec![Scalar::String("2026-03".to_string())]);
        assert!(BillingHistoryWarehouseDailyArgsParsed::parse(&positioned).is_err());

        let missing = TableArgs::new_named(HashMap::new());
        assert!(BillingHistoryWarehouseDailyArgsParsed::parse(&missing).is_err());

        let invalid = TableArgs::new_named(HashMap::from([(
            "month".to_string(),
            Scalar::String("2026/03".to_string()),
        )]));
        assert!(BillingHistoryWarehouseDailyArgsParsed::parse(&invalid).is_err());
    }

    #[test]
    fn test_parse_warehouse_daily_response_to_datablock() {
        let expected_tags =
            serde_json::to_vec(&HashMap::from([("env".to_string(), "test".to_string())])).unwrap();

        let block = parse_billing_history_warehouse_daily_to_datablock(
            databend_common_cloud_control::pb::GetBillingHistoryWarehouseDailyResponse {
                rows: vec![
                    databend_common_cloud_control::pb::BillingHistoryWarehouseDailyRow {
                        date: "2026-03-02".to_string(),
                        warehouse_name: "default".to_string(),
                        cluster_name: "cl-00000".to_string(),
                        max_clusters: 1,
                        size: "XSmall".to_string(),
                        seconds: 2653,
                        price_per_second: "0.0002777777777778".to_string(),
                        credits: "0.737".to_string(),
                        tags: BTreeMap::from([("env".to_string(), "test".to_string())]),
                    },
                ],
                error: None,
            },
        );

        assert_eq!(block.num_rows(), 1);
        assert_eq!(block.num_columns(), 9);

        assert_eq!(
            block.get_by_offset(0).index(0).unwrap(),
            ScalarRef::String("2026-03-02")
        );
        assert_eq!(
            block.get_by_offset(1).index(0).unwrap(),
            ScalarRef::String("default")
        );
        assert_eq!(
            block.get_by_offset(3).index(0).unwrap(),
            ScalarRef::Number(NumberScalar::UInt64(1))
        );
        assert_eq!(
            block.get_by_offset(5).index(0).unwrap(),
            ScalarRef::Number(NumberScalar::UInt64(2653))
        );
        match block.get_by_offset(8).index(0).unwrap() {
            ScalarRef::Variant(bytes) => assert_eq!(bytes, expected_tags.as_slice()),
            other => panic!("unexpected scalar type for tags: {other:?}"),
        }
    }
}
