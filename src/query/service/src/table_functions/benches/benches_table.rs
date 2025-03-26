use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_sources::EmptySource;
use databend_common_storage::DataOperator;
use databend_common_storages_system::BenchesArguments;
use databend_common_storages_system::TestMetric;
use databend_common_storages_system::TestMode;
use opendal::Operator;
use tokio::sync::Barrier;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BenchesTablePart;

#[typetag::serde(name = "system")]
impl PartInfo for BenchesTablePart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<BenchesTablePart>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        0
    }
}

pub struct BenchesTable {
    table_info: TableInfo,
    arguments: BenchesArguments,
}

impl BenchesTable {
    pub fn create(
        _database_name: &str,
        _table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args = table_args.expect_all_positioned("benches", Some(1))?;

        if args.len() != 1 {
            return Err(ErrorCode::BadArguments(""));
        }

        let arguments = match args[0].as_string() {
            None => Err(ErrorCode::BadArguments("")),
            Some(value) => Ok(serde_json::from_str::<BenchesArguments>(value)?),
        }?;

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: "'system'.'benches'".to_string(),
            name: "benches".to_string(),
            meta: TableMeta {
                schema: TableSchemaRefExt::create(vec![
                    TableField::new(
                        "min_throughput_per_object",
                        TableDataType::Number(NumberDataType::Float64),
                    ),
                    TableField::new(
                        "max_throughput_per_object",
                        TableDataType::Number(NumberDataType::Float64),
                    ),
                    TableField::new("total_size", TableDataType::Number(NumberDataType::UInt64)),
                    TableField::new(
                        "total_duration",
                        TableDataType::Number(NumberDataType::UInt64),
                    ),
                    TableField::new(
                        "latencies",
                        TableDataType::Array(Box::new(TableDataType::Number(
                            NumberDataType::UInt64,
                        ))),
                    ),
                ]),
                engine: String::from("BenchesTable"),
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(BenchesTable {
            table_info,
            arguments,
        }))
    }
}

#[async_trait::async_trait]
impl Table for BenchesTable {
    fn distribution_level(&self) -> DistributionLevel {
        self.arguments.level()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        _: Option<PushDownInfo>,
        _: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), match self.arguments.level() {
            DistributionLevel::Local => {
                Partitions::create(PartitionsShuffleKind::Seq, vec![Arc::new(Box::new(
                    BenchesTablePart,
                ))])
            }
            DistributionLevel::Cluster => Partitions::create(
                PartitionsShuffleKind::BroadcastCluster,
                vec![Arc::new(Box::new(BenchesTablePart))],
            ),
            DistributionLevel::Warehouse => Partitions::create(
                PartitionsShuffleKind::BroadcastWarehouse,
                vec![Arc::new(Box::new(BenchesTablePart))],
            ),
            DistributionLevel::Tenant => Partitions::create(
                PartitionsShuffleKind::BroadcastTenant,
                vec![Arc::new(Box::new(BenchesTablePart))],
            ),
        }))
    }

    fn table_args(&self) -> Option<TableArgs> {
        match serde_json::to_string(&self.arguments) {
            Err(_) => None,
            Ok(argument) => Some(TableArgs::new_positioned(vec![Scalar::String(argument)])),
        }
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        if plan.parts.partitions.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        }

        pipeline.add_source(
            |output| {
                AsyncSourcer::create(ctx.clone(), output, BenchesWorker {
                    argument: self.arguments.clone(),
                    finished: false,
                })
            },
            1,
        )
    }
}

pub struct BenchesWorker {
    argument: BenchesArguments,
    // mode: TestMode,
    // concurrency: usize,
    finished: bool,
}

#[async_trait::async_trait]
impl AsyncSource for BenchesWorker {
    const NAME: &'static str = "benches_worker";

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if !self.finished {
            self.finished = true;
            let operator = DataOperator::instance().operator();
            let metric = test(
                operator,
                self.argument.test_mode.clone(),
                self.argument.concurrency,
                self.argument.dir.clone(),
            )
            .await?;

            return Ok(Some(DataBlock::new_from_columns(vec![
                Float64Type::from_data(vec![metric.min_throughput_per_object]),
                Float64Type::from_data(vec![metric.max_throughput_per_object]),
                UInt64Type::from_data(vec![metric.total_size as u64]),
                UInt64Type::from_data(vec![metric.total_duration as u64]),
                ArrayType::upcast_column(ArrayType::<UInt64Type>::column_from_iter(
                    vec![UInt64Type::column_from_iter(
                        metric.latencies.into_iter(),
                        &[],
                    )]
                    .into_iter(),
                    &[],
                )),
            ])));
        }

        Ok(None)
    }
}

async fn test(
    operator: Operator,
    mode: TestMode,
    concurrency: usize,
    prefix: String,
) -> Result<TestMetric> {
    let (file_size, iteration) = match mode {
        TestMode::Read {
            file_size,
            iteration,
        } => (file_size, iteration),
        TestMode::Write {
            file_size,
            iteration,
        } => (file_size, iteration),
    };

    let content = Bytes::from(vec![234u8; file_size]);
    let operator = Arc::new(operator);
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(concurrency));
    let node_id = GlobalConfig::instance().query.node_id.clone();

    for task_id in 0..concurrency {
        let op = operator.clone();
        let content = content.clone();
        let mode = mode.clone();
        let barrier = barrier.clone();
        let node_id = node_id.clone();
        let prefix = prefix.clone();

        handles.push(databend_common_base::runtime::spawn(async move {
            let mut latencies = Vec::with_capacity(iteration);
            let _permit = barrier.wait().await;

            for i in 0..iteration {
                let object_path = format!("benchmark/{prefix}/{node_id}-{task_id}-{i}");
                let start = Instant::now();

                match mode {
                    TestMode::Read { .. } => {
                        let _ = op.read(&object_path).await?;
                    }
                    TestMode::Write { .. } => {
                        let _ = op.write(&object_path, content.clone()).await?;
                    }
                };

                latencies.push(start.elapsed().as_millis() as u64);
            }
            Ok::<_, ErrorCode>(latencies)
        }));
    }

    let mut all_latencies = Vec::with_capacity(concurrency * iteration);
    for handle in handles {
        all_latencies.extend(handle.await.unwrap()?);
    }

    let throughputs: Vec<f64> = all_latencies
        .iter()
        .map(|d| match *d <= 1000 {
            true => file_size as f64,
            false => (file_size as f64) / *d as f64,
        })
        .collect();

    Ok(TestMetric {
        min_throughput_per_object: throughputs.iter().fold(f64::INFINITY, |a, &b| a.min(b)),
        max_throughput_per_object: throughputs.iter().fold(0.0, |a, &b| a.max(b)),
        total_size: file_size * concurrency * iteration,
        total_duration: all_latencies.iter().cloned().sum::<u64>() as usize,
        latencies: all_latencies,
    })
}

impl TableFunction for BenchesTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
