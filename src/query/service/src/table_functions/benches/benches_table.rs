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
use std::time::Instant;

use bytes::Bytes;
use chrono::DateTime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataBlock;
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

#[typetag::serde(name = "bench")]
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
                    TableField::new("error_count", TableDataType::Number(NumberDataType::UInt64)),
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
                    runtime: Runtime::with_default_worker_threads()?,
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
    runtime: Runtime,
}

#[async_trait::async_trait]
impl AsyncSource for BenchesWorker {
    const NAME: &'static str = "benches_worker";

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if !self.finished {
            self.finished = true;
            let operator = DataOperator::instance().operator();
            let metric = test(
                &self.runtime,
                operator,
                self.argument.test_mode.clone(),
                self.argument.concurrency,
                self.argument.dir.clone(),
            )
            .await?;

            return Ok(Some(metric.to_block()));
        }

        Ok(None)
    }
}

async fn test(
    runtime: &Runtime,
    operator: Operator,
    mode: TestMode,
    raw_concurrency: usize,
    prefix: String,
) -> Result<TestMetric> {
    let (file_size, iteration, concurrency) = match mode {
        TestMode::Read {
            file_size,
            iteration,
            read_write_ratio,
        } => (file_size, iteration, raw_concurrency * read_write_ratio),
        TestMode::Write {
            file_size,
            iteration,
        } => (file_size, iteration, raw_concurrency),
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
        let task_id = task_id % raw_concurrency;

        handles.push(runtime.spawn(async move {
            let mut latencies = Vec::with_capacity(iteration);
            let _permit = barrier.wait().await;

            let mut error_count = 0;
            for i in 0..iteration {
                let object_path = format!("benchmark/{prefix}/{node_id}-{task_id}-{i}");
                let start = Instant::now();

                match mode {
                    TestMode::Read { .. } => {
                        if op.read(&object_path).await.is_err() {
                            error_count += 1;
                        }

                        latencies.push(start.elapsed().as_millis() as u64);
                    }
                    TestMode::Write { .. } => {
                        // We will read it later
                        if op.write(&object_path, content.clone()).await.is_err() {
                            error_count += 1;
                        }

                        latencies.push(start.elapsed().as_millis() as u64);
                    }
                };
            }
            Ok::<_, ErrorCode>((error_count, latencies))
        }));
    }

    let mut totals_error_code = 0;
    let mut all_latencies = Vec::with_capacity(concurrency * iteration);
    for handle in handles {
        let (error_count, latencies) = handle.await.unwrap()?;
        totals_error_code += error_count;
        all_latencies.extend(latencies);
    }

    if let TestMode::Read { .. } = mode {
        // Safe delete after read
        for task_id in 0..raw_concurrency {
            if let Err(err) = operator
                .delete_iter(
                    (0..iteration)
                        .map(|idx| format!("benchmark/{prefix}/{node_id}-{task_id}-{idx}")),
                )
                .await
            {
                log::warn!("Cannot remove benchmark file {:?}", err);
            }
        }
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
        error_count: totals_error_code,
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
