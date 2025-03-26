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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_base::base::GlobalUniqName;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::cluster_info::FlightParams;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::VariantType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_types::NodeInfo;
use databend_common_storage::DataOperator;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub static BENCHES_ACTION: &str = "/actions/benches";

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum TestMode {
    Read { file_size: usize, iteration: usize },
    Write { file_size: usize, iteration: usize },
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct BenchesArguments {
    pub level: String,
    pub test_mode: TestMode,
    pub concurrency: usize,
    pub dir: String,
}

impl BenchesArguments {
    pub fn level(&self) -> DistributionLevel {
        match self.level.to_lowercase().as_str() {
            "local" => DistributionLevel::Local,
            "cluster" => DistributionLevel::Cluster,
            "warehouse" => DistributionLevel::Warehouse,
            "tenant" => DistributionLevel::Tenant,
            _ => unreachable!(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Default)]
pub struct TestMetric {
    pub min_throughput_per_object: f64,
    pub max_throughput_per_object: f64,
    pub total_size: usize,
    pub total_duration: usize,
    pub latencies: Vec<u64>,
}

impl TestMetric {
    fn get_f64(block: &DataBlock, column: usize, row: usize) -> Option<f64> {
        let column = block.get_by_offset(column);
        let value = column.value.index(row)?;
        value.as_number()?.float_to_f64()
    }

    fn get_u64(block: &DataBlock, column: usize, row: usize) -> Option<u64> {
        let column = block.get_by_offset(column);
        let value = column.value.index(row)?;
        value.as_number()?.as_u_int64().cloned()
    }

    fn get_u64_array(block: &DataBlock, column: usize, row: usize) -> Option<Vec<u64>> {
        let column = block.get_by_offset(column);
        let scalar_ref = column.value.index(row)?;
        let array_value = scalar_ref.as_array()?;
        Some(array_value.as_number()?.as_u_int64()?.to_vec())
    }

    pub fn from_block(block: DataBlock) -> Vec<Self> {
        let mut metrics = Vec::with_capacity(block.num_rows());
        for index in 0..block.num_rows() {
            let Some(min_throughput_per_object) = Self::get_f64(&block, 0, index) else {
                continue;
            };

            let Some(max_throughput_per_object) = Self::get_f64(&block, 1, index) else {
                continue;
            };

            let Some(total_size) = Self::get_u64(&block, 2, index) else {
                continue;
            };

            let Some(total_duration) = Self::get_u64(&block, 3, index) else {
                continue;
            };

            let Some(latencies) = Self::get_u64_array(&block, 4, index) else {
                continue;
            };

            metrics.push(TestMetric {
                min_throughput_per_object,
                max_throughput_per_object,
                total_size: total_size as usize,
                total_duration: total_duration as usize,
                latencies,
            })
        }

        metrics
    }

    pub fn to_block(self) -> DataBlock {
        DataBlock::new_from_columns(vec![
            Float64Type::from_data(vec![self.min_throughput_per_object]),
            Float64Type::from_data(vec![self.max_throughput_per_object]),
            UInt64Type::from_data(vec![self.total_size as u64]),
            UInt64Type::from_data(vec![self.total_duration as u64]),
            ArrayType::upcast_column(ArrayType::<UInt64Type>::column_from_iter(
                vec![UInt64Type::column_from_iter(self.latencies.into_iter(), &[])].into_iter(),
                &[],
            )),
        ])
    }

    pub fn merge(mut self, other: Self) -> Self {
        self.min_throughput_per_object = self
            .min_throughput_per_object
            .min(other.min_throughput_per_object);
        self.max_throughput_per_object = self
            .max_throughput_per_object
            .min(other.max_throughput_per_object);
        self.total_size += other.total_size;
        self.total_duration += other.total_duration;
        self.latencies.extend(other.latencies);
        self
    }

    pub fn throughput(&self) -> ThroughputSummary {
        let totals = match self.total_duration <= 1000 {
            true => self.total_size as f64,
            false => self.total_size as f64 / ((self.total_duration * 1000) as f64),
        };

        let min_throughput_per_object = self.min_throughput_per_object / 1000_f64;
        let max_throughput_per_object = self.max_throughput_per_object / 1000_f64;
        ThroughputSummary {
            totals: format!("{}/s", convert_byte_size(totals)),
            min_throughput_per_object: format!(
                "{}/s",
                convert_byte_size(min_throughput_per_object)
            ),
            max_throughput_per_object: format!(
                "{}/s",
                convert_byte_size(max_throughput_per_object)
            ),
        }
    }

    pub fn score(&self) -> f64 {
        0_f64
    }

    pub fn latency(&self) -> LatencySummary {
        let seconds_data: Vec<f64> = self
            .latencies
            .iter()
            .map(|&ms| ms as f64 / 1000.0)
            .collect();
        LatencySummary::from_data(seconds_data)
    }
}

pub struct BenchesSummaryTable {
    table_info: TableInfo,
}

impl BenchesSummaryTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            // node, cluster, warehouse, tenant
            TableField::new("bench_level", TableDataType::String),
            // list, read, write, read_and_write
            TableField::new("bench_type", TableDataType::String),
            // read, write, read_and_write: (64K，1M，4M，16M)
            // concurrency: 128, 256, 512, 1024, 2048, 4096, 8192
            // list: 10, 100, 1000
            TableField::new("bench_arguments", TableDataType::Variant),
            // {"min_throughput_per_object": "{:.2} MB/s", "max_throughput_per_object": "{:.2} MB/s", summary: "{:.2} MB/s"}
            TableField::new("throughput", TableDataType::Variant),
            // latency: {"min_latency": "{:.2}Ms", "max_latency": "{:.2}Ms", "avg_latency":"{:.2}Ms", "variance": "", "p50", "p90", "p95", "p99"}
            TableField::new("latency", TableDataType::Variant),
            // throughput / p99
            TableField::new("score", TableDataType::Number(NumberDataType::Float64)),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'benches_summary'".to_string(),
            name: "benches_summary".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemBenchesSummary".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}

#[async_trait::async_trait]
impl AsyncSystemTable for BenchesSummaryTable {
    const NAME: &'static str = "system.benches_summary";

    const DISTRIBUTION_LEVEL: DistributionLevel = DistributionLevel::Local;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant_nodes = ctx.get_tenant_nodes().await?;

        if tenant_nodes.nodes.is_empty() {
            return Ok(DataBlock::empty());
        }

        let mut worker = BenchWorker::create(
            tenant_nodes.clone(),
            tenant_nodes.nodes[0].clone(),
            DistributionLevel::Tenant,
        );

        worker.probe_concurrency(100).await?;

        let summary_items = worker.finalize();

        let mut display_case = Vec::with_capacity(summary_items.len());
        let mut display_level = Vec::with_capacity(summary_items.len());
        let mut display_argument = Vec::with_capacity(summary_items.len());
        let mut display_throughput = Vec::with_capacity(summary_items.len());
        let mut display_latency = Vec::with_capacity(summary_items.len());
        let mut display_score = Vec::with_capacity(summary_items.len());

        for summary_item in summary_items {
            display_case.push(summary_item.case);
            display_level.push(summary_item.level);
            display_argument.push(serde_json::to_vec(&summary_item.case_arguments).unwrap());
            display_throughput.push(serde_json::to_vec(&summary_item.throughput).unwrap());
            display_latency.push(serde_json::to_vec(&summary_item.latency).unwrap());
            display_score.push(summary_item.score);
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(display_level),
            StringType::from_data(display_case),
            VariantType::from_data(display_argument),
            VariantType::from_data(display_throughput),
            VariantType::from_data(display_latency),
            Float64Type::from_data(display_score),
        ]))
    }
}

struct BenchWorker {
    cluster: Arc<Cluster>,
    level: DistributionLevel,
    running_node: Arc<NodeInfo>,
    summary_items: Vec<TestSummary>,
}

impl BenchWorker {
    pub fn create(cluster: Arc<Cluster>, node: Arc<NodeInfo>, level: DistributionLevel) -> Self {
        BenchWorker {
            cluster,
            level,
            running_node: node,
            summary_items: vec![],
        }
    }
    async fn run(&mut self, concurrency: usize, test_mode: TestMode, dir: String) -> Result<()> {
        let mut messages = HashMap::with_capacity(1);

        let req_level = match self.level {
            DistributionLevel::Local => String::from("local"),
            DistributionLevel::Cluster => String::from("cluster"),
            DistributionLevel::Warehouse => String::from("warehouse"),
            DistributionLevel::Tenant => String::from("tenant"),
        };

        let (display_case, display_arguments) = match test_mode {
            TestMode::Read {
                file_size,
                iteration,
            } => (
                String::from("Read"),
                HashMap::from([
                    ("file_size".to_string(), convert_byte_size(file_size as f64)),
                    (
                        "iteration".to_string(),
                        convert_number_size(iteration as f64),
                    ),
                ]),
            ),
            TestMode::Write {
                file_size,
                iteration,
            } => (
                String::from("Write"),
                HashMap::from([
                    ("file_size".to_string(), convert_byte_size(file_size as f64)),
                    (
                        "iteration".to_string(),
                        convert_number_size(iteration as f64),
                    ),
                ]),
            ),
        };

        messages.insert(self.running_node.id.clone(), BenchesArguments {
            test_mode,
            concurrency,
            level: req_level,
            dir,
        });

        let mut response = self
            .cluster
            .do_action::<_, Vec<TestMetric>>(BENCHES_ACTION, messages, FlightParams {
                timeout: 3 * 60 * 60 * 24,
                retry_times: 0,
                retry_interval: 0,
            })
            .await?;

        let Some(response) = response.remove(&self.running_node.id) else {
            return Err(ErrorCode::Internal(format!(
                "Not found {} response",
                self.running_node.id
            )));
        };

        let Some(metric) = response.into_iter().reduce(TestMetric::merge) else {
            return Err(ErrorCode::Internal(format!(
                "Not found {} response",
                self.running_node.id
            )));
        };

        let display_level = match self.level {
            DistributionLevel::Local => format!("Local({:?})", self.running_node.id),
            DistributionLevel::Cluster => format!("Cluster({:?})", self.running_node.cluster_id),
            DistributionLevel::Warehouse => {
                format!("Cluster({:?})", self.running_node.warehouse_id)
            }
            DistributionLevel::Tenant => String::from("Tenant"),
        };

        self.summary_items.push(TestSummary {
            case: display_case,
            level: display_level,
            case_arguments: display_arguments,
            throughput: metric.throughput(),
            latency: metric.latency(),
            score: metric.score(),
        });

        Ok(())
    }

    async fn probe_file_size(&mut self, concurrency: usize, iteration: usize) -> Result<()> {
        for file_size in [
            64 * 1024,
            // 1 * 1024 * 1024,
            // 4 * 1024 * 1024,
            // 16 * 1024 * 1024,
        ] {
            let dir = GlobalUniqName::unique();
            self.run(
                concurrency,
                TestMode::Write {
                    file_size,
                    iteration,
                },
                dir.clone(),
            )
            .await?;
            self.run(
                concurrency,
                TestMode::Read {
                    file_size,
                    iteration,
                },
                dir.clone(),
            )
            .await?;

            let operator = DataOperator::instance().operator();
            operator.remove_all(&format!("benchmark/{}/", dir)).await?;
        }

        Ok(())
    }

    pub async fn probe_concurrency(&mut self, iteration: usize) -> Result<()> {
        for concurrency in [3 /* 128, 256, 512, 1024, 2048, 4096, 8192 */] {
            self.probe_file_size(concurrency, iteration).await?;
        }

        Ok(())
    }

    pub fn finalize(self) -> Vec<TestSummary> {
        self.summary_items
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ThroughputSummary {
    totals: String,
    min_throughput_per_object: String,
    max_throughput_per_object: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct LatencySummary {
    min_latency: String,
    max_latency: String,
    avg_latency: String,
    variance: String,
    p50: String,
    p90: String,
    p95: String,
    p99: String,
}

impl LatencySummary {
    pub fn from_data(mut sorted_data: Vec<f64>) -> Self {
        if sorted_data.is_empty() {
            return Self::empty_summary();
        }

        sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let min = sorted_data[0];
        let max = *sorted_data.last().unwrap();
        let avg = sorted_data.iter().sum::<f64>() / sorted_data.len() as f64;
        let variance =
            sorted_data.iter().map(|x| (x - avg).powi(2)).sum::<f64>() / sorted_data.len() as f64;

        Self {
            min_latency: format!("{:.3}", min),
            max_latency: format!("{:.3}", max),
            avg_latency: format!("{:.3}", avg),
            variance: format!("{:.3}", variance),
            p50: format!("{:.3}", Self::percentile(&sorted_data, 50.0)),
            p90: format!("{:.3}", Self::percentile(&sorted_data, 90.0)),
            p95: format!("{:.3}", Self::percentile(&sorted_data, 95.0)),
            p99: format!("{:.3}", Self::percentile(&sorted_data, 99.0)),
        }
    }

    fn empty_summary() -> Self {
        Self {
            min_latency: "N/A".to_string(),
            max_latency: "N/A".to_string(),
            avg_latency: "N/A".to_string(),
            variance: "N/A".to_string(),
            p50: "N/A".to_string(),
            p90: "N/A".to_string(),
            p95: "N/A".to_string(),
            p99: "N/A".to_string(),
        }
    }

    fn percentile(sorted_data: &[f64], p: f64) -> f64 {
        let n = sorted_data.len();
        let rank = (p / 100.0) * (n - 1) as f64;
        let i = rank.floor() as usize;
        let fraction = rank - i as f64;

        if i >= n - 1 {
            sorted_data[n - 1]
        } else {
            sorted_data[i] + fraction * (sorted_data[i + 1] - sorted_data[i])
        }
    }
}

pub struct TestSummary {
    level: String,
    case: String,
    case_arguments: HashMap<String, String>,

    latency: LatencySummary,
    throughput: ThroughputSummary,
    score: f64,
}
