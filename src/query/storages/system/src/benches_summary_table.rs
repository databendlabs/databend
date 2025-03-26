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
use serde::ser::SerializeStruct;
use serde::Serializer;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub static BENCHES_ACTION: &str = "/actions/benches";

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum TestMode {
    Read {
        file_size: usize,
        iteration: usize,
        read_write_ratio: usize,
    },
    Write {
        file_size: usize,
        iteration: usize,
    },
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
    pub error_count: usize,
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

            let Some(error_count) = Self::get_u64(&block, 4, index) else {
                continue;
            };

            let Some(latencies) = Self::get_u64_array(&block, 5, index) else {
                continue;
            };

            metrics.push(TestMetric {
                min_throughput_per_object,
                max_throughput_per_object,
                total_size: total_size as usize,
                total_duration: total_duration as usize,
                latencies,
                error_count: error_count as usize,
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
            UInt64Type::from_data(vec![self.error_count as u64]),
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
            totals,
            min_throughput_per_object,
            max_throughput_per_object,
        }
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
        let warehouse_nodes = ctx.get_warehouse_nodes().await?;

        if warehouse_nodes.nodes.is_empty() {
            return Ok(DataBlock::empty());
        }

        let mut worker = BenchWorker::create(
            warehouse_nodes.clone(),
            warehouse_nodes.nodes[0].clone(),
            DistributionLevel::Tenant,
        );

        worker.probe_bench().await?;

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
                read_write_ratio: multiple_write,
            } => (
                String::from("Read"),
                HashMap::from([
                    ("file_size", convert_byte_size(file_size as f64)),
                    ("iteration", convert_number_size(iteration as f64)),
                    ("multiple_write", multiple_write.to_string()),
                    ("concurrency", concurrency.to_string()),
                ]),
            ),
            TestMode::Write {
                file_size,
                iteration,
            } => (
                String::from("Write"),
                HashMap::from([
                    ("concurrency", concurrency.to_string()),
                    ("file_size", convert_byte_size(file_size as f64)),
                    ("iteration", convert_number_size(iteration as f64)),
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

        let latency = metric.latency();
        let throughput = metric.throughput();
        let score = match latency.p99 <= 0_f64 {
            true => f64::MAX,
            false => throughput.totals / latency.p99,
        };

        self.summary_items.push(TestSummary {
            case: display_case,
            level: display_level,
            case_arguments: display_arguments,
            throughput,
            latency,
            score,
        });

        Ok(())
    }

    async fn probe_bench(&mut self) -> Result<()> {
        let case = vec![
            // 64KB * 1000 * 128 = 8G data per node
            (64 * 1024, 1000, 128, 2),  // concurrency: W-128, R-256
            // (64 * 1024, 1000, 128, 4),  // concurrency: W-128, R-512
            // (64 * 1024, 1000, 128, 8),  // concurrency: W-128, R-1024
            // (64 * 1024, 1000, 128, 32), // concurrency: W-128, R-4096
            // (64 * 1024, 1000, 128, 64), // concurrency: W-128, R-8192
            // 1MB * 1000 * 8 = 8G data per node
            (1024 * 1024, 1000, 8, 32),  // concurrency: W-8, R-256
            // (1024 * 1024, 1000, 8, 64),  // concurrency: W-8, R-512
            // (1024 * 1024, 1000, 8, 128), // concurrency: W-8, R-1024
            // (1 * 1024 * 1024, 1000, 8, 512), // concurrency: W-8, R-4096
            // (1 * 1024 * 1024, 1000, 8, 1024), // concurrency: W-8, R-8192
            // 4MB * 1000 * 2 = 8G data per node
            (4 * 1024 * 1024, 1000, 2, 128), // concurrency: W-2, R-256
            // (4 * 1024 * 1024, 1000, 2, 256), // concurrency: W-2, R-512
            // (4 * 1024 * 1024, 1000, 2, 512), // concurrency: W-2, R-1024
            // (4 * 1024 * 1024, 1000, 2, 2048), // concurrency: W-2, R-4096
            // (4 * 1024 * 1024, 1000, 2, 4096), // concurrency: W-2, R-8192
            // 16MB * 256 * 2 = 8G data per node
            (16 * 1024 * 1024, 256, 2, 128), // concurrency: W-2, R-256
            // (16 * 1024 * 1024, 256, 2, 256), // concurrency: W-2, R-512
            // (16 * 1024 * 1024, 256, 2, 512), // concurrency: W-2, R-1024
            // (16 * 1024 * 1024, 256, 2, 2048), // concurrency: W-2, R-4096
            // (16 * 1024 * 1024, 256, 2, 4096), // concurrency: W-2, R-8192
        ];

        for (file_size, iteration, concurrency, read_write_ratio) in case {
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
                    read_write_ratio,
                },
                dir.clone(),
            )
            .await?;
        }

        Ok(())
    }

    pub fn finalize(self) -> Vec<TestSummary> {
        self.summary_items
    }
}

pub struct ThroughputSummary {
    totals: f64,
    min_throughput_per_object: f64,
    max_throughput_per_object: f64,
}

impl serde::Serialize for ThroughputSummary {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        let mut throughput = serializer.serialize_struct("ThroughputSummary", 3)?;
        throughput.serialize_field("totals", &format!("{}/s", convert_byte_size(self.totals)))?;
        throughput.serialize_field(
            "min_throughput_per_object",
            &format!("{}/s", convert_byte_size(self.min_throughput_per_object)),
        )?;
        throughput.serialize_field(
            "max_throughput_per_object",
            &format!("{}/s", convert_byte_size(self.max_throughput_per_object)),
        )?;
        throughput.end()
    }
}

pub struct LatencySummary {
    min_latency: f64,
    max_latency: f64,
    avg_latency: f64,
    variance: f64,
    p50: f64,
    p90: f64,
    p95: f64,
    p99: f64,
}

impl serde::Serialize for LatencySummary {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        let mut latency_summary = serializer.serialize_struct("LatencySummary", 8)?;

        latency_summary.serialize_field("min_latency", &match self.min_latency.is_nan() {
            true => "N/A".to_string(),
            false => format!("{:.3}", self.min_latency),
        })?;

        latency_summary.serialize_field("max_latency", &match self.max_latency.is_nan() {
            true => "N/A".to_string(),
            false => format!("{:.3}", self.max_latency),
        })?;

        latency_summary.serialize_field("avg_latency", &match self.avg_latency.is_nan() {
            true => "N/A".to_string(),
            false => format!("{:.3}", self.avg_latency),
        })?;

        latency_summary.serialize_field("variance", &match self.variance.is_nan() {
            true => "N/A".to_string(),
            false => format!("{:.3}", self.variance),
        })?;

        latency_summary.serialize_field("p50", &match self.p50.is_nan() {
            true => "N/A".to_string(),
            false => format!("{:.3}", self.p50),
        })?;

        latency_summary.serialize_field("p90", &match self.p90.is_nan() {
            true => "N/A".to_string(),
            false => format!("{:.3}", self.p90),
        })?;

        latency_summary.serialize_field("p95", &match self.p95.is_nan() {
            true => "N/A".to_string(),
            false => format!("{:.3}", self.p95),
        })?;

        latency_summary.serialize_field("p99", &match self.p99.is_nan() {
            true => "N/A".to_string(),
            false => format!("{:.3}", self.p99),
        })?;

        latency_summary.end()
    }
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
            min_latency: min,
            max_latency: max,
            avg_latency: avg,
            variance,
            p50: Self::percentile(&sorted_data, 50.0),
            p90: Self::percentile(&sorted_data, 90.0),
            p95: Self::percentile(&sorted_data, 95.0),
            p99: Self::percentile(&sorted_data, 99.0),
        }
    }

    fn empty_summary() -> Self {
        Self {
            min_latency: f64::NAN,
            max_latency: f64::NAN,
            avg_latency: f64::NAN,
            variance: f64::NAN,
            p50: f64::NAN,
            p90: f64::NAN,
            p95: f64::NAN,
            p99: f64::NAN,
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
    case_arguments: HashMap<&'static str, String>,

    latency: LatencySummary,
    throughput: ThroughputSummary,
    score: f64,
}
