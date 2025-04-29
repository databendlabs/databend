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
use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::SExprVisitor;
use databend_common_sql::optimizer::ir::VisitAction;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::Statistics;
use databend_common_sql::BaseTableColumn;
use databend_common_sql::ColumnEntry;
use databend_common_sql::FormatOptions;
use databend_common_sql::IndexType;
use databend_common_sql::MetadataRef;
use databend_common_storage::Datum;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;
use goldenfile::Mint;
use serde::Deserialize;
use serde::Serialize;

use crate::sql::planner::optimizer::test_utils::execute_sql;
use crate::sql::planner::optimizer::test_utils::optimize_plan;
use crate::sql::planner::optimizer::test_utils::raw_plan;

/// YAML representation of a test case
#[derive(Debug, Serialize, Deserialize)]
struct YamlTestCase {
    name: String,
    description: Option<String>,
    sql: String,
    #[serde(default)]
    table_statistics: HashMap<String, YamlTableStatistics>,
    #[serde(default)]
    column_statistics: HashMap<String, YamlColumnStatistics>,
    #[serde(default)]
    statistics_file: Option<String>,
    good_plan: Option<String>,
}

/// YAML representation of table statistics
#[derive(Debug, Serialize, Deserialize, Clone)]
struct YamlTableStatistics {
    num_rows: Option<u64>,
    data_size: Option<u64>,
    data_size_compressed: Option<u64>,
    index_size: Option<u64>,
    number_of_blocks: Option<u64>,
    number_of_segments: Option<u64>,
}

/// YAML representation of column statistics
#[derive(Debug, Serialize, Deserialize, Clone)]
struct YamlColumnStatistics {
    min: Option<serde_json::Value>,
    max: Option<serde_json::Value>,
    ndv: Option<u64>,
    null_count: Option<u64>,
}

struct TestCase {
    pub name: &'static str,
    pub sql: &'static str,
    pub table_statistics: HashMap<String, YamlTableStatistics>,
    pub column_statistics: HashMap<String, YamlColumnStatistics>,
}

/// Setup TPC-DS tables with required schema
async fn setup_tpcds_tables(ctx: &Arc<QueryContext>) -> Result<()> {
    // Get the base path for table definitions
    let base_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/it/sql/planner/optimizer/data/tables");

    // Check if the directory exists
    if !base_path.exists() {
        return Err(ErrorCode::UnknownException(format!(
            "Tables directory not found at {:?}",
            base_path
        )));
    }

    // Read all SQL files from the tables directory
    for entry in fs::read_dir(&base_path)? {
        let entry = entry?;
        let path = entry.path();

        // Only process SQL files
        if path.is_file() && path.extension().is_some_and(|ext| ext == "sql") {
            // Extract table name from filename (without extension)
            if let Some(file_stem) = path.file_stem() {
                if let Some(table_name) = file_stem.to_str() {
                    let sql = fs::read_to_string(&path)?;
                    println!("Creating table: {}", table_name);
                    execute_sql(ctx, &sql).await?;
                }
            }
        }
    }

    Ok(())
}

/// Convert a YAML test case to a TestCase
fn create_test_case(yaml: YamlTestCase, base_path: &Path) -> Result<TestCase> {
    let mut table_statistics = yaml.table_statistics;
    let mut column_statistics = yaml.column_statistics;

    // If there's a statistics file reference, load it and merge with inline statistics
    if let Some(stats_file) = yaml.statistics_file {
        let (file_table_stats, file_column_stats) = load_statistics_file(base_path, &stats_file)?;

        // Merge table statistics (file stats take precedence)
        for (table_name, stats) in file_table_stats {
            table_statistics.insert(table_name, stats);
        }

        // Merge column statistics (file stats take precedence)
        for (column_name, stats) in file_column_stats {
            column_statistics.insert(column_name, stats);
        }
    }

    Ok(TestCase {
        name: Box::leak(yaml.name.into_boxed_str()),
        sql: Box::leak(yaml.sql.into_boxed_str()),
        table_statistics,
        column_statistics,
    })
}

/// Convert a JSON value to a Datum
fn convert_to_datum(value: &Option<serde_json::Value>) -> Option<Datum> {
    if let Some(val) = value {
        match val {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    // Convert to i64 datum using from_scalar
                    return Datum::from_scalar(Scalar::Number(NumberScalar::Int64(i)));
                } else if let Some(f) = n.as_f64() {
                    // Convert to f64 datum using from_scalar
                    return Datum::from_scalar(Scalar::Number(NumberScalar::Float64(f.into())));
                }
            }
            serde_json::Value::String(s) => {
                // Convert to string datum using from_scalar
                return Datum::from_scalar(Scalar::String(s.clone()));
            }
            // Add other type conversions as needed
            _ => {}
        }
    }
    None
}

/// Load statistics from an external YAML file
fn load_statistics_file(
    base_path: &Path,
    file_name: &str,
) -> Result<(
    HashMap<String, YamlTableStatistics>,
    HashMap<String, YamlColumnStatistics>,
)> {
    #[derive(Debug, Serialize, Deserialize)]
    struct StatisticsFile {
        table_statistics: HashMap<String, YamlTableStatistics>,
        column_statistics: HashMap<String, YamlColumnStatistics>,
    }

    // Statistics files are located in the statistics directory
    let stats_path = base_path.join("statistics").join(file_name);
    if !stats_path.exists() {
        return Err(ErrorCode::Internal(format!(
            "Statistics file not found: {}",
            stats_path.display()
        )));
    }

    let content = fs::read_to_string(&stats_path)?;
    let stats: StatisticsFile = serde_yaml::from_str(&content)
        .map_err(|e| ErrorCode::Internal(format!("Failed to parse statistics YAML: {}", e)))?;

    Ok((stats.table_statistics, stats.column_statistics))
}

/// Load test cases from YAML files
fn load_test_cases(base_path: &Path) -> Result<Vec<TestCase>> {
    let cases_dir = base_path.join("cases");
    let mut test_cases = Vec::new();

    if !cases_dir.exists() {
        return Ok(Vec::new());
    }

    let mut entrys = fs::read_dir(cases_dir)?
        .into_iter()
        .collect::<io::Result<Vec<_>>>()?;
    entrys.sort_by_key(fs::DirEntry::file_name);
    for entry in entrys {
        let path = entry.path();

        if path.is_file()
            && path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml")
        {
            let content = fs::read_to_string(&path)?;
            let yaml_test_case: YamlTestCase = serde_yaml::from_str(&content)
                .map_err(|e| ErrorCode::Internal(format!("Failed to parse YAML: {}", e)))?;
            let test_case = create_test_case(yaml_test_case, base_path)?;
            test_cases.push(test_case);
        }
    }

    Ok(test_cases)
}

fn apply_scan_stats(
    plan: &mut Plan,
    table_statistics: HashMap<String, YamlTableStatistics>,
    column_statistics: HashMap<String, YamlColumnStatistics>,
) -> Result<()> {
    if let Plan::Query {
        s_expr, metadata, ..
    } = plan
    {
        let mut visitor = ScanStatsVisitor {
            metadata,
            table_statistics: &table_statistics,
            column_statistics: &column_statistics,
        };

        if let Some(new_s_expr) = s_expr.accept(&mut visitor)? {
            *s_expr = Box::new(new_s_expr);
        }
    }

    Ok(())
}

struct ScanStatsVisitor<'a> {
    metadata: &'a MetadataRef,
    table_statistics: &'a HashMap<String, YamlTableStatistics>,
    column_statistics: &'a HashMap<String, YamlColumnStatistics>,
}

impl<'a> SExprVisitor for ScanStatsVisitor<'a> {
    fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
        if let RelOperator::Scan(scan) = expr.plan() {
            let table_index = scan.table_index;
            let metadata_guard = self.metadata.read();
            let table_entry = metadata_guard.table(table_index);
            let table_name = table_entry.name();

            if let Some(stats) = self.table_statistics.get(table_name) {
                let mut column_stats = HashMap::new();
                let columns = metadata_guard.columns_by_table_index(table_index);
                for (column_idx, column) in columns.iter().enumerate() {
                    if let ColumnEntry::BaseTableColumn(BaseTableColumn { column_name, .. }) =
                        column
                    {
                        let column_name = format!("{}.{}", table_name, column_name);
                        if let Some(col_stats_option) = self.column_statistics.get(&column_name) {
                            let col_stats = BasicColumnStatistics {
                                min: convert_to_datum(&col_stats_option.min),
                                max: convert_to_datum(&col_stats_option.max),
                                ndv: col_stats_option.ndv,
                                null_count: col_stats_option.null_count.unwrap_or(0),
                            };
                            column_stats.insert(column_idx as IndexType, Some(col_stats));
                        } else {
                            println!(
                                "Column statistics not found from yaml for column: {}",
                                column_name
                            );
                        }
                    }
                }

                let table_stats = TableStatistics {
                    num_rows: stats.num_rows,
                    data_size: stats.data_size,
                    data_size_compressed: stats.data_size_compressed,
                    index_size: stats.index_size,
                    number_of_blocks: stats.number_of_blocks,
                    number_of_segments: stats.number_of_segments,
                };

                let new_stats = Statistics {
                    table_stats: Some(table_stats),
                    column_stats,
                    histograms: HashMap::new(),
                };

                let mut new_scan = scan.clone();
                new_scan.statistics = Arc::new(new_stats.clone());

                let new_plan = Arc::new(RelOperator::Scan(new_scan));
                let new_expr = expr.replace_plan(new_plan);
                println!(
                    "Set statistics for table: {}, table_idx:{}, new stats:\n{:#?}",
                    table_name,
                    table_index,
                    new_stats.clone()
                );

                return Ok(VisitAction::Replace(new_expr));
            } else {
                println!(
                    "Table statistics not found from yaml for table: {}, table_idx: {}",
                    table_name, table_index
                );
            }
        }

        Ok(VisitAction::Continue)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tpcds_optimizer() -> Result<()> {
    // Create a test fixture with a query context
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // Setup tables needed for TPC-DS queries
    setup_tpcds_tables(&ctx).await?;

    // Load test cases from YAML files
    let base_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/it/sql/planner/optimizer/data");
    let mut mint = Mint::new("tests/it/sql/planner/optimizer/data/cases");

    let tests = load_test_cases(&base_path)?;

    if tests.is_empty() {
        println!("No test cases found in {:?}", base_path);
        return Ok(());
    }

    // Run all test cases
    for test in tests {
        println!("\n\n========== Testing: {} ==========", test.name);
        let file = &mut mint.new_goldenfile(format!("{}.txt", test.name)).unwrap();

        // Parse SQL to get raw plan
        let mut raw_plan = raw_plan(&ctx, test.sql).await?;

        // Set statistics for the plan
        apply_scan_stats(&mut raw_plan, test.table_statistics, test.column_statistics)?;

        // Print and verify raw plan
        let format_option = FormatOptions { verbose: false };
        let raw_plan_str = raw_plan.format_indent(format_option)?;

        // Verify raw plan matches expected
        writeln!(file, "Raw plan:\n{}", raw_plan_str)?;

        // Optimize the plan
        let optimized_plan = optimize_plan(&ctx, raw_plan).await?;
        let optimized_plan_str = optimized_plan.format_indent(Default::default())?;
        writeln!(file, "Optimized plan:\n{}", optimized_plan_str)?;

        println!("✅ {} test passed!", test.name);
    }

    Ok(())
}
