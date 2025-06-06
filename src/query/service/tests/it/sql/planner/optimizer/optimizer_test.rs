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
use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_column::binview::ViewType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::F64;
use databend_common_expression::Scalar;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimize;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::SExprVisitor;
use databend_common_sql::optimizer::ir::VisitAction;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::Statistics;
use databend_common_sql::BaseTableColumn;
use databend_common_sql::ColumnEntry;
use databend_common_sql::FormatOptions;
use databend_common_sql::IndexType;
use databend_common_sql::Metadata;
use databend_common_sql::MetadataRef;
use databend_common_storage::Datum;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;
use goldenfile::Mint;
use serde::Deserialize;
use serde::Serialize;

use crate::sql::planner::optimizer::test_utils::execute_sql;
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
    #[serde(default)]
    auto_statistics: bool,
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
    pub auto_statistics: bool,
    pub original_file_name: String, // Store the original file name without extension
}

/// Enum representing different types of directories in the test data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DirectoryType {
    Tables,
    Cases,
    Statistics,
    Results,
}

/// Struct to hold all test files organized by directory type
struct TestFiles {
    /// Files organized by directory type and subdirectory
    files: HashMap<DirectoryType, HashMap<String, Vec<PathBuf>>>,
    /// Base path for all test data
    base_path: PathBuf,
    /// Subdirectory filter used when collecting files
    subdir_filter: Option<String>,
}

impl TestFiles {
    /// Create a new TestFiles instance and collect all files
    fn new(base_path: &Path, subdir_filter: Option<&str>) -> Result<Self> {
        // Create a map for each directory type
        let mut files = HashMap::new();
        files.insert(DirectoryType::Tables, HashMap::new());
        files.insert(DirectoryType::Cases, HashMap::new());
        files.insert(DirectoryType::Statistics, HashMap::new());
        files.insert(DirectoryType::Results, HashMap::new());

        // Create the TestFiles instance
        let mut test_files = Self {
            files,
            base_path: base_path.to_path_buf(),
            subdir_filter: subdir_filter.map(|s| s.to_string()),
        };

        // Collect files for each directory type
        Self::collect_directory_files(
            base_path,
            DirectoryType::Tables,
            &mut test_files.files,
            subdir_filter,
            &["sql"],
        )?;
        Self::collect_directory_files(
            base_path,
            DirectoryType::Cases,
            &mut test_files.files,
            subdir_filter,
            &["yaml", "yml"],
        )?;
        Self::collect_directory_files(
            base_path,
            DirectoryType::Statistics,
            &mut test_files.files,
            subdir_filter,
            &["yaml", "yml"],
        )?;
        Self::collect_directory_files(
            base_path,
            DirectoryType::Results,
            &mut test_files.files,
            subdir_filter,
            &["txt"],
        )?;

        Ok(test_files)
    }

    /// Collect files from a specific directory type
    fn collect_directory_files(
        base_path: &Path,
        dir_type: DirectoryType,
        files_map: &mut HashMap<DirectoryType, HashMap<String, Vec<PathBuf>>>,
        subdir_filter: Option<&str>,
        extensions: &[&str],
    ) -> Result<()> {
        // Get the directory name based on directory type
        let dir_name = match dir_type {
            DirectoryType::Tables => "tables",
            DirectoryType::Cases => "cases",
            DirectoryType::Statistics => "statistics",
            DirectoryType::Results => "results",
        };

        let dir_path = base_path.join(dir_name);
        if !dir_path.exists() {
            return Ok(());
        }

        // Determine the directory to scan based on subdir_filter
        let scan_dirs = if let Some(subdir) = subdir_filter {
            let subdir_path = dir_path.join(subdir);
            if !subdir_path.exists() {
                // If the specific subdirectory doesn't exist, return empty
                return Ok(());
            }
            vec![subdir_path]
        } else {
            // If no filter, scan all immediate subdirectories
            let mut subdirs = Vec::new();
            for entry in fs::read_dir(&dir_path)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    subdirs.push(path);
                }
            }
            // If no subdirectories found, use the main directory
            if subdirs.is_empty() {
                subdirs.push(dir_path);
            }
            subdirs
        };

        // Get the files map for this directory type
        let type_files = files_map.get_mut(&dir_type).unwrap();

        // Process each scan directory
        for scan_dir in scan_dirs {
            // Get subdirectory name (or "root" if it's the main directory)
            let subdir_name = if let Some(file_name) = scan_dir.file_name() {
                if let Some(name_str) = file_name.to_str() {
                    name_str.to_string()
                } else {
                    "root".to_string()
                }
            } else {
                "root".to_string()
            };

            // Create entry for this subdirectory if it doesn't exist
            let subdir_files = type_files.entry(subdir_name).or_default();

            // Collect all files with the specified extensions
            let mut collected_files = Vec::new();
            Self::collect_files(&scan_dir, &mut collected_files, extensions)?;

            // Sort files for consistent ordering
            collected_files.sort();

            // Add collected files to the map
            subdir_files.extend(collected_files);
        }

        Ok(())
    }

    /// Recursively collect files with specific extensions
    fn collect_files(dir: &Path, files: &mut Vec<PathBuf>, extensions: &[&str]) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                Self::collect_files(&path, files, extensions)?;
            } else if path.is_file()
                && path
                    .extension()
                    .is_some_and(|s| s.to_str().is_some_and(|s_str| extensions.contains(&s_str)))
            {
                files.push(path);
            }
        }

        Ok(())
    }

    /// Get files of a specific type and subdirectory
    fn get_files(&self, dir_type: DirectoryType, subdir: Option<&str>) -> Vec<PathBuf> {
        let type_files = match self.files.get(&dir_type) {
            Some(files) => files,
            None => return Vec::new(),
        };

        if let Some(subdir_name) = subdir {
            if let Some(files) = type_files.get(subdir_name) {
                files.clone()
            } else {
                Vec::new()
            }
        } else {
            // If no subdirectory specified, return all files from all subdirectories
            let mut all_files = Vec::new();
            for files in type_files.values() {
                all_files.extend(files.clone());
            }
            all_files.sort();
            all_files
        }
    }

}

/// Convert a YAML test case to a TestCase
fn create_test_case(yaml: YamlTestCase, base_path: &Path, file_path: &Path) -> Result<TestCase> {
    let mut table_statistics = yaml.table_statistics;
    let mut column_statistics = yaml.column_statistics;

    // If there's a statistics file reference, load it and merge with inline statistics
    if let Some(stats_file) = yaml.statistics_file {
        // Create a TestFiles instance for loading statistics
        let test_files = TestFiles::new(base_path, None)?;
        let (file_table_stats, file_column_stats) = load_statistics_file(&test_files, &stats_file)?;

        // Merge table statistics (file stats take precedence)
        for (table_name, stats) in file_table_stats {
            table_statistics.insert(table_name, stats);
        }

        // Merge column statistics (file stats take precedence)
        for (column_name, stats) in file_column_stats {
            column_statistics.insert(column_name, stats);
        }
    }

    // Extract the file name without extension to use for result file paths
    let file_name = file_path.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown")
        .to_string();

    Ok(TestCase {
        name: Box::leak(yaml.name.clone().into_boxed_str()),
        sql: Box::leak(yaml.sql.clone().into_boxed_str()),
        table_statistics,
        column_statistics,
        auto_statistics: yaml.auto_statistics,
        original_file_name: file_name,
    })
}

/// Setup tables with required schema using TestFiles
async fn setup_tables(ctx: &Arc<QueryContext>, test_files: &TestFiles) {
    // Get SQL files for table definitions from TestFiles
    let table_files =
        test_files.get_files(DirectoryType::Tables, test_files.subdir_filter.as_deref());

    // Process each SQL file to create tables
    for file_path in table_files {
        // Read the SQL file content
        let sql = match fs::read_to_string(&file_path) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Error reading table file {:?}: {}", file_path, e);
                continue;
            }
        };

        // Execute each SQL statement to create tables
        for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
            match execute_sql(ctx, statement).await {
                Ok(_) => println!("Created table: {}", file_path.display()),
                Err(e) => eprintln!("Error creating table {}: {}", file_path.display(), e),
            }
        }
    }
}



/// Load statistics from an external YAML file
fn load_statistics_file(
    test_files: &TestFiles,
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

    // Parse the file name to extract subdirectory path and file name
    let parts: Vec<&str> = file_name.split('/').collect();
    let (subdir_path, stats_file_name) = if parts.len() > 1 {
        // If there's a subdirectory path
        let name = parts.last().unwrap();
        let subdir = parts[..parts.len() - 1].join("/");
        (Some(subdir), name.to_string())
    } else {
        // If there's no subdirectory
        (None, file_name.to_string())
    };
    
    // Extract the file stem (without extension) for comparison
    let stats_file_stem = Path::new(&stats_file_name)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&stats_file_name);

    // Get all statistics files for the specified subdirectory
    let stats_files = test_files.get_files(DirectoryType::Statistics, subdir_path.as_deref());

    // First try exact match
    for path in &stats_files {
        if let Some(file_stem) = path.file_stem() {
            if let Some(name) = file_stem.to_str() {
                // Compare file stem to file stem (without extensions)
                if name == stats_file_stem {
                    let content = fs::read_to_string(path)?;
                    let stats: StatisticsFile = serde_yaml::from_str(&content).map_err(|e| {
                        ErrorCode::Internal(format!("Failed to parse statistics YAML: {}", e))
                    })?;

                    return Ok((stats.table_statistics, stats.column_statistics));
                }
            }
        }
    }

    // If exact match not found, try to find a file with numeric prefix
    for path in stats_files {
        if let Some(file_stem) = path.file_stem() {
            if let Some(name) = file_stem.to_str() {
                // Check if the file name contains our target name (ignoring numeric prefixes)
                // For example, "01_tpcds_100g.yaml" should match "tpcds_100g"
                // Compare file stem to file stem (without extensions)
                if name.ends_with(stats_file_stem) || name.contains(stats_file_stem) {
                    let content = fs::read_to_string(&path)?;
                    let stats: StatisticsFile = serde_yaml::from_str(&content).map_err(|e| {
                        ErrorCode::Internal(format!("Failed to parse statistics YAML: {}", e))
                    })?;

                    println!("Found statistics file with prefix: {}", path.display());
                    return Ok((stats.table_statistics, stats.column_statistics));
                }
            }
        }
    }

    // If we get here, the file wasn't found
    Err(ErrorCode::Internal(format!(
        "Statistics file not found: {} (also tried with numeric prefixes)",
        file_name
    )))
}

/// Load test cases from YAML files using TestFiles
fn load_test_cases(test_files: &TestFiles) -> Result<Vec<TestCase>> {
    // Get YAML test case files from TestFiles
    let yaml_files =
        test_files.get_files(DirectoryType::Cases, test_files.subdir_filter.as_deref());

    if yaml_files.is_empty() {
        if let Some(subdir) = &test_files.subdir_filter {
            println!("No test case files found for subdirectory: {}", subdir);
        } else {
            println!("No test case files found");
        }
        return Ok(Vec::new());
    }

    // Log what we're doing
    if let Some(subdir) = &test_files.subdir_filter {
        println!("Loading test cases from subdirectory: {}", subdir);
    } else {
        println!("Loading all test cases");
    }

    // Load test cases from YAML files
    let mut test_cases = Vec::new();
    let cases_dir = test_files.base_path.join("cases");

    for file in yaml_files {
        // Read the YAML file
        let yaml_content = fs::read_to_string(&file)?;

        // Parse the YAML content
        let yaml_test_case: YamlTestCase = serde_yaml::from_str(&yaml_content)
            .map_err(|e| ErrorCode::Internal(format!("Failed to parse YAML: {}", e)))?;

        // Convert to TestCase
        let mut test_case = create_test_case(yaml_test_case, &test_files.base_path, &file)?;

        // Extract subdirectory information from the path
        if let Ok(rel_path) = file.strip_prefix(&cases_dir) {
            let parent_path = rel_path.parent();
            if let Some(parent) = parent_path {
                if !parent.as_os_str().is_empty() {
                    // Store the subdirectory path as part of the test case name
                    let subdir = parent.to_string_lossy().to_string();
                    let test_name = test_case.name;
                    let full_name = format!("{}/{}", subdir, test_name);
                    test_case.name = Box::leak(full_name.into_boxed_str());
                }
            }
        }

        test_cases.push(test_case);
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
                                min: match convert_to_datum(&col_stats_option.min) {
                                    Some(v) => Some(v),
                                    None => {
                                        if column.data_type().is_floating() {
                                            Some(Datum::Float(F64::MIN))
                                        } else if column.data_type().is_signed_numeric() {
                                            Some(Datum::Int(i64::MIN))
                                        } else if column.data_type().is_unsigned_numeric() {
                                            Some(Datum::UInt(u64::MIN))
                                        } else {
                                            Some(Datum::Bytes(
                                                "\0\0\0\0\0\0\0\0".to_bytes().to_vec(),
                                            ))
                                        }
                                    }
                                },
                                max: match convert_to_datum(&col_stats_option.max) {
                                    Some(v) => Some(v),
                                    None => {
                                        if column.data_type().is_floating() {
                                            Some(Datum::Float(F64::MAX))
                                        } else if column.data_type().is_signed_numeric() {
                                            Some(Datum::Int(i64::MAX))
                                        } else if column.data_type().is_unsigned_numeric() {
                                            Some(Datum::UInt(u64::MAX))
                                        } else {
                                            Some(Datum::Bytes("\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}".to_bytes().to_vec()))
                                        }
                                    }
                                },
                                ndv: col_stats_option.ndv,
                                null_count: col_stats_option.null_count.unwrap_or(0),
                            };
                            column_stats.insert(column_idx as IndexType, Some(col_stats));
                        } else {
                            eprintln!(
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
                // println!(
                //     "Set statistics for table: {}, table_idx:{}, new stats:\n{:#?}",
                //     table_name,
                //     table_index,
                //     new_stats.clone()
                // );

                return Ok(VisitAction::Replace(new_expr));
            } else {
                eprintln!(
                    "Table statistics not found from yaml for table: {}, table_idx: {}",
                    table_name, table_index
                );
            }
        }

        Ok(VisitAction::Continue)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_optimizer() -> Result<()> {
    // Check if a subdirectory filter is specified via environment variable
    let subdir = std::env::var("TEST_SUBDIR").ok();
    let subdir_ref = subdir.as_deref();

    // Get the base path for test data
    let base_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/it/sql/planner/optimizer/data");

    // Collect all test files, filtered by subdirectory if specified
    let test_files = TestFiles::new(&base_path, subdir_ref)?;

    // Create a test fixture with a query context
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // Setup tables needed for queries using TestFiles
    setup_tables(&ctx, &test_files).await;

    // Load test cases from YAML files using TestFiles
    let tests = load_test_cases(&test_files)?;

    if tests.is_empty() {
        if let Some(subdir) = subdir_ref {
            println!("No test cases found for subdirectory: {}", subdir);
        } else {
            println!("No test cases found in {:?}", base_path);
        }
        return Ok(());
    }

    // Get the results directory path
    let results_dir = base_path.join("results");
    
    // Create a HashMap to store Mint instances for each subdirectory
    let mut subdirectory_mints: HashMap<String, Mint> = HashMap::new();
    
    // Create a root Mint instance for tests without subdirectory
    let mut root_mint = Mint::new(&results_dir);

    // Run all test cases
    for test in tests {
        let settings = ctx.get_settings();

        if !test.auto_statistics {
            settings.set_optimizer_skip_list("CollectStatisticsOptimizer".to_string())?;
        } else {
            settings.set_optimizer_skip_list("".to_string())?;
        }

        println!("\n\n========== Testing: {} ==========", test.name);

        // Split the test name to extract subdirectory and actual test name
        let parts: Vec<&str> = test.name.split('/').collect();
        let (subdir_path, _test_name) = if parts.len() > 1 {
            // If there's a subdirectory path
            let name = parts.last().unwrap();
            let subdir = parts[..parts.len() - 1].join("/");
            (Some(subdir), *name)
        } else {
            // If there's no subdirectory
            (None, test.name)
        };

        let result_path = format!("{}_raw.txt", test.original_file_name);
        println!("Test: {}, Subdirectory: {:?}, Original file name: {}, Result path: {}", 
                 test.name, subdir_path, test.original_file_name, result_path);
        
        // Get the appropriate Mint instance based on subdirectory
        let file = if let Some(subdir) = &subdir_path {
            // Get or create a Mint instance for this subdirectory
            if !subdirectory_mints.contains_key(subdir) {
                let subdir_results = results_dir.join(subdir);
                subdirectory_mints.insert(subdir.clone(), Mint::new(&subdir_results));
            }
            &mut subdirectory_mints.get_mut(subdir).unwrap().new_goldenfile(&result_path).unwrap()
        } else {
            &mut root_mint.new_goldenfile(&result_path).unwrap()
        };

        // Parse SQL to get raw plan
        let mut raw_plan = raw_plan(&ctx, test.sql).await?;

        // Set statistics for the plan
        apply_scan_stats(&mut raw_plan, test.table_statistics, test.column_statistics)?;

        // Print and verify raw plan
        let format_option = FormatOptions { verbose: false };
        let raw_plan_str = raw_plan.format_indent(format_option)?;

        // Verify raw plan matches expected
        writeln!(file, "{}", raw_plan_str)?;

        // Optimize the plan
        let metadata = match &raw_plan {
            Plan::Query { metadata, .. } => metadata.clone(),
            _ => {
                // If it's not a Query, we still need to provide a metadata, but log a warning
                eprintln!("Warning: Plan is not a Query variant, creating new metadata");
                Arc::new(parking_lot::RwLock::new(Metadata::default()))
            }
        };
        let optimized_plan = {
            let opt_ctx = OptimizerContext::new(ctx.clone(), metadata.clone())
                .set_enable_distributed_optimization(true)
                .set_enable_join_reorder(true)
                .set_enable_dphyp(true)
                .set_max_push_down_limit(10000)
                .set_enable_trace(true)
                .clone();

            optimize(opt_ctx, raw_plan).await?
        };

        // Generate optimized plan result path
        let result_path = format!("{}_optimized.txt", test.original_file_name);

        // Get the appropriate Mint instance based on subdirectory
        let file = if let Some(subdir) = &subdir_path {
            &mut subdirectory_mints.get_mut(subdir).unwrap().new_goldenfile(&result_path).unwrap()
        } else {
            &mut root_mint.new_goldenfile(&result_path).unwrap()
        };
        let optimized_plan_str = optimized_plan.format_indent(FormatOptions::default())?;
        writeln!(file, "{}", optimized_plan_str)?;
        if let Plan::Query {
            metadata,
            bind_context,
            s_expr,
            ..
        } = optimized_plan
        {
            let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx.clone(), false);
            let physical_plan = builder.build(&s_expr, bind_context.column_set()).await?;

            // Generate physical plan result path
            let result_path = format!("{}_physical.txt", test.original_file_name);

            // Get the appropriate Mint instance based on subdirectory
            let file = if let Some(subdir) = &subdir_path {
                &mut subdirectory_mints.get_mut(subdir).unwrap().new_goldenfile(&result_path).unwrap()
            } else {
                &mut root_mint.new_goldenfile(&result_path).unwrap()
            };

            let result = physical_plan
                .format(metadata.clone(), Default::default())?
                .format_pretty()?;
            writeln!(file, "{}", result)?;
        }

        println!("✅ {} test passed!", test.name);
    }

    Ok(())
}
