// Copyright 2025 Datafuse Labs.
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

#[derive(Debug, Serialize, Deserialize)]
struct TestSpec {
    name: String,
    description: Option<String>,
    sql: String,
    #[serde(default)]
    table_statistics: HashMap<String, TableStats>,
    #[serde(default)]
    column_statistics: HashMap<String, ColumnStats>,
    #[serde(default)]
    statistics_file: Option<String>,
    #[serde(default)]
    auto_statistics: bool,
    good_plan: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TableStats {
    num_rows: Option<u64>,
    data_size: Option<u64>,
    data_size_compressed: Option<u64>,
    index_size: Option<u64>,
    number_of_blocks: Option<u64>,
    number_of_segments: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ColumnStats {
    min: Option<serde_json::Value>,
    max: Option<serde_json::Value>,
    ndv: Option<u64>,
    null_count: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StatsFile {
    table_statistics: HashMap<String, TableStats>,
    column_statistics: HashMap<String, ColumnStats>,
}

struct TestCase {
    name: &'static str,
    sql: &'static str,
    table_stats: HashMap<String, TableStats>,
    column_stats: HashMap<String, ColumnStats>,
    auto_stats: bool,
    stem: String,
    subdir: Option<String>,
}

struct TestSuite {
    base_path: PathBuf,
    subdir: Option<String>,
}

impl TestSuite {
    fn new(base_path: PathBuf, subdir: Option<String>) -> Self {
        Self { base_path, subdir }
    }

    fn find_files(&self, dir: &str, exts: &[&str]) -> Vec<PathBuf> {
        let mut files = Vec::new();
        let dir_path = self.base_path.join(dir);

        if !dir_path.exists() {
            return files;
        }

        let search_paths = self
            .subdir
            .as_ref()
            .map(|s| vec![dir_path.join(s)])
            .unwrap_or_else(|| {
                let subdirs = self.collect_subdirs(&dir_path);
                if subdirs.is_empty() {
                    vec![dir_path]
                } else {
                    subdirs
                }
            });

        for path in search_paths {
            Self::collect_files_recursive(&path, &mut files, exts);
        }

        files.sort();
        files
    }

    fn collect_subdirs(&self, dir: &Path) -> Vec<PathBuf> {
        fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| p.is_dir())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn collect_files_recursive(dir: &Path, files: &mut Vec<PathBuf>, exts: &[&str]) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.filter_map(|e| e.ok()) {
                let path = entry.path();
                if path.is_dir() {
                    Self::collect_files_recursive(&path, files, exts);
                } else if path
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|s| exts.contains(&s))
                    .unwrap_or(false)
                {
                    files.push(path);
                }
            }
        }
    }

    async fn setup_tables(&self, ctx: &Arc<QueryContext>) -> Result<()> {
        for file in self.find_files("tables", &["sql"]) {
            let sql = fs::read_to_string(&file)
                .map_err(|e| ErrorCode::Internal(format!("Failed to read file: {}", e)))?;
            for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
                match execute_sql(ctx, statement).await {
                    Ok(_) => {}
                    Err(e) if e.code() == ErrorCode::TABLE_ALREADY_EXISTS => {
                        // Ignore table already exists errors
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
    }

    fn load_cases(&self) -> Result<Vec<TestCase>> {
        let cases_dir = self.base_path.join("cases");
        self.find_files("cases", &["yaml", "yml"])
            .into_iter()
            .map(|path| {
                // Extract subdirectory info
                let subdir = path
                    .strip_prefix(&cases_dir)
                    .ok()
                    .and_then(|p| p.parent())
                    .filter(|p| !p.as_os_str().is_empty())
                    .map(|p| p.to_string_lossy().to_string());

                self.load_case(&path, subdir)
            })
            .collect()
    }

    fn load_case(&self, path: &Path, subdir: Option<String>) -> Result<TestCase> {
        let content = fs::read_to_string(path)
            .map_err(|e| ErrorCode::Internal(format!("Failed to read file: {}", e)))?;
        let spec: TestSpec = serde_yaml::from_str(&content)
            .map_err(|e| ErrorCode::Internal(format!("Failed to parse YAML: {}", e)))?;

        let name = spec.name.clone();
        let sql = spec.sql.clone();
        let auto_stats = spec.auto_statistics;
        let (table_stats, column_stats) = self.resolve_stats(spec)?;
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();

        // Combine subdirectory with name if subdirectory exists
        let full_name = if let Some(ref sub) = subdir {
            format!("{}/{}", sub, name)
        } else {
            name
        };

        Ok(TestCase {
            name: Box::leak(full_name.into_boxed_str()),
            sql: Box::leak(sql.into_boxed_str()),
            table_stats,
            column_stats,
            auto_stats,
            stem,
            subdir,
        })
    }

    fn resolve_stats(
        &self,
        mut spec: TestSpec,
    ) -> Result<(HashMap<String, TableStats>, HashMap<String, ColumnStats>)> {
        if let Some(file_ref) = spec.statistics_file {
            let stats = self.load_stats_file(&file_ref)?;
            spec.table_statistics.extend(stats.table_statistics);
            spec.column_statistics.extend(stats.column_statistics);
        }
        Ok((spec.table_statistics, spec.column_statistics))
    }

    fn load_stats_file(&self, file_ref: &str) -> Result<StatsFile> {
        let stats_files = self.find_files("statistics", &["yaml", "yml"]);
        let target_stem = Path::new(file_ref)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(file_ref);

        stats_files
            .iter()
            .find(|path| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s == target_stem || s.ends_with(target_stem))
                    .unwrap_or(false)
            })
            .ok_or_else(|| ErrorCode::Internal(format!("Statistics file not found: {}", file_ref)))
            .and_then(|path| {
                let content = fs::read_to_string(path)
                    .map_err(|e| ErrorCode::Internal(format!("Failed to read file: {}", e)))?;
                serde_yaml::from_str(&content)
                    .map_err(|e| ErrorCode::Internal(format!("Failed to parse stats YAML: {}", e)))
            })
    }
}

fn to_datum(value: &Option<serde_json::Value>) -> Option<Datum> {
    value.as_ref().and_then(|v| match v {
        serde_json::Value::Number(n) => n
            .as_i64()
            .and_then(|i| Datum::from_scalar(Scalar::Number(NumberScalar::Int64(i))))
            .or_else(|| {
                n.as_f64().and_then(|f| {
                    Datum::from_scalar(Scalar::Number(NumberScalar::Float64(f.into())))
                })
            }),
        serde_json::Value::String(s) => Datum::from_scalar(Scalar::String(s.clone())),
        _ => None,
    })
}

fn default_min_datum(data_type: &databend_common_expression::types::DataType) -> Option<Datum> {
    if data_type.is_floating() {
        Some(Datum::Float(F64::MIN))
    } else if data_type.is_signed_numeric() {
        Some(Datum::Int(i64::MIN))
    } else if data_type.is_unsigned_numeric() {
        Some(Datum::UInt(u64::MIN))
    } else {
        Some(Datum::Bytes("\0\0\0\0\0\0\0\0".to_bytes().to_vec()))
    }
}

fn default_max_datum(data_type: &databend_common_expression::types::DataType) -> Option<Datum> {
    if data_type.is_floating() {
        Some(Datum::Float(F64::MAX))
    } else if data_type.is_signed_numeric() {
        Some(Datum::Int(i64::MAX))
    } else if data_type.is_unsigned_numeric() {
        Some(Datum::UInt(u64::MAX))
    } else {
        Some(Datum::Bytes(
            "\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}\u{FFFF}"
                .to_bytes()
                .to_vec(),
        ))
    }
}

struct StatsApplier<'a> {
    metadata: &'a MetadataRef,
    table_stats: &'a HashMap<String, TableStats>,
    column_stats: &'a HashMap<String, ColumnStats>,
}

impl<'a> SExprVisitor for StatsApplier<'a> {
    fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
        if let RelOperator::Scan(scan) = expr.plan() {
            let metadata = self.metadata.read();
            let table = metadata.table(scan.table_index);

            if let Some(stats) = self.table_stats.get(table.name()) {
                let column_stats =
                    self.build_column_stats(&metadata, scan.table_index, table.name());
                let table_stats = TableStatistics {
                    num_rows: stats.num_rows,
                    data_size: stats.data_size,
                    data_size_compressed: stats.data_size_compressed,
                    index_size: stats.index_size,
                    number_of_blocks: stats.number_of_blocks,
                    number_of_segments: stats.number_of_segments,
                };

                let mut new_scan = scan.clone();
                new_scan.statistics = Arc::new(Statistics {
                    table_stats: Some(table_stats),
                    column_stats,
                    histograms: HashMap::new(),
                });

                return Ok(VisitAction::Replace(
                    expr.replace_plan(Arc::new(RelOperator::Scan(new_scan))),
                ));
            }
        }
        Ok(VisitAction::Continue)
    }
}

impl<'a> StatsApplier<'a> {
    fn build_column_stats(
        &self,
        metadata: &databend_common_sql::Metadata,
        table_index: IndexType,
        table_name: &str,
    ) -> HashMap<IndexType, Option<BasicColumnStatistics>> {
        let mut result = HashMap::new();

        for (idx, column) in metadata
            .columns_by_table_index(table_index)
            .iter()
            .enumerate()
        {
            if let ColumnEntry::BaseTableColumn(BaseTableColumn { column_name, .. }) = column {
                let full_name = format!("{}.{}", table_name, column_name);
                if let Some(stats) = self.column_stats.get(&full_name) {
                    result.insert(
                        idx as IndexType,
                        Some(BasicColumnStatistics {
                            min: to_datum(&stats.min)
                                .or_else(|| default_min_datum(&column.data_type())),
                            max: to_datum(&stats.max)
                                .or_else(|| default_max_datum(&column.data_type())),
                            ndv: stats.ndv,
                            null_count: stats.null_count.unwrap_or(0),
                        }),
                    );
                }
            }
        }

        result
    }
}

async fn optimize_plan(ctx: Arc<QueryContext>, plan: Plan) -> Result<Plan> {
    let metadata = match &plan {
        Plan::Query { metadata, .. } => metadata.clone(),
        _ => Arc::new(parking_lot::RwLock::new(Metadata::default())),
    };

    let opt_ctx = OptimizerContext::new(ctx, metadata)
        .set_enable_distributed_optimization(true)
        .set_enable_join_reorder(true)
        .set_enable_dphyp(true)
        .set_max_push_down_limit(10000)
        .set_enable_trace(true)
        .clone();

    optimize(opt_ctx, plan).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_optimizer() -> Result<()> {
    let base_path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/it/sql/planner/optimizer/data");
    let subdir = std::env::var("TEST_SUBDIR").ok();

    let suite = TestSuite::new(base_path.clone(), subdir);
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_enable_auto_materialize_cte(0)?;

    suite.setup_tables(&ctx).await?;

    let cases = suite.load_cases()?;
    if cases.is_empty() {
        return Ok(());
    }

    let results_dir = base_path.join("results");
    let mut root_mint = Mint::new(&results_dir);
    let mut subdir_mints: HashMap<String, Mint> = HashMap::new();

    for case in cases {
        println!("\n========== Testing: {} ==========", case.name);

        run_test_case(&ctx, &case, &mut root_mint, &mut subdir_mints, &results_dir).await?;

        println!("✅ {} test passed!", case.name);
    }

    Ok(())
}

async fn run_test_case(
    ctx: &Arc<QueryContext>,
    case: &TestCase,
    root_mint: &mut Mint,
    subdir_mints: &mut HashMap<String, Mint>,
    results_dir: &Path,
) -> Result<()> {
    configure_optimizer(ctx, case.auto_stats)?;

    let mut plan = raw_plan(ctx, case.sql).await?;
    apply_stats(&mut plan, &case.table_stats, &case.column_stats)?;

    // Get the appropriate Mint instance
    let mint = if let Some(ref subdir) = case.subdir {
        subdir_mints.entry(subdir.clone()).or_insert_with(|| {
            let subdir_results = results_dir.join(subdir);
            Mint::new(subdir_results)
        })
    } else {
        root_mint
    };

    write_result(mint, &format!("{}_raw.txt", case.stem), |f| {
        writeln!(
            f,
            "{}",
            plan.format_indent(FormatOptions { verbose: false })?
        )
        .map_err(|e| ErrorCode::Internal(format!("Failed to write: {}", e)))
    })?;

    let optimized = optimize_plan(ctx.clone(), plan).await?;
    write_result(mint, &format!("{}_optimized.txt", case.stem), |f| {
        writeln!(f, "{}", optimized.format_indent(FormatOptions::default())?)
            .map_err(|e| ErrorCode::Internal(format!("Failed to write: {}", e)))
    })?;

    if let Plan::Query {
        metadata,
        bind_context,
        s_expr,
        ..
    } = optimized
    {
        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx.clone(), false);
        let physical = builder.build(&s_expr, bind_context.column_set()).await?;

        write_result(mint, &format!("{}_physical.txt", case.stem), |f| {
            writeln!(
                f,
                "{}",
                physical
                    .format(metadata, Default::default())?
                    .format_pretty()?
            )
            .map_err(|e| ErrorCode::Internal(format!("Failed to write: {}", e)))
        })?;
    }

    Ok(())
}

fn configure_optimizer(ctx: &Arc<QueryContext>, auto_stats: bool) -> Result<()> {
    let settings = ctx.get_settings();
    if auto_stats {
        settings.set_optimizer_skip_list("".to_string())
    } else {
        settings.set_optimizer_skip_list("CollectStatisticsOptimizer".to_string())
    }
}

fn apply_stats(
    plan: &mut Plan,
    table_stats: &HashMap<String, TableStats>,
    column_stats: &HashMap<String, ColumnStats>,
) -> Result<()> {
    if let Plan::Query {
        s_expr, metadata, ..
    } = plan
    {
        let mut applier = StatsApplier {
            metadata,
            table_stats,
            column_stats,
        };
        if let Some(new_expr) = s_expr.accept(&mut applier)? {
            *s_expr = Box::new(new_expr);
        }
    }
    Ok(())
}

fn write_result<F>(mint: &mut Mint, name: &str, f: F) -> Result<()>
where F: FnOnce(&mut dyn Write) -> Result<()> {
    let mut file = mint.new_goldenfile(name).unwrap();
    f(&mut file)
}
