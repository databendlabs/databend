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
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::F64;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberScalar;
use databend_common_settings::Settings;
use databend_common_sql::BaseTableColumn;
use databend_common_sql::ColumnEntry;
use databend_common_sql::IndexType;
use databend_common_sql::Metadata;
use databend_common_sql::MetadataRef;
use databend_common_sql::Symbol;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::SExprVisitor;
use databend_common_sql::optimizer::ir::VisitAction;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::Statistics;
use databend_common_statistics::Datum;
use goldenfile::Mint;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
struct TestSpec {
    name: String,
    #[serde(default, rename = "description")]
    _description: Option<String>,
    sql: String,
    #[serde(default)]
    table_statistics: HashMap<String, TableStats>,
    #[serde(default)]
    column_statistics: HashMap<String, ColumnStats>,
    #[serde(default)]
    statistics_file: Option<String>,
    #[serde(default)]
    tables: HashMap<String, String>,
    #[serde(default)]
    auto_statistics: bool,
    #[serde(default)]
    node_num: Option<u64>,
    #[serde(default, rename = "good_plan")]
    _good_plan: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableStats {
    pub num_rows: Option<u64>,
    pub data_size: Option<u64>,
    pub data_size_compressed: Option<u64>,
    pub index_size: Option<u64>,
    pub number_of_blocks: Option<u64>,
    pub number_of_segments: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnStats {
    pub min: Option<serde_json::Value>,
    pub max: Option<serde_json::Value>,
    pub ndv: Option<u64>,
    pub null_count: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StatsFile {
    table_statistics: HashMap<String, TableStats>,
    column_statistics: HashMap<String, ColumnStats>,
}

#[derive(Debug, Clone)]
pub struct TestCase {
    pub name: String,
    pub sql: String,
    pub table_stats: HashMap<String, TableStats>,
    pub column_stats: HashMap<String, ColumnStats>,
    pub auto_stats: bool,
    pub stem: String,
    pub subdir: Option<String>,
    pub node_num: Option<u64>,
    pub tables: HashMap<String, String>,
}

pub struct TestRun {
    pub raw: String,
    pub optimized: String,
    pub optimized_plan: Plan,
    pub physical: Option<String>,
}

pub struct TestSuite {
    base_path: PathBuf,
    subdir: Option<String>,
}

pub struct TestSuiteMints {
    results_dir: PathBuf,
    root_mint: Mint,
    subdir_mints: HashMap<String, Mint>,
}

impl TestSuite {
    pub fn new(base_path: PathBuf, subdir: Option<String>) -> Self {
        Self { base_path, subdir }
    }

    pub fn optimizer_data_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("data")
    }

    pub fn create_mints(&self) -> TestSuiteMints {
        let results_dir = self.base_path.join("results");
        TestSuiteMints {
            root_mint: Mint::new(&results_dir),
            results_dir,
            subdir_mints: HashMap::new(),
        }
    }

    pub fn load_cases(&self) -> Result<Vec<TestCase>> {
        let cases_dir = self.base_path.join("cases");
        if !cases_dir.exists() {
            return Ok(vec![]);
        }

        let search_paths = self
            .subdir
            .as_ref()
            .map(|subdir| vec![cases_dir.join(subdir)])
            .unwrap_or_else(|| {
                let subdirs = self.collect_subdirs(&cases_dir);
                if subdirs.is_empty() {
                    vec![cases_dir.clone()]
                } else {
                    subdirs
                }
            });

        let mut cases = Vec::new();
        for search_path in search_paths {
            for path in Self::find_yaml_files(&search_path) {
                let subdir = path
                    .parent()
                    .and_then(|parent| parent.strip_prefix(&cases_dir).ok())
                    .and_then(|rel| {
                        let text = rel.to_string_lossy();
                        if text.is_empty() {
                            None
                        } else {
                            Some(text.to_string())
                        }
                    });
                cases.push(self.load_case(&path, subdir)?);
            }
        }

        cases.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(cases)
    }

    pub fn expected_result(&self, case: &TestCase, kind: &str) -> Result<String> {
        let path = self
            .base_path
            .join("results")
            .join(case.subdir.as_deref().unwrap_or(""))
            .join(format!("{}_{}.txt", case.stem, kind));
        fs::read_to_string(&path).map_err(|e| {
            ErrorCode::Internal(format!("Failed to read expected result {path:?}: {e}"))
        })
    }

    fn load_case(&self, path: &Path, subdir: Option<String>) -> Result<TestCase> {
        let content = fs::read_to_string(path)
            .map_err(|e| ErrorCode::Internal(format!("Failed to read file: {e}")))?;
        let spec: TestSpec = serde_yaml::from_str(&content)
            .map_err(|e| ErrorCode::Internal(format!("Failed to parse YAML: {e}")))?;

        let stem = path
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ErrorCode::Internal(format!("Invalid file stem for {path:?}")))?
            .to_string();

        let tables = self.resolve_tables(&spec)?;
        let (table_stats, column_stats) = self.resolve_stats(&spec)?;
        let auto_stats = spec.auto_statistics;
        let sql = spec.sql;
        let name = spec.name;
        let node_num = spec.node_num;

        Ok(TestCase {
            name,
            sql,
            table_stats,
            column_stats,
            auto_stats,
            stem,
            subdir,
            node_num,
            tables,
        })
    }

    fn resolve_tables(&self, spec: &TestSpec) -> Result<HashMap<String, String>> {
        spec.tables
            .iter()
            .map(|(table_name, file_ref)| {
                self.find_files("tables", &["sql"])
                    .into_iter()
                    .find(|path| path.ends_with(file_ref))
                    .ok_or_else(|| ErrorCode::Internal(format!("Table file not found: {file_ref}")))
                    .and_then(|path| {
                        fs::read_to_string(&path)
                            .map(|content| (table_name.clone(), content))
                            .map_err(|e| ErrorCode::Internal(format!("Failed to read file: {e}")))
                    })
            })
            .collect()
    }

    fn resolve_stats(
        &self,
        spec: &TestSpec,
    ) -> Result<(HashMap<String, TableStats>, HashMap<String, ColumnStats>)> {
        let mut table_stats = spec.table_statistics.clone();
        let mut column_stats = spec.column_statistics.clone();

        if let Some(file_ref) = spec.statistics_file.as_deref() {
            let stats_file = self.load_stats_file(file_ref)?;
            table_stats.extend(stats_file.table_statistics);
            column_stats.extend(stats_file.column_statistics);
        }

        Ok((table_stats, column_stats))
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
            .ok_or_else(|| ErrorCode::Internal(format!("Statistics file not found: {file_ref}")))
            .and_then(|path| {
                let content = fs::read_to_string(path)
                    .map_err(|e| ErrorCode::Internal(format!("Failed to read file: {e}")))?;
                serde_yaml::from_str(&content)
                    .map_err(|e| ErrorCode::Internal(format!("Failed to parse stats YAML: {e}")))
            })
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

    fn find_yaml_files(dir: &Path) -> Vec<PathBuf> {
        let mut files = Vec::new();
        Self::collect_files_recursive(dir, &mut files, &["yaml", "yml"]);
        files.sort();
        files
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
}

impl TestSuiteMints {
    pub fn mint_for(&mut self, case: &TestCase) -> &mut Mint {
        if let Some(ref subdir) = case.subdir {
            self.subdir_mints.entry(subdir.clone()).or_insert_with(|| {
                let subdir_results = self.results_dir.join(subdir);
                Mint::new(subdir_results)
            })
        } else {
            &mut self.root_mint
        }
    }
}

pub fn configure_optimizer_settings(settings: &Settings, auto_stats: bool) -> Result<()> {
    settings.set_setting("enable_dphyp".to_string(), "1".to_string())?;
    settings.set_setting("max_push_down_limit".to_string(), "10000".to_string())?;
    settings.set_setting("enable_optimizer_trace".to_string(), "1".to_string())?;
    settings.set_setting("enable_shuffle_sort".to_string(), "0".to_string())?;

    if auto_stats {
        settings.set_optimizer_skip_list("".to_string())
    } else {
        settings.set_optimizer_skip_list("CollectStatisticsOptimizer".to_string())
    }
}

fn to_datum(value: &Option<serde_json::Value>) -> Option<Datum> {
    value.as_ref().and_then(|v| match v {
        serde_json::Value::Number(n) => n
            .as_i64()
            .and_then(|i| Scalar::Number(NumberScalar::Int64(i)).to_datum())
            .or_else(|| {
                n.as_f64()
                    .and_then(|f| Scalar::Number(NumberScalar::Float64(f.into())).to_datum())
            }),
        serde_json::Value::String(s) => Scalar::String(s.clone()).to_datum(),
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
        Some(Datum::Bytes("\0\0\0\0\0\0\0\0".as_bytes().to_vec()))
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
                .as_bytes()
                .to_vec(),
        ))
    }
}

struct StatsApplier<'a> {
    metadata: &'a MetadataRef,
    table_stats: &'a HashMap<String, TableStats>,
    column_stats: &'a HashMap<String, ColumnStats>,
}

impl SExprVisitor for StatsApplier<'_> {
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
                    bloom_index_size: None,
                    ngram_index_size: None,
                    inverted_index_size: None,
                    vector_index_size: None,
                    virtual_column_size: None,
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

impl StatsApplier<'_> {
    fn build_column_stats(
        &self,
        metadata: &Metadata,
        table_index: IndexType,
        table_name: &str,
    ) -> HashMap<Symbol, Option<BasicColumnStatistics>> {
        let mut result = HashMap::new();

        for (idx, column) in metadata
            .columns_by_table_index(table_index)
            .iter()
            .enumerate()
        {
            if let ColumnEntry::BaseTableColumn(BaseTableColumn { column_name, .. }) = column {
                let full_name = format!("{table_name}.{column_name}");
                if let Some(stats) = self.column_stats.get(&full_name) {
                    result.insert(
                        Symbol::new(idx),
                        Some(BasicColumnStatistics {
                            min: to_datum(&stats.min)
                                .or_else(|| default_min_datum(&column.data_type())),
                            max: to_datum(&stats.max)
                                .or_else(|| default_max_datum(&column.data_type())),
                            ndv: stats.ndv,
                            null_count: stats.null_count.unwrap_or(0),
                            in_memory_size: 0,
                        }),
                    );
                }
            }
        }

        result
    }
}

fn write_result<F>(mint: &mut Mint, name: &str, f: F) -> Result<()>
where F: FnOnce(&mut dyn Write) -> Result<()> {
    let mut file = mint.new_goldenfile(name).unwrap();
    f(&mut file)
}

#[allow(async_fn_in_trait)]
pub trait TestCaseRunner {
    async fn bind_sql(&self, sql: &str) -> Result<Plan>;

    async fn optimize_plan(&self, plan: Plan) -> Result<Plan>;

    async fn build_physical(&self, _optimized: &Plan) -> Result<Option<String>> {
        Ok(None)
    }
}

pub async fn run_test_case_core<R>(
    case: &TestCase,
    mint: &mut Mint,
    runner: &R,
) -> Result<TestRun>
where
    R: TestCaseRunner + ?Sized,
{
    let mut raw_plan = runner.bind_sql(&case.sql).await?;
    apply_stats(&mut raw_plan, &case.table_stats, &case.column_stats)?;
    let raw = raw_plan.format_indent(databend_common_sql::FormatOptions { verbose: false })?;
    write_result(mint, &format!("{}_raw.txt", case.stem), |f| {
        writeln!(f, "{}", raw).map_err(|e| ErrorCode::Internal(format!("Failed to write: {}", e)))
    })?;

    let optimized_plan = runner.optimize_plan(raw_plan).await?;
    let optimized = optimized_plan.format_indent(databend_common_sql::FormatOptions::default())?;
    write_result(mint, &format!("{}_optimized.txt", case.stem), |f| {
        writeln!(f, "{}", optimized)
            .map_err(|e| ErrorCode::Internal(format!("Failed to write: {}", e)))
    })?;

    let physical = runner.build_physical(&optimized_plan).await?;
    if let Some(physical_text) = &physical {
        write_result(mint, &format!("{}_physical.txt", case.stem), |f| {
            writeln!(f, "{}", physical_text)
                .map_err(|e| ErrorCode::Internal(format!("Failed to write: {}", e)))
        })?;
    }

    Ok(TestRun {
        raw,
        optimized,
        optimized_plan,
        physical,
    })
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
            **s_expr = new_expr;
        }
    }
    Ok(())
}
