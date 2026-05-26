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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_schema::Field;
use databend_common_ast::Span;
use databend_common_ast::ast::ColumnMatchMode;
use databend_common_ast::ast::CopySchemaEvolutionOptions;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table::TableExt;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::DecompressDecoder;
use databend_common_compress::DecompressState;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FromData;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::SendableDataBlockStream;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_pipeline::core::Pipeline;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_storage::StageFileInfo;
use databend_common_storage::init_stage_operator;
use databend_common_storage::parquet::infer_schema_with_extension;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_parquet::read_metas_in_parallel_for_copy;
use databend_query_storage_stage_support::StageTable;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use itertools::Itertools;
use log::debug;
use log::info;

use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::physical_plans::CopyIntoTable;
use crate::physical_plans::CopyIntoTableSource;
use crate::physical_plans::Exchange;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::TableScan;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;
use crate::sql::plans::CopyIntoTablePlan;
use crate::sql::plans::Plan;
use crate::stream::DataBlockStream;
use crate::table_functions::infer_schema::InferSchemaSeparator;
use crate::table_functions::infer_schema::merge_schema;

const NDJSON_SCHEMA_EVOLUTION_AUTO_SAMPLE_FILES: usize = 64;
const NDJSON_SCHEMA_EVOLUTION_AUTO_RECORDS_PER_FILE: usize = 1000;
const NDJSON_SCHEMA_EVOLUTION_AUTO_TOTAL_RECORDS: usize = 10000;
const NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE: usize = 32 * 1024 * 1024;
const NDJSON_SCHEMA_EVOLUTION_COMPRESSED_READ_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy)]
struct NdJsonSchemaEvolutionOptions {
    sample_files: usize,
    sample_records_per_file: usize,
    sample_total_records: usize,
}

struct NdJsonSampleBytes {
    data: Vec<u8>,
    is_truncated: bool,
}

impl NdJsonSchemaEvolutionOptions {
    fn resolve(
        options: Option<&CopySchemaEvolutionOptions>,
        total_files: usize,
        max_threads: usize,
    ) -> Self {
        let auto_sample_files = total_files
            .min(max_threads.max(1) * 4)
            .clamp(1, NDJSON_SCHEMA_EVOLUTION_AUTO_SAMPLE_FILES);
        Self {
            sample_files: options
                .and_then(|v| v.sample_files)
                .unwrap_or(auto_sample_files)
                .min(total_files)
                .max(1),
            sample_records_per_file: options
                .and_then(|v| v.sample_records_per_file)
                .unwrap_or(NDJSON_SCHEMA_EVOLUTION_AUTO_RECORDS_PER_FILE),
            sample_total_records: options
                .and_then(|v| v.sample_total_records)
                .unwrap_or(NDJSON_SCHEMA_EVOLUTION_AUTO_TOTAL_RECORDS),
        }
    }
}

fn normalize_schema_field_name(name: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        name.to_string()
    } else {
        name.to_lowercase()
    }
}

fn trim_ndjson_sample_bytes<'a>(
    bytes: &'a [u8],
    is_truncated: bool,
    path: &str,
) -> Result<&'a [u8]> {
    if bytes.is_empty() || matches!(bytes.last(), Some(b'\n' | b'\r')) {
        return Ok(bytes);
    }
    match bytes.iter().rposition(|b| *b == b'\n') {
        Some(pos) => Ok(&bytes[..=pos]),
        None if is_truncated => Err(ErrorCode::BadBytes(format!(
            "insufficient NDJSON sample data for file '{path}': the sampled prefix does not contain a complete JSON record. Please adjust SCHEMA_EVOLUTION sample options such as SAMPLE_FILES, SAMPLE_RECORDS_PER_FILE, or SAMPLE_TOTAL_RECORDS",
        ))),
        None => Ok(bytes),
    }
}

fn compression_algo_from_path(
    compression: StageFileCompression,
    path: &str,
) -> Result<Option<CompressAlgorithm>> {
    let algo = match compression {
        StageFileCompression::Auto => CompressAlgorithm::from_path(path),
        StageFileCompression::Gzip => Some(CompressAlgorithm::Gzip),
        StageFileCompression::Bz2 => Some(CompressAlgorithm::Bz2),
        StageFileCompression::Brotli => Some(CompressAlgorithm::Brotli),
        StageFileCompression::Zstd => Some(CompressAlgorithm::Zstd),
        StageFileCompression::Deflate => Some(CompressAlgorithm::Zlib),
        StageFileCompression::RawDeflate => Some(CompressAlgorithm::Deflate),
        StageFileCompression::Xz => Some(CompressAlgorithm::Xz),
        StageFileCompression::Lzo => {
            return Err(ErrorCode::Unimplemented(
                "compress type lzo is unimplemented for copy into",
            ));
        }
        StageFileCompression::Snappy => {
            return Err(ErrorCode::Unimplemented(
                "compress type snappy is unimplemented for copy into",
            ));
        }
        StageFileCompression::None => None,
        StageFileCompression::Zip => Some(CompressAlgorithm::Zip),
    };
    Ok(algo)
}

pub struct CopyIntoTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyIntoTablePlan,
}

impl CopyIntoTableInterpreter {
    /// Create a CopyInterpreter with context and [`CopyIntoTablePlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyIntoTablePlan) -> Result<Self> {
        Ok(CopyIntoTableInterpreter { ctx, plan })
    }

    #[async_backtrace::framed]
    async fn build_query(
        &self,
        query: &Plan,
    ) -> Result<(SelectInterpreter, Vec<UpdateStreamMetaReq>)> {
        let (s_expr, metadata, bind_context, formatted_ast) = match query {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                formatted_ast,
                ..
            } => (s_expr, metadata, bind_context, formatted_ast),
            v => unreachable!("Input plan must be Query, but it's {}", v),
        };

        let update_stream_meta = dml_build_update_stream_req(self.ctx.clone()).await?;

        let select_interpreter = SelectInterpreter::try_create(
            self.ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
            formatted_ast.clone(),
            false,
        )?;

        Ok((select_interpreter, update_stream_meta))
    }

    #[async_backtrace::framed]
    pub async fn build_physical_plan(
        &self,
        mut table_info: TableInfo,
        plan: &CopyIntoTablePlan,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<(
        PhysicalPlan,
        Vec<UpdateStreamMetaReq>,
        Option<TableSchemaRef>,
    )> {
        let mut new_schema = None;
        let mut update_stream_meta_reqs = vec![];
        let (source, project_columns) = if let Some(ref query) = plan.query {
            let query = if plan.enable_distributed {
                query.remove_exchange_for_select()
            } else {
                *query.clone()
            };

            let (query_interpreter, update_stream_meta) = self.build_query(&query).await?;
            update_stream_meta_reqs = update_stream_meta;
            let query_physical_plan = query_interpreter.build_physical_plan().await?;

            let result_columns = query_interpreter.get_result_columns();
            (
                CopyIntoTableSource::Query(query_physical_plan),
                Some(result_columns),
            )
        } else {
            let mut stage_table_info = plan.stage_table_info.clone();
            if plan.enable_schema_evolution {
                stage_table_info
                    .copy_into_table_options
                    .schema_evolution
                    .get_or_insert_default();
                new_schema = Self::infer_schema(&mut stage_table_info, self.ctx.clone())
                    .await
                    .map_err(|e| e.with_context("infer_schema"))?;
            }

            let stage_table = StageTable::try_create(stage_table_info)?;

            let data_source_plan = stage_table
                .read_plan(self.ctx.clone(), None, None, false, false)
                .await?;

            let mut name_mapping = BTreeMap::new();
            for (idx, field) in data_source_plan.schema().fields.iter().enumerate() {
                name_mapping.insert(field.name.clone(), idx.to_string());
            }

            (
                CopyIntoTableSource::Stage(PhysicalPlan::new(TableScan {
                    scan_id: 0,
                    name_mapping,
                    stat_info: None,
                    table_index: None,
                    internal_column: None,
                    source: Box::new(data_source_plan),
                    meta: PhysicalPlanMeta::new("TableScan"),
                })),
                None,
            )
        };

        let mut required_values_schema = plan.required_values_schema.clone();
        let mut required_source_schema = plan.required_source_schema.clone();
        if let Some(schema) = &new_schema {
            table_info.meta.schema = schema.clone();
            let data_schema: DataSchema = schema.into();
            required_source_schema = Arc::new(data_schema);
            required_values_schema = required_source_schema.clone();
        }

        let mut root = PhysicalPlan::new(CopyIntoTable {
            required_values_schema,
            values_consts: plan.values_consts.clone(),
            required_source_schema,
            stage_table_info: plan.stage_table_info.clone(),
            table_info,
            write_mode: plan.write_mode,
            validation_mode: plan.validation_mode.clone(),
            project_columns,
            source,
            is_transform: plan.is_transform,
            table_meta_timestamps,
            meta: PhysicalPlanMeta::new("CopyIntoTable"),
        });

        if plan.enable_distributed {
            root = PhysicalPlan::new(Exchange {
                input: root,
                kind: FragmentKind::Merge,
                keys: Vec::new(),
                allow_adjust_parallelism: true,
                ignore_exchange: false,
                meta: PhysicalPlanMeta::new("Exchange"),
            });
        }

        let mut next_plan_id = 0;
        root.adjust_plan_id(&mut next_plan_id);

        Ok((root, update_stream_meta_reqs, new_schema))
    }

    async fn infer_schema(
        stage_table_info: &mut StageTableInfo,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Option<TableSchemaRef>> {
        #[allow(clippy::single_match)]
        match &stage_table_info.stage_info.file_format_params {
            FileFormatParams::Parquet(_) => {
                let settings = ctx.get_settings();
                let max_threads = settings.get_max_threads()? as usize;
                let max_memory_usage = settings.get_max_memory_usage()?;

                let operator = init_stage_operator(&stage_table_info.stage_info)?;
                // User set the files.
                let files = stage_table_info.files_to_copy.as_ref().expect(
                    "ParquetTableForCopy::do_read_partitions must be called with files_to_copy set",
                );
                let file_infos = files
                    .iter()
                    .filter(|f| f.size > 0)
                    .map(|f| (f.path.clone(), f.size))
                    .collect::<Vec<_>>();
                ctx.set_status_info("[TABLE-SCAN] Infer Parquet Schemas");
                let metas = read_metas_in_parallel_for_copy(
                    &operator,
                    &file_infos,
                    max_threads,
                    max_memory_usage,
                )
                .await?;

                let case_sensitive = stage_table_info.copy_into_table_options.column_match_mode
                    == Some(ColumnMatchMode::CaseSensitive);

                let mut new_schema = stage_table_info.schema.as_ref().to_owned();
                let old_fields: HashMap<String, TableDataType> = stage_table_info
                    .schema
                    .fields
                    .iter()
                    .map(|f| {
                        (
                            if case_sensitive {
                                f.name.clone()
                            } else {
                                f.name.to_lowercase()
                            },
                            f.data_type.clone(),
                        )
                    })
                    .collect::<_>();
                let mut new_fields: HashMap<String, Field> = HashMap::new();
                for meta in &metas {
                    let arrow_schema = infer_schema_with_extension(meta.meta.file_metadata())?;
                    for field in arrow_schema.fields().clone().into_iter() {
                        let name = if case_sensitive {
                            field.name().clone()
                        } else {
                            field.name().to_lowercase()
                        };
                        if !old_fields.contains_key(&name) {
                            if let Some(f) = new_fields.get_mut(&name) {
                                if f.data_type() != field.data_type() {
                                    return Err(ErrorCode::BadBytes(format!(
                                        "data type of {name} mismatch: {} and {}",
                                        f.data_type(),
                                        field.data_type()
                                    )));
                                }
                            } else {
                                new_fields.insert(name, field.as_ref().clone());
                            }
                        }
                    }
                }

                stage_table_info.parquet_metas = Some(metas);
                if new_fields.is_empty() {
                    return Ok(None);
                } else {
                    let new_fields: Vec<_> = new_fields.into_iter().sorted().collect();
                    for (_, f) in new_fields {
                        let mut tf: TableField = (&f).try_into()?;
                        tf.data_type = tf.data_type.wrap_nullable();
                        if let Some(exprs) = &mut stage_table_info.default_exprs {
                            exprs.push(RemoteDefaultExpr::RemoteExpr(RemoteExpr::Constant {
                                scalar: Scalar::Null,
                                data_type: DataType::Null,
                                span: Span::default(),
                            }))
                        }
                        new_schema.add_column(&tf, new_schema.num_fields())?;
                    }
                    let schema = Arc::new(new_schema);
                    stage_table_info.schema = schema.clone();
                    return Ok(Some(schema));
                }
            }
            FileFormatParams::NdJson(_) => {
                let settings = ctx.get_settings();
                let max_threads = settings.get_max_threads()? as usize;
                let max_memory_usage = settings.get_max_memory_usage()?;
                let files = stage_table_info.files_to_copy.as_ref().expect(
                    "StageTableForCopy::do_read_partitions must be called with files_to_copy set",
                );
                let mut files = files
                    .iter()
                    .filter(|f| f.size > 0)
                    .cloned()
                    .collect::<Vec<_>>();
                files.sort_by(|a, b| a.path.cmp(&b.path));
                if files.is_empty() {
                    return Ok(None);
                }

                let options = NdJsonSchemaEvolutionOptions::resolve(
                    stage_table_info
                        .copy_into_table_options
                        .schema_evolution
                        .as_ref(),
                    files.len(),
                    max_threads,
                );
                let sampled_files = Self::sample_ndjson_files(&files, options.sample_files);
                if sampled_files.is_empty() || options.sample_total_records == 0 {
                    return Ok(None);
                }

                ctx.set_status_info("[COPY] Infer NDJSON schema");
                let start = Instant::now();
                let operator = init_stage_operator(&stage_table_info.stage_info)?;
                let compression = stage_table_info.stage_info.file_format_params.compression();
                let mut remaining_records = options.sample_total_records;
                let mut tasks = Vec::with_capacity(sampled_files.len());
                for file in sampled_files {
                    let max_records = remaining_records.min(options.sample_records_per_file);
                    if max_records == 0 {
                        break;
                    }
                    remaining_records -= max_records;
                    let operator = operator.clone();
                    let path = file.path.clone();
                    let file_size = file.size;
                    tasks.push(async move {
                        let sample = Self::read_ndjson_sample_bytes(
                            operator,
                            &path,
                            file_size,
                            compression,
                            max_memory_usage,
                        )
                        .await?;
                        let bytes =
                            trim_ndjson_sample_bytes(&sample.data, sample.is_truncated, &path)?;
                        if bytes.is_empty() {
                            return Ok(None);
                        }
                        let schema =
                            InferSchemaSeparator::infer_ndjson_schema(bytes, Some(max_records))?;
                        Ok::<_, ErrorCode>(Some((path, max_records, schema)))
                    });
                }

                let concurrency = max_threads.max(1).min(tasks.len().max(1));
                let results = execute_futures_in_parallel(
                    tasks,
                    concurrency,
                    concurrency,
                    "infer-ndjson-schema-worker".to_owned(),
                )
                .await?;

                let mut inferred_schema: Option<TableSchema> = None;
                let mut sampled_records_limit = 0usize;
                let mut sampled_file_count = 0usize;
                for result in results {
                    let Some((_path, max_records, schema)) = result? else {
                        continue;
                    };
                    sampled_records_limit += max_records;
                    sampled_file_count += 1;
                    inferred_schema = Some(match inferred_schema {
                        None => schema,
                        Some(existing) => merge_schema(existing, schema),
                    });
                }

                let Some(inferred_schema) = inferred_schema else {
                    return Ok(None);
                };

                let case_sensitive = stage_table_info.copy_into_table_options.column_match_mode
                    == Some(ColumnMatchMode::CaseSensitive);
                let mut new_schema = stage_table_info.schema.as_ref().to_owned();
                let old_fields: HashMap<String, TableDataType> = stage_table_info
                    .schema
                    .fields
                    .iter()
                    .map(|f| {
                        (
                            normalize_schema_field_name(f.name(), case_sensitive),
                            f.data_type.clone(),
                        )
                    })
                    .collect::<_>();
                let mut new_fields: HashMap<String, TableField> = HashMap::new();
                for field in inferred_schema.fields().iter() {
                    let name = normalize_schema_field_name(field.name(), case_sensitive);
                    if old_fields.contains_key(&name) {
                        continue;
                    }
                    if let Some(existing) = new_fields.get_mut(&name) {
                        let merged = merge_schema(
                            TableSchema::new(vec![existing.clone()]),
                            TableSchema::new(vec![field.clone()]),
                        );
                        *existing = merged.fields()[0].clone();
                    } else {
                        new_fields.insert(name, field.clone());
                    }
                }

                if new_fields.is_empty() {
                    return Ok(None);
                }

                let new_fields = new_fields.into_iter().sorted_by(|a, b| a.0.cmp(&b.0));
                let new_fields_count = new_fields.len();
                for (_, mut field) in new_fields {
                    field.data_type = field.data_type.wrap_nullable();
                    if let Some(exprs) = &mut stage_table_info.default_exprs {
                        exprs.push(RemoteDefaultExpr::RemoteExpr(RemoteExpr::Constant {
                            scalar: Scalar::Null,
                            data_type: DataType::Null,
                            span: Span::default(),
                        }))
                    }
                    new_schema.add_column(&field, new_schema.num_fields())?;
                }

                info!(
                    "Infer NDJSON schema for COPY: total_files={}, sampled_files={}, sample_records_per_file={}, sample_total_records={}, sampled_records_limit={}, new_columns={}, elapsed={:?}",
                    files.len(),
                    sampled_file_count,
                    options.sample_records_per_file,
                    options.sample_total_records,
                    sampled_records_limit,
                    new_fields_count,
                    start.elapsed(),
                );

                let schema = Arc::new(new_schema);
                stage_table_info.schema = schema.clone();
                return Ok(Some(schema));
            }
            _ => {}
        }
        Ok(None)
    }

    fn sample_ndjson_files(files: &[StageFileInfo], sample_files: usize) -> Vec<StageFileInfo> {
        if files.len() <= sample_files {
            return files.to_vec();
        }
        if sample_files == 1 {
            return vec![files[files.len() / 2].clone()];
        }

        let mut sampled = BTreeMap::new();
        let last = files.len() - 1;
        for i in 0..sample_files {
            let index = i * last / (sample_files - 1);
            sampled.insert(index, files[index].clone());
        }
        sampled.into_values().collect()
    }

    async fn read_ndjson_sample_bytes(
        operator: opendal::Operator,
        path: &str,
        file_size: u64,
        compression: StageFileCompression,
        max_memory_usage: u64,
    ) -> Result<NdJsonSampleBytes> {
        let algo = compression_algo_from_path(compression, path)?;
        let sample = match algo {
            None => {
                let read_size = file_size.min(NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE as u64);
                let data = operator
                    .read_with(path)
                    .range(0..read_size)
                    .await?
                    .to_bytes()
                    .to_vec();
                NdJsonSampleBytes {
                    data,
                    is_truncated: read_size < file_size,
                }
            }
            Some(algo) => {
                if algo == CompressAlgorithm::Zip {
                    Self::check_zip_ndjson_sample_size(path, file_size)?;
                    let compressed = operator.read(path).await?.to_bytes().to_vec();
                    let data = Self::read_zip_ndjson_sample_bytes(
                        &compressed,
                        path,
                        file_size,
                        max_memory_usage as usize,
                    )?;
                    let is_truncated = data.len() > NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE;
                    NdJsonSampleBytes { data, is_truncated }
                } else {
                    Self::read_compressed_ndjson_sample_bytes(operator, path, file_size, algo)
                        .await?
                }
            }
        };
        Ok(NdJsonSampleBytes {
            is_truncated: sample.is_truncated
                || sample.data.len() > NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE,
            data: sample
                .data
                .into_iter()
                .take(NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE)
                .collect(),
        })
    }

    fn check_zip_ndjson_sample_size(path: &str, file_size: u64) -> Result<()> {
        if file_size > NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE as u64 {
            return Err(ErrorCode::BadBytes(format!(
                "zip file {path} is too large for bounded NDJSON schema evolution sampling, compressed_size = {file_size}, maximum allowed = {}",
                NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE,
            )));
        }
        Ok(())
    }

    fn read_zip_ndjson_sample_bytes(
        compressed: &[u8],
        path: &str,
        file_size: u64,
        max_memory_usage: usize,
    ) -> Result<Vec<u8>> {
        Self::check_zip_ndjson_sample_size(path, file_size)?;
        DecompressDecoder::decompress_all_zip(compressed, path, max_memory_usage)
    }

    async fn read_compressed_ndjson_sample_bytes(
        operator: opendal::Operator,
        path: &str,
        file_size: u64,
        algo: CompressAlgorithm,
    ) -> Result<NdJsonSampleBytes> {
        let reader = operator.reader(path).await?;
        let mut decoder = DecompressDecoder::new(algo);
        let mut output = Vec::with_capacity(NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE);
        let compressed_read_limit =
            file_size.min(NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE as u64);
        let mut offset = 0_u64;

        while offset < compressed_read_limit
            && output.len() < NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE
            && !matches!(decoder.state(), DecompressState::Done)
        {
            let end = (offset + NDJSON_SCHEMA_EVOLUTION_COMPRESSED_READ_CHUNK_BYTES as u64)
                .min(compressed_read_limit);
            let chunk = reader.read(offset..end).await?.to_vec();
            if chunk.is_empty() {
                return Err(ErrorCode::BadBytes(format!(
                    "Unexpected EOF {path} expect {file_size} bytes, read only {offset} bytes.",
                )));
            }
            offset += chunk.len() as u64;
            decoder.fill(&chunk);
            Self::drain_ndjson_decompressor_sample(&mut decoder, &mut output, false)?;
        }

        if offset == file_size
            && output.len() < NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE
            && !matches!(decoder.state(), DecompressState::Done)
        {
            if matches!(decoder.state(), DecompressState::Reading) {
                decoder.fill(&[]);
            }
            Self::drain_ndjson_decompressor_sample(&mut decoder, &mut output, true)?;
            if !matches!(decoder.state(), DecompressState::Done) {
                return Err(ErrorCode::BadBytes(format!(
                    "decompressor state is {:?} after decompressing all data from {}",
                    decoder.state(),
                    path,
                )));
            }
        }

        Ok(NdJsonSampleBytes {
            is_truncated: offset < file_size || !matches!(decoder.state(), DecompressState::Done),
            data: output,
        })
    }

    fn drain_ndjson_decompressor_sample(
        decoder: &mut DecompressDecoder,
        output: &mut Vec<u8>,
        finish: bool,
    ) -> Result<()> {
        while output.len() < NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE {
            match decoder.state() {
                DecompressState::Decoding => {
                    let remaining = NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE - output.len();
                    let mut buf = vec![0_u8; remaining.min(4096)];
                    let written = decoder.decode(&mut buf).map_err(|e| {
                        ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
                    })?;
                    buf.truncate(written);
                    output.extend_from_slice(&buf);
                }
                DecompressState::Flushing => {
                    let remaining = NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE - output.len();
                    let mut buf = vec![0_u8; remaining.min(4096)];
                    let written = decoder.finish(&mut buf).map_err(|e| {
                        ErrorCode::InvalidCompressionData(format!("compression data invalid: {e}"))
                    })?;
                    buf.truncate(written);
                    output.extend_from_slice(&buf);
                }
                DecompressState::Reading if finish => {
                    decoder.fill(&[]);
                }
                DecompressState::Reading | DecompressState::Done => break,
            }
        }
        Ok(())
    }

    fn get_copy_into_table_result(&self) -> Result<Vec<DataBlock>> {
        let return_all = !self
            .plan
            .stage_table_info
            .copy_into_table_options
            .return_failed_only;
        let cs = self.ctx.copy_state().copy_status();

        let mut results = cs.files.iter().collect::<Vec<_>>();
        results.sort_by(|a, b| a.key().cmp(b.key()));

        let n = cs.files.len();
        let mut files = Vec::with_capacity(n);
        let mut rows_loaded = Vec::with_capacity(n);
        let mut errors_seen = Vec::with_capacity(n);
        let mut first_error = Vec::with_capacity(n);
        let mut first_error_line = Vec::with_capacity(n);

        for entry in results {
            let status = entry.value();
            if let Some(err) = &status.error {
                files.push(entry.key().clone());
                rows_loaded.push(status.num_rows_loaded as i32);
                errors_seen.push(err.num_errors as i32);
                first_error.push(Some(err.first_error.error.to_string().clone()));
                first_error_line.push(Some(err.first_error.line as i32 + 1));
            } else if return_all {
                files.push(entry.key().clone());
                rows_loaded.push(status.num_rows_loaded as i32);
                errors_seen.push(0);
                first_error.push(None);
                first_error_line.push(None);
            }
        }
        let blocks = vec![DataBlock::new_from_columns(vec![
            StringType::from_data(files),
            Int32Type::from_data(rows_loaded),
            Int32Type::from_data(errors_seen),
            StringType::from_opt_data(first_error),
            Int32Type::from_opt_data(first_error_line),
        ])];
        Ok(blocks)
    }

    /// Build commit insertion pipeline.
    async fn commit_insertion(
        &self,
        main_pipeline: &mut Pipeline,
        plan: &CopyIntoTablePlan,
        files_to_copy: Vec<StageFileInfo>,
        duplicated_files_detected: Vec<String>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        deduplicated_label: Option<String>,
        path_prefix: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
        new_schema: Option<TableSchemaRef>,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let mut to_table = ctx
            .get_table(
                plan.catalog_info.catalog_name(),
                &plan.database_name,
                &plan.table_name,
            )
            .await?;

        let mut prev_snapshot_id = None;

        // Commit.
        {
            let mut table_info = to_table.get_table_info().clone();
            if let Some(new_schema) = new_schema {
                let fuse_table = FuseTable::try_from_table(to_table.as_ref())?;
                let base_snapshot = fuse_table.read_table_snapshot().await?;
                prev_snapshot_id = base_snapshot.snapshot_id().map(|(id, _)| id);

                table_info.meta.fill_field_comments();
                while table_info.meta.field_comments.len() < new_schema.fields.len() {
                    table_info.meta.field_comments.push("".to_string());
                }
                table_info.meta.schema = new_schema;
                to_table = FuseTable::create_and_refresh_table_info(
                    table_info,
                    ctx.get_settings().get_s3_storage_class()?,
                )?
                .into();
            }

            let copied_files_meta_req = PipelineBuilder::build_upsert_copied_files_to_meta_req(
                ctx.clone(),
                to_table.as_ref(),
                &files_to_copy,
                &plan.stage_table_info.copy_into_table_options,
                path_prefix,
            )?;

            to_table.commit_insertion(
                ctx.clone(),
                main_pipeline,
                copied_files_meta_req,
                update_stream_meta,
                plan.write_mode.is_overwrite(),
                prev_snapshot_id,
                deduplicated_label,
                table_meta_timestamps,
            )?;
        }

        // Purge files.
        {
            info!(
                "set files to be purged, # of copied files: {}, # of duplicated files: {}",
                files_to_copy.len(),
                duplicated_files_detected.len()
            );

            let files_to_be_deleted = files_to_copy
                .into_iter()
                .map(|v| v.path)
                .chain(duplicated_files_detected)
                .collect::<Vec<_>>();
            // set on_finished callback.
            PipelineBuilder::set_purge_files_on_finished(
                ctx.clone(),
                files_to_be_deleted,
                &plan.stage_table_info.copy_into_table_options,
                plan.stage_table_info.stage_info.clone(),
                main_pipeline,
            )?;
        }
        Ok(())
    }

    async fn on_no_files_to_copy(&self) -> Result<PipelineBuildResult> {
        // currently, there is only one thing that we care about:
        //
        // if `purge_duplicated_files_in_copy` and `purge` are all enabled,
        // and there are duplicated files detected, we should clean them up immediately.

        // it might be better to reuse the PipelineBuilder::set_purge_files_on_finished,
        // unfortunately, hooking the on_finished callback of a "blank" pipeline,
        // e.g. `PipelineBuildResult::create` leads to runtime error (during pipeline execution).

        if self.plan.stage_table_info.copy_into_table_options.purge
            && !self
                .plan
                .stage_table_info
                .duplicated_files_detected
                .is_empty()
            && self
                .ctx
                .get_settings()
                .get_enable_purge_duplicated_files_in_copy()?
        {
            info!(
                "purge_duplicated_files_in_copy enabled, number of duplicated files: {}",
                self.plan.stage_table_info.duplicated_files_detected.len()
            );

            PipelineBuilder::purge_files_immediately(
                self.ctx.clone(),
                self.plan.stage_table_info.duplicated_files_detected.clone(),
                self.plan.stage_table_info.stage_info.clone(),
            )
            .await?;
        }
        Ok(PipelineBuildResult::create())
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyIntoTableInterpreter {
    fn name(&self) -> &str {
        "CopyIntoTableInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "copy_into_table_interpreter_execute_v2");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let plan = &self.plan;
        let to_table = self
            .ctx
            .get_table(
                plan.catalog_info.catalog_name(),
                &plan.database_name,
                &plan.table_name,
            )
            .await?;

        to_table.check_mutable()?;

        if self.plan.no_file_to_copy {
            info!("no file to copy");
            return self.on_no_files_to_copy().await;
        }

        let snapshot = FuseTable::try_from_table(to_table.as_ref())?
            .read_table_snapshot()
            .await?;
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(to_table.as_ref(), snapshot)?;

        let (physical_plan, update_stream_meta, new_schema) = self
            .build_physical_plan(
                to_table.get_table_info().clone(),
                &self.plan,
                table_meta_timestamps,
            )
            .await?;

        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;

        // Build commit insertion pipeline.
        {
            let files_to_copy = self
                .plan
                .stage_table_info
                .files_to_copy
                .clone()
                .unwrap_or_default();

            let duplicated_files_detected =
                self.plan.stage_table_info.duplicated_files_detected.clone();

            self.commit_insertion(
                &mut build_res.main_pipeline,
                &self.plan,
                files_to_copy,
                duplicated_files_detected,
                update_stream_meta,
                unsafe { self.ctx.get_settings().get_deduplicate_label()? },
                self.plan.path_prefix.clone(),
                table_meta_timestamps,
                new_schema,
            )
            .await?;
        }

        // Execute hook.
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                self.plan.catalog_info.catalog_name().to_string(),
                self.plan.database_name.to_string(),
                self.plan.table_name.to_string(),
                MutationKind::Insert,
                LockTableOption::LockNoRetry,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let blocks = if self.plan.no_file_to_copy {
            vec![DataBlock::empty_with_schema(&self.plan.schema())]
        } else {
            self.get_copy_into_table_result()?
        };

        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_ast::ast::CopySchemaEvolutionOptions;
    use databend_common_compress::CompressCodec;
    use databend_common_storage::StageFileStatus;

    use super::*;

    fn stage_file(path: &str) -> StageFileInfo {
        StageFileInfo {
            path: path.to_string(),
            size: 1,
            md5: None,
            last_modified: None,
            etag: None,
            status: StageFileStatus::NeedCopy,
            creator: None,
        }
    }

    #[test]
    fn test_ndjson_schema_evolution_auto_options() {
        let options = NdJsonSchemaEvolutionOptions::resolve(None, 100, 8);
        assert_eq!(options.sample_files, 32);
        assert_eq!(
            options.sample_records_per_file,
            NDJSON_SCHEMA_EVOLUTION_AUTO_RECORDS_PER_FILE
        );
        assert_eq!(
            options.sample_total_records,
            NDJSON_SCHEMA_EVOLUTION_AUTO_TOTAL_RECORDS
        );

        let explicit = CopySchemaEvolutionOptions {
            sample_files: Some(3),
            sample_records_per_file: Some(10),
            sample_total_records: Some(20),
        };
        let options = NdJsonSchemaEvolutionOptions::resolve(Some(&explicit), 100, 8);
        assert_eq!(options.sample_files, 3);
        assert_eq!(options.sample_records_per_file, 10);
        assert_eq!(options.sample_total_records, 20);
    }

    #[test]
    fn test_sample_ndjson_files_evenly() {
        let files = ["a", "b", "c", "d", "e"]
            .into_iter()
            .map(stage_file)
            .collect::<Vec<_>>();
        let sampled = CopyIntoTableInterpreter::sample_ndjson_files(&files, 3);
        let paths = sampled.iter().map(|f| f.path.as_str()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["a", "c", "e"]);

        let sampled = CopyIntoTableInterpreter::sample_ndjson_files(&files, 1);
        let paths = sampled.iter().map(|f| f.path.as_str()).collect::<Vec<_>>();
        assert_eq!(paths, vec!["c"]);
    }

    #[test]
    fn test_trim_ndjson_sample_bytes() -> Result<()> {
        assert_eq!(
            trim_ndjson_sample_bytes(b"{\"a\":1}\n{\"b\"", true, "sample.ndjson")?,
            b"{\"a\":1}\n"
        );
        assert_eq!(
            trim_ndjson_sample_bytes(b"{\"a\":1}\n", true, "sample.ndjson")?,
            b"{\"a\":1}\n"
        );
        assert_eq!(
            trim_ndjson_sample_bytes(b"{\"a\":1}", false, "sample.ndjson")?,
            b"{\"a\":1}"
        );
        let err = trim_ndjson_sample_bytes(b"{\"a\":1", true, "sample.ndjson").unwrap_err();
        assert!(err.message().contains("insufficient NDJSON sample data"));
        assert!(err.message().contains("sample.ndjson"));
        Ok(())
    }

    #[test]
    fn test_drain_compressed_ndjson_prefix_does_not_require_eof() -> Result<()> {
        let content = vec![b'a'; 1024 * 1024];
        let mut encoder = CompressCodec::from(CompressAlgorithm::Gzip);
        let compressed = encoder.compress_all(&content)?;
        let mut decoder = DecompressDecoder::new(CompressAlgorithm::Gzip);
        let mut output = Vec::new();

        decoder.fill(&compressed[..compressed.len() / 2]);
        CopyIntoTableInterpreter::drain_ndjson_decompressor_sample(
            &mut decoder,
            &mut output,
            false,
        )?;

        assert!(!matches!(decoder.state(), DecompressState::Done));
        assert!(output.len() < content.len());
        Ok(())
    }

    #[test]
    fn test_drain_compressed_ndjson_sample_caps_decompressed_bytes() -> Result<()> {
        let content = vec![b'a'; NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE + 1024];
        let mut encoder = CompressCodec::from(CompressAlgorithm::Gzip);
        let compressed = encoder.compress_all(&content)?;
        assert!(compressed.len() < NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE);

        let mut decoder = DecompressDecoder::new(CompressAlgorithm::Gzip);
        let mut sample = Vec::new();
        decoder.fill(&compressed);
        CopyIntoTableInterpreter::drain_ndjson_decompressor_sample(
            &mut decoder,
            &mut sample,
            false,
        )?;

        assert_eq!(sample.len(), NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE);
        assert!(sample.iter().all(|v| *v == b'a'));
        Ok(())
    }

    #[test]
    fn test_read_small_zip_ndjson_sample() -> Result<()> {
        let content = b"{\"a\":1}\n{\"a\":2}\n";
        let compressed = CompressCodec::compress_all_zip(content, "data.ndjson")?;
        let path = "/compressed/small.ndjson.zip";

        let sample = CopyIntoTableInterpreter::read_zip_ndjson_sample_bytes(
            &compressed,
            path,
            compressed.len() as u64,
            0,
        )?;

        assert_eq!(sample, content);
        Ok(())
    }

    #[test]
    fn test_read_large_zip_ndjson_sample_rejects_unbounded_read() -> Result<()> {
        let err = CopyIntoTableInterpreter::read_zip_ndjson_sample_bytes(
            &[],
            "/compressed/large.ndjson.zip",
            NDJSON_SCHEMA_EVOLUTION_SAMPLE_BYTES_PER_FILE as u64 + 1,
            0,
        )
        .unwrap_err();

        assert!(err.message().contains("bounded NDJSON schema evolution"));
        Ok(())
    }
}
