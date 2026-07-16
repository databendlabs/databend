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
use std::sync::Arc;

use databend_common_ast::ast::ColumnMatchMode;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ParquetCopySchema;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::plan::StreamColumn;
use databend_common_catalog::plan::StreamColumnType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
use databend_common_expression::ORIGIN_VERSION_COL_NAME;
use databend_common_expression::TableSchema;
use databend_common_expression::is_stream_column;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_pipeline::core::Pipeline;
use databend_common_storage::FileStatus;
use databend_common_storage::parquet::infer_schema_with_extension;
use parquet::file::metadata::FileMetaData;

use crate::ParquetPart;
use crate::copy_into_table::reader::RowGroupReaderForCopy;
use crate::copy_into_table::source::ParquetCopySource;
use crate::meta::read_metas_in_parallel_for_copy;
use crate::meta::read_metas_in_parallel_for_copy_by_paths;
use crate::partition::ParquetRowGroupPart;
use crate::schema::arrow_to_table_schema;

pub struct ParquetTableForCopy {}

impl ParquetTableForCopy {
    /// Read and validate every Fuse recovery footer before a write pipeline is built.
    #[async_backtrace::framed]
    pub async fn prepare_fuse_recovery(
        stage_table_info: &mut StageTableInfo,
        ctx: Arc<dyn TableContext>,
    ) -> Result<()> {
        let Some(recovery) = &stage_table_info.fuse_recovery else {
            return Ok(());
        };
        let expected_schema = recovery
            .table_info
            .schema()
            .remove_virtual_computed_fields();
        let file_paths = stage_table_info
            .files_to_copy
            .as_ref()
            .expect("ParquetTableForCopy::prepare_fuse_recovery requires files_to_copy")
            .iter()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>();
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let max_memory_usage = settings.get_max_memory_usage()?;
        let operator = stage_table_info.operator()?;
        let metas = read_metas_in_parallel_for_copy_by_paths(
            &operator,
            &file_paths,
            max_threads,
            max_memory_usage,
        )
        .await?;

        if metas.len() != file_paths.len() {
            let loaded = metas
                .iter()
                .map(|meta| meta.location.as_str())
                .collect::<HashSet<_>>();
            let missing = file_paths
                .iter()
                .filter(|path| !loaded.contains(path.as_str()))
                .map(String::as_str)
                .collect::<Vec<_>>();
            return Err(ErrorCode::BadBytes(format!(
                "FUSE_RECOVERY_BLOCKS requires a non-empty Parquet block for every FILES entry; invalid files: {}",
                missing.join(", ")
            )));
        }

        let sizes = metas
            .iter()
            .map(|meta| (meta.location.as_str(), meta.size))
            .collect::<HashMap<_, _>>();
        for file in stage_table_info
            .files_to_copy
            .as_mut()
            .expect("ParquetTableForCopy::prepare_fuse_recovery requires files_to_copy")
        {
            file.size = *sizes
                .get(file.path.as_str())
                .expect("each recovery file must have prepared Parquet metadata");
        }

        let mut copy_schemas: Vec<ParquetCopySchema> = Vec::new();
        for meta in &metas {
            if meta.meta.row_groups().len() != 1 || meta.meta.file_metadata().num_rows() <= 0 {
                return Err(ErrorCode::BadBytes(format!(
                    "FUSE_RECOVERY_BLOCKS source '{}' is not a non-empty single-row-group Fuse block",
                    meta.location
                )));
            }
            let file_metadata = meta.meta.file_metadata();
            let arrow_schema = infer_schema_with_extension(file_metadata)?;
            let file_schema = arrow_to_table_schema(&arrow_schema, true, true)?;
            validate_fuse_recovery_schema(&meta.location, &file_schema, &expected_schema)?;

            let schema_descr = file_metadata.schema_descr_ptr();
            if !copy_schemas.iter().any(|schema| {
                schema.arrow_schema == arrow_schema && schema.schema_descr == schema_descr
            }) {
                copy_schemas.push(ParquetCopySchema {
                    arrow_schema,
                    schema_descr,
                });
            }
        }

        stage_table_info.parquet_copy_schemas = copy_schemas;
        stage_table_info.parquet_metas = Some(metas);
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn do_read_partitions(
        stage_table_info: &StageTableInfo,
        ctx: Arc<dyn TableContext>,
        _push_down: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let files = stage_table_info.files_to_copy.as_ref().expect(
            "ParquetTableForCopy::do_read_partitions must be called with files_to_copy set",
        );
        let file_infos = files
            .iter()
            .filter(|f| f.size > 0)
            .map(|f| (f.path.clone(), f.size))
            .collect::<Vec<_>>();
        let total_size = file_infos.iter().map(|(_, size)| *size as usize).sum();

        let metas = if let Some(v) = &stage_table_info.parquet_metas {
            v.clone()
        } else {
            let settings = ctx.get_settings();
            let max_threads = settings.get_max_threads()? as usize;
            let max_memory_usage = settings.get_max_memory_usage()?;

            let operator = stage_table_info.operator()?;
            read_metas_in_parallel_for_copy(&operator, &file_infos, max_threads, max_memory_usage)
                .await?
        };

        let mut schemas = vec![];
        let mut parts = vec![];
        let copy_status = ctx.copy_state().copy_status();
        let mut stats = PartStatistics::default();
        for meta in metas.iter() {
            let file_metadata = meta.meta.file_metadata();
            let schema = file_metadata.schema_descr_ptr();
            let schema_index = if stage_table_info.fuse_recovery.is_some() {
                let arrow_schema = infer_schema_with_extension(file_metadata)?;
                stage_table_info
                    .parquet_copy_schemas
                    .iter()
                    .position(|prepared| {
                        prepared.arrow_schema == arrow_schema && prepared.schema_descr == schema
                    })
                    .ok_or_else(|| {
                        ErrorCode::Internal(format!(
                            "missing prepared recovery schema for {}",
                            meta.location
                        ))
                    })?
            } else {
                match schemas.iter().position(|s| s == &schema) {
                    Some(i) => i,
                    None => {
                        schemas.push(schema);
                        schemas.len() - 1
                    }
                }
            };
            let num_rows = meta.meta.file_metadata().num_rows() as usize;
            stats.read_rows += num_rows;
            // For files that will produce no rows, no blocks will be emitted
            // by the source processor, so register them here to ensure they
            // appear in the COPY result with rows_loaded = 0.
            if meta.meta.file_metadata().num_rows() == 0 {
                copy_status.add_chunk(meta.location.as_str(), FileStatus {
                    num_rows_loaded: 0,
                    error: None,
                });
            }
            let mut start_row = 0;
            for rg in meta.meta.row_groups() {
                let part = ParquetRowGroupPart {
                    location: meta.location.clone(),
                    start_row,
                    meta: rg.clone(),
                    schema_index,
                    uncompressed_size: rg.total_byte_size() as u64,
                    compressed_size: rg.compressed_size() as u64,
                    sort_min_max: None,
                    omit_filter: false,
                    page_locations: None,
                    selectors: None,
                };
                start_row += rg.num_rows() as u64;
                parts.push(part);
            }
        }
        let parts: Vec<_> = parts
            .into_iter()
            .map(|p| Arc::new(Box::new(ParquetPart::RowGroup(p)) as Box<dyn PartInfo>))
            .collect();

        stats.partitions_scanned = parts.len();
        stats.partitions_total = parts.len();
        stats.read_bytes = total_size;

        Ok((stats, Partitions::create(PartitionsShuffleKind::Mod, parts)))
    }

    pub fn do_read_data(
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let stage_table_info =
            if let DataSourceInfo::StageSource(stage_table_info) = &plan.source_info {
                stage_table_info
            } else {
                return Err(ErrorCode::Internal(
                    "bug: ParquetTableForCopy::read_data must be called with StageSource",
                ));
            };
        let case_sensitive = stage_table_info.copy_into_table_options.column_match_mode
            == Some(ColumnMatchMode::CaseSensitive);

        let fmt = match &stage_table_info.stage_info.file_format_params {
            FileFormatParams::Parquet(fmt) => fmt,
            _ => unreachable!("do_read_partitions expect parquet"),
        };

        let operator = stage_table_info.operator()?;

        let mut readers = HashMap::new();

        for part in &plan.parts.partitions {
            let part = part.as_any().downcast_ref::<ParquetPart>().unwrap();
            match part {
                ParquetPart::RowGroup(part) => {
                    if let std::collections::hash_map::Entry::Vacant(e) =
                        readers.entry(part.schema_index)
                    {
                        let reader = if stage_table_info.fuse_recovery.is_some() {
                            let prepared = stage_table_info
                                .parquet_copy_schemas
                                .get(part.schema_index)
                                .ok_or_else(|| {
                                    ErrorCode::Internal(format!(
                                        "invalid recovery schema index {}",
                                        part.schema_index
                                    ))
                                })?;
                            RowGroupReaderForCopy::try_create_with_schema(
                                &part.location,
                                ctx.clone(),
                                operator.clone(),
                                prepared.arrow_schema.clone(),
                                prepared.schema_descr.clone(),
                                stage_table_info.schema.clone(),
                                stage_table_info.default_exprs.clone(),
                                &fmt.missing_field_as,
                                case_sensitive,
                                fmt.use_logic_type,
                            )?
                        } else {
                            // TODO: preserve key-value metadata for all Parquet COPY sources.
                            let file_meta_data = FileMetaData::new(
                                0,
                                0,
                                None,
                                None,
                                part.meta.schema_descr_ptr(),
                                None,
                            );
                            RowGroupReaderForCopy::try_create(
                                &part.location,
                                ctx.clone(),
                                operator.clone(),
                                &file_meta_data,
                                stage_table_info.schema.clone(),
                                stage_table_info.default_exprs.clone(),
                                &fmt.missing_field_as,
                                case_sensitive,
                                fmt.use_logic_type,
                            )?
                        };
                        e.insert(reader);
                    }
                }
                _ => unreachable!(),
            }
        }
        let readers = Arc::new(readers);
        ctx.set_partitions(plan.parts.clone())?;

        let data_schema = Arc::new(DataSchema::from(&stage_table_info.schema));
        pipeline.add_source(
            |output| {
                ParquetCopySource::try_create(
                    ctx.clone(),
                    output,
                    readers.clone(),
                    operator.clone(),
                    data_schema.clone(),
                )
            },
            max_threads,
        )?;
        Ok(())
    }
}

fn validate_fuse_recovery_schema(
    location: &str,
    source_schema: &TableSchema,
    expected_schema: &TableSchema,
) -> Result<()> {
    // SAFETY: Footer-only recovery has no stable ColumnId. Exact name/type matching
    // is sound only under the FuseRecoveryBlocksInfo safety contract documented in
    // databend-common-catalog. In particular, same-name DROP+ADD and rename cycles
    // that return to the same physical schema are not detectable from a footer.
    let mut source_fields = HashMap::with_capacity(source_schema.num_fields());
    for field in source_schema.fields() {
        if is_stream_column(field.name()) {
            let expected_type = match field.name().as_str() {
                ORIGIN_VERSION_COL_NAME => {
                    StreamColumn::new(ORIGIN_VERSION_COL_NAME, StreamColumnType::OriginVersion)
                        .table_data_type()
                }
                ORIGIN_BLOCK_ID_COL_NAME => {
                    StreamColumn::new(ORIGIN_BLOCK_ID_COL_NAME, StreamColumnType::OriginBlockId)
                        .table_data_type()
                }
                ORIGIN_BLOCK_ROW_NUM_COL_NAME => StreamColumn::new(
                    ORIGIN_BLOCK_ROW_NUM_COL_NAME,
                    StreamColumnType::OriginRowNum,
                )
                .table_data_type(),
                _ => unreachable!(),
            };
            if field.data_type() != &expected_type {
                return Err(recovery_schema_mismatch(
                    location,
                    format!(
                        "stream column '{}' has type {}, expected {}",
                        field.name(),
                        field.data_type(),
                        expected_type
                    ),
                ));
            }
            continue;
        }
        if source_fields.insert(field.name(), field).is_some() {
            return Err(recovery_schema_mismatch(
                location,
                format!("duplicate source column '{}'", field.name()),
            ));
        }
    }

    let expected_fields = expected_schema
        .fields()
        .iter()
        .map(|field| (field.name(), field))
        .collect::<HashMap<_, _>>();
    let mut differences = Vec::new();
    for (name, expected) in &expected_fields {
        match source_fields.get(name) {
            None => differences.push(format!("missing column '{name}'")),
            Some(actual) if actual.data_type() != expected.data_type() => {
                differences.push(format!(
                    "column '{}' has type {}, expected {}",
                    name,
                    actual.data_type(),
                    expected.data_type()
                ))
            }
            Some(_) => {}
        }
    }
    for name in source_fields.keys() {
        if !expected_fields.contains_key(name) {
            differences.push(format!("unexpected column '{name}'"));
        }
    }
    differences.sort();
    if !differences.is_empty() {
        return Err(recovery_schema_mismatch(location, differences.join(", ")));
    }
    Ok(())
}

fn recovery_schema_mismatch(location: &str, reason: String) -> ErrorCode {
    ErrorCode::TableSchemaMismatch(format!(
        "FUSE_RECOVERY_BLOCKS schema mismatch for '{}': {}. Recovery requires exact case-sensitive names and complete logical types",
        location, reason
    ))
}

#[cfg(test)]
mod tests {
    use databend_common_catalog::plan::StreamColumn;
    use databend_common_catalog::plan::StreamColumnType;
    use databend_common_expression::ORIGIN_BLOCK_ID_COL_NAME;
    use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
    use databend_common_expression::ORIGIN_VERSION_COL_NAME;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;

    use super::validate_fuse_recovery_schema;

    fn schema(fields: Vec<(&str, TableDataType)>) -> TableSchema {
        TableSchema::new(
            fields
                .into_iter()
                .map(|(name, data_type)| TableField::new(name, data_type))
                .collect(),
        )
    }

    fn uint64() -> TableDataType {
        TableDataType::Number(NumberDataType::UInt64)
    }

    fn assert_mismatch(source: &TableSchema, expected: &TableSchema, reason: &str) {
        let error = validate_fuse_recovery_schema("block.parquet", source, expected).unwrap_err();
        assert!(
            error.message().contains(reason),
            "expected mismatch containing {reason:?}, got: {}",
            error.message()
        );
    }

    #[test]
    fn test_fuse_recovery_schema_allows_reordered_top_level_fields() {
        let expected = schema(vec![("id", uint64()), ("payload", TableDataType::String)]);
        let source = schema(vec![("payload", TableDataType::String), ("id", uint64())]);

        validate_fuse_recovery_schema("block.parquet", &source, &expected).unwrap();
    }

    #[test]
    fn test_fuse_recovery_schema_rejects_missing_extra_type_and_nullability() {
        let expected = schema(vec![("id", uint64()), ("payload", TableDataType::String)]);

        let missing = schema(vec![("id", uint64())]);
        assert_mismatch(&missing, &expected, "missing column 'payload'");

        let extra = schema(vec![
            ("id", uint64()),
            ("payload", TableDataType::String),
            ("extra", TableDataType::Boolean),
        ]);
        assert_mismatch(&extra, &expected, "unexpected column 'extra'");

        let wrong_type = schema(vec![("id", uint64()), ("payload", TableDataType::Boolean)]);
        assert_mismatch(&wrong_type, &expected, "column 'payload' has type Boolean");

        let wrong_nullability = schema(vec![
            ("id", uint64()),
            (
                "payload",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ]);
        assert_mismatch(
            &wrong_nullability,
            &expected,
            "column 'payload' has type String NULL",
        );
    }

    #[test]
    fn test_fuse_recovery_schema_rejects_nested_mismatch() {
        let expected_nested = TableDataType::Tuple {
            fields_name: vec!["name".to_string(), "active".to_string()],
            fields_type: vec![TableDataType::String, TableDataType::Boolean],
        };
        let source_nested = TableDataType::Tuple {
            fields_name: vec!["name".to_string(), "active".to_string()],
            fields_type: vec![TableDataType::String, uint64()],
        };
        let expected = schema(vec![("nested", expected_nested)]);
        let source = schema(vec![("nested", source_nested)]);

        assert_mismatch(&source, &expected, "column 'nested' has type");
    }

    #[test]
    fn test_fuse_recovery_schema_ignores_valid_stream_fields() {
        let expected = schema(vec![("id", uint64()), ("payload", TableDataType::String)]);
        let source = TableSchema::new(vec![
            StreamColumn::new(ORIGIN_VERSION_COL_NAME, StreamColumnType::OriginVersion)
                .table_field(),
            TableField::new("payload", TableDataType::String),
            StreamColumn::new(ORIGIN_BLOCK_ID_COL_NAME, StreamColumnType::OriginBlockId)
                .table_field(),
            TableField::new("id", uint64()),
            StreamColumn::new(
                ORIGIN_BLOCK_ROW_NUM_COL_NAME,
                StreamColumnType::OriginRowNum,
            )
            .table_field(),
        ]);

        validate_fuse_recovery_schema("block.parquet", &source, &expected).unwrap();
    }

    #[test]
    fn test_fuse_recovery_schema_rejects_stream_field_with_wrong_type() {
        let expected = schema(vec![("id", uint64())]);
        let source = TableSchema::new(vec![
            TableField::new("id", uint64()),
            TableField::new(ORIGIN_VERSION_COL_NAME, TableDataType::String),
        ]);

        assert_mismatch(
            &source,
            &expected,
            "stream column '_origin_version' has type String",
        );
    }
}
