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

use std::intrinsics::unlikely;
use std::sync::Arc;

use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::FullParquetMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableField;
use databend_common_storage::read_metadata_async;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::InMemoryCacheReader;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Loader;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescPtr;
use parquet::schema::types::SchemaDescriptor;
use parquet::schema::types::Type as ParquetType;

use crate::statistics::collect_row_group_stats;

pub async fn read_metadata_async_cached(
    path: &str,
    operator: &Operator,
    file_size: Option<u64>,
    dedup_key: &str,
) -> Result<Arc<ParquetMetaData>> {
    let info = operator.info();
    let location = format!("{dedup_key}:{}/{}/{}", info.name(), info.root(), path);
    let reader = MetaReader::meta_data_reader(operator.clone(), location.len() - path.len());
    let load_params = LoadParams {
        location,
        len_hint: file_size,
        ver: 0,
        put_cache: true,
    };
    reader.read(&load_params).await
}

#[async_backtrace::framed]
pub async fn read_metas_in_parallel(
    op: &Operator,
    file_infos: &[(String, u64, String)],
    expected: (SchemaDescPtr, String),
    leaf_fields: Arc<Vec<TableField>>,
    num_threads: usize,
    max_memory_usage: u64,
    enable_cache: bool,
) -> Result<Vec<Arc<FullParquetMeta>>> {
    if file_infos.is_empty() {
        return Ok(vec![]);
    }
    let num_files = file_infos.len();

    let mut tasks = Vec::with_capacity(num_threads);
    // Equally distribute the tasks
    for i in 0..num_threads {
        let begin = num_files * i / num_threads;
        let end = num_files * (i + 1) / num_threads;
        if begin == end {
            continue;
        }

        let file_infos = file_infos[begin..end].to_vec();
        let op = op.clone();
        let (expected_schema, schema_from) = expected.clone();
        let leaf_fields = leaf_fields.clone();

        tasks.push(read_parquet_metas_batch(
            file_infos,
            op,
            expected_schema,
            leaf_fields,
            schema_from,
            max_memory_usage,
            enable_cache,
        ));
    }

    let metas = execute_futures_in_parallel(
        tasks,
        num_threads,
        num_threads * 2,
        "read-parquet-metas-worker".to_owned(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<_>>>()?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    Ok(metas)
}

pub(crate) fn check_parquet_schema(
    expect: &SchemaDescriptor,
    actual: &SchemaDescriptor,
    path: &str,
    schema_from: &str,
) -> Result<()> {
    if let Some(difference) =
        first_schema_difference(expect.root_schema(), actual.root_schema(), "")
    {
        return Err(ErrorCode::TableSchemaMismatch(format!(
            "Parquet schema mismatch in file '{}'. Schema inferred from '{}'. First difference at '{}': {} differs (inferred: {}, file: {})",
            path,
            schema_from,
            difference.path,
            difference.property,
            difference.expected,
            difference.actual,
        )));
    }
    Ok(())
}

struct SchemaDifference {
    path: String,
    property: &'static str,
    expected: String,
    actual: String,
}

fn first_schema_difference(
    expected: &ParquetType,
    actual: &ParquetType,
    path: &str,
) -> Option<SchemaDifference> {
    let expected_info = expected.get_basic_info();
    let actual_info = actual.get_basic_info();
    let current_path = if path.is_empty() { "<root>" } else { path };

    macro_rules! difference {
        ($property:expr, $expected:expr, $actual:expr) => {
            return Some(SchemaDifference {
                path: current_path.to_string(),
                property: $property,
                expected: format!("{:?}", $expected),
                actual: format!("{:?}", $actual),
            })
        };
    }

    // The root message name is producer-specific metadata (for example,
    // "schema" in Arrow and "spark_schema" in Spark), not a column name.
    if !path.is_empty() && expected_info.name() != actual_info.name() {
        difference!("field name", expected_info.name(), actual_info.name());
    }

    let expected_repetition = expected_info
        .has_repetition()
        .then(|| expected_info.repetition());
    let actual_repetition = actual_info
        .has_repetition()
        .then(|| actual_info.repetition());
    if expected_repetition != actual_repetition {
        difference!("repetition", expected_repetition, actual_repetition);
    }

    match (expected, actual) {
        (
            ParquetType::PrimitiveType {
                physical_type: expected_physical,
                type_length: expected_length,
                scale: expected_scale,
                precision: expected_precision,
                ..
            },
            ParquetType::PrimitiveType {
                physical_type: actual_physical,
                type_length: actual_length,
                scale: actual_scale,
                precision: actual_precision,
                ..
            },
        ) => {
            if expected_physical != actual_physical {
                difference!("physical type", expected_physical, actual_physical);
            }
            if expected_length != actual_length {
                difference!("type length", expected_length, actual_length);
            }
            if expected_precision != actual_precision {
                difference!("precision", expected_precision, actual_precision);
            }
            if expected_scale != actual_scale {
                difference!("scale", expected_scale, actual_scale);
            }
        }
        (ParquetType::GroupType { .. }, ParquetType::GroupType { .. }) => {}
        _ => difference!("node type", node_kind(expected), node_kind(actual)),
    }

    if expected_info.converted_type() != actual_info.converted_type() {
        difference!(
            "converted type",
            expected_info.converted_type(),
            actual_info.converted_type()
        );
    }
    if expected_info.logical_type_ref() != actual_info.logical_type_ref() {
        difference!(
            "logical type",
            expected_info.logical_type_ref(),
            actual_info.logical_type_ref()
        );
    }

    // Field IDs are metadata and are not used to map columns in stage Parquet reads.

    if let (
        ParquetType::GroupType {
            fields: expected_fields,
            ..
        },
        ParquetType::GroupType {
            fields: actual_fields,
            ..
        },
    ) = (expected, actual)
    {
        if expected_fields.len() != actual_fields.len() {
            difference!("field count", expected_fields.len(), actual_fields.len());
        }
        for (expected_field, actual_field) in expected_fields.iter().zip(actual_fields) {
            let field_path = if path.is_empty() {
                expected_field.name().to_string()
            } else {
                format!("{path}.{}", expected_field.name())
            };
            if let Some(difference) =
                first_schema_difference(expected_field, actual_field, &field_path)
            {
                return Some(difference);
            }
        }
    }

    None
}

fn node_kind(schema: &ParquetType) -> &'static str {
    match schema {
        ParquetType::PrimitiveType { .. } => "primitive",
        ParquetType::GroupType { .. } => "group",
    }
}

#[async_backtrace::framed]
pub async fn read_metas_in_parallel_for_copy(
    op: &Operator,
    file_infos: &[(String, u64)],
    num_threads: usize,
    max_memory_usage: u64,
) -> Result<Vec<Arc<FullParquetMeta>>> {
    if file_infos.is_empty() {
        return Ok(vec![]);
    }
    let num_files = file_infos.len();

    let mut tasks = Vec::with_capacity(num_threads);
    // Equally distribute the tasks
    for i in 0..num_threads {
        let begin = num_files * i / num_threads;
        let end = num_files * (i + 1) / num_threads;
        if begin == end {
            continue;
        }

        let file_infos = file_infos[begin..end].to_vec();
        let op = op.clone();

        tasks.push(read_parquet_metas_batch_for_copy(
            file_infos,
            op,
            max_memory_usage,
        ));
    }

    let metas = execute_futures_in_parallel(
        tasks,
        num_threads,
        num_threads * 2,
        "read-parquet-metas-worker".to_owned(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<_>>>()?
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    Ok(metas)
}

/// Load parquet meta and check if the schema is matched.
#[async_backtrace::framed]
async fn load_and_check_parquet_meta(
    file: &str,
    size: u64,
    op: Operator,
    expect: &SchemaDescriptor,
    schema_from: &str,
    enable_cache: bool,
    dedup_key: &str,
) -> Result<Arc<ParquetMetaData>> {
    let metadata = if enable_cache {
        read_metadata_async_cached(file, &op, Some(size), dedup_key).await?
    } else {
        Arc::new(read_metadata_async(file, &op, Some(size)).await?)
    };
    check_parquet_schema(
        expect,
        metadata.file_metadata().schema_descr(),
        file,
        schema_from,
    )?;
    Ok(metadata)
}

pub async fn read_parquet_metas_batch(
    file_infos: Vec<(String, u64, String)>,
    op: Operator,
    expect: SchemaDescPtr,
    leaf_fields: Arc<Vec<TableField>>,
    schema_from: String,
    max_memory_usage: u64,
    enable_cache: bool,
) -> Result<Vec<Arc<FullParquetMeta>>> {
    let mut metas = Vec::with_capacity(file_infos.len());
    for (location, size, dedup_key) in file_infos {
        let meta = load_and_check_parquet_meta(
            &location,
            size,
            op.clone(),
            &expect,
            &schema_from,
            enable_cache,
            &dedup_key,
        )
        .await?;
        if unlikely(meta.file_metadata().num_rows() == 0) {
            // Don't collect empty files
            continue;
        }
        let stats = collect_row_group_stats(meta.row_groups(), &leaf_fields, None);
        metas.push(Arc::new(FullParquetMeta {
            location,
            size,
            meta,
            row_group_level_stats: stats,
        }));
    }

    check_memory_usage(max_memory_usage)?;
    Ok(metas)
}

pub async fn read_parquet_metas_batch_for_copy(
    file_infos: Vec<(String, u64)>,
    op: Operator,
    max_memory_usage: u64,
) -> Result<Vec<Arc<FullParquetMeta>>> {
    let mut metas = Vec::with_capacity(file_infos.len());
    for (location, size) in file_infos {
        let meta = Arc::new(read_metadata_async(&location, &op, Some(size)).await?);
        if unlikely(meta.file_metadata().num_rows() == 0) {
            // Don't collect empty files
            continue;
        }
        metas.push(Arc::new(FullParquetMeta {
            location,
            size,
            meta,
            row_group_level_stats: None,
        }));
    }
    check_memory_usage(max_memory_usage)?;
    Ok(metas)
}

// TODO(parquet): how to limit the memory when running this method is to be determined.
fn check_memory_usage(max_memory_usage: u64) -> Result<()> {
    let used = GLOBAL_MEM_STAT.get_memory_usage();
    if (max_memory_usage - used as u64) < 100 * 1024 * 1024 {
        return Err(ErrorCode::Internal(format!(
            "not enough memory to load parquet file metas, max_memory_usage = {}, used = {}.",
            max_memory_usage, used
        )));
    }
    Ok(())
}

pub struct LoaderWrapper<T>(T, usize);
pub type ParquetMetaReader = InMemoryCacheReader<ParquetMetaData, LoaderWrapper<Operator>>;

pub struct MetaReader;
impl MetaReader {
    pub fn meta_data_reader(dal: Operator, prefix_len: usize) -> ParquetMetaReader {
        ParquetMetaReader::new(
            CacheManager::instance().get_parquet_meta_data_cache(),
            LoaderWrapper(dal, prefix_len),
        )
    }
}

#[async_trait::async_trait]
impl Loader<ParquetMetaData> for LoaderWrapper<Operator> {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<ParquetMetaData> {
        let location = &params.location[self.1..];
        let size = match params.len_hint {
            Some(v) => v,
            None => self.0.stat(location).await?.content_length(),
        };
        read_metadata_async(location, &self.0, Some(size)).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet::basic::ConvertedType;
    use parquet::basic::LogicalType;
    use parquet::basic::Repetition;
    use parquet::basic::Type as PhysicalType;
    use parquet::schema::types::SchemaDescriptor;
    use parquet::schema::types::Type;

    use super::check_parquet_schema;

    fn decimal_schema(logical_type: Option<LogicalType>) -> SchemaDescriptor {
        let deal = Type::primitive_type_builder("deal", PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(Repetition::OPTIONAL)
            .with_converted_type(ConvertedType::DECIMAL)
            .with_logical_type(logical_type)
            .with_length(9)
            .with_precision(20)
            .with_scale(0)
            .build()
            .unwrap();
        let root = Type::group_type_builder("spark_schema")
            .with_fields(vec![Arc::new(deal)])
            .build()
            .unwrap();
        SchemaDescriptor::new(Arc::new(root))
    }

    fn primitive_schema(
        root_name: &str,
        field_name: &str,
        physical_type: PhysicalType,
        field_id: Option<i32>,
    ) -> SchemaDescriptor {
        let field = Type::primitive_type_builder(field_name, physical_type)
            .with_repetition(Repetition::REQUIRED)
            .with_id(field_id)
            .build()
            .unwrap();
        let root = Type::group_type_builder(root_name)
            .with_fields(vec![Arc::new(field)])
            .build()
            .unwrap();
        SchemaDescriptor::new(Arc::new(root))
    }

    #[test]
    fn test_check_parquet_schema_reports_first_difference() {
        let inferred = decimal_schema(Some(LogicalType::Decimal {
            scale: 0,
            precision: 20,
        }));
        let file = decimal_schema(None);

        let error = check_parquet_schema(&inferred, &file, "actual.parquet", "inferred.parquet")
            .unwrap_err()
            .to_string();

        assert!(error.contains("Parquet schema mismatch in file 'actual.parquet'"));
        assert!(error.contains("Schema inferred from 'inferred.parquet'"));
        assert!(error.contains("First difference at 'deal': logical type differs"));
        assert!(error.contains("file: None"));
    }

    #[test]
    fn test_check_parquet_schema_ignores_root_name_in_diagnostics() {
        let inferred = primitive_schema("schema", "id", PhysicalType::INT32, None);
        let file = primitive_schema(
            "spark_schema",
            "resourceType",
            PhysicalType::BYTE_ARRAY,
            None,
        );

        let error = check_parquet_schema(&inferred, &file, "actual.parquet", "inferred.parquet")
            .unwrap_err()
            .to_string();

        assert!(error.contains(
            "First difference at 'id': field name differs (inferred: \"id\", file: \"resourceType\")"
        ));
    }

    #[test]
    fn test_check_parquet_schema_accepts_different_root_names() {
        let inferred = primitive_schema("schema", "id", PhysicalType::INT32, None);
        let file = primitive_schema("spark_schema", "id", PhysicalType::INT32, None);

        check_parquet_schema(&inferred, &file, "actual.parquet", "inferred.parquet").unwrap();
    }

    #[test]
    fn test_check_parquet_schema_accepts_different_field_ids() {
        let inferred = primitive_schema("schema", "id", PhysicalType::INT32, Some(1));
        let file = primitive_schema("schema", "id", PhysicalType::INT32, Some(2));

        check_parquet_schema(&inferred, &file, "actual.parquet", "inferred.parquet").unwrap();
    }

    #[test]
    fn test_check_parquet_schema_reports_node_kind() {
        let nested = Type::group_type_builder("value")
            .with_repetition(Repetition::REQUIRED)
            .with_fields(vec![Arc::new(
                Type::primitive_type_builder("id", PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap(),
            )])
            .build()
            .unwrap();
        let inferred = SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![Arc::new(nested)])
                .build()
                .unwrap(),
        ));
        let file = primitive_schema("schema", "value", PhysicalType::INT32, None);

        let error = check_parquet_schema(&inferred, &file, "actual.parquet", "inferred.parquet")
            .unwrap_err()
            .to_string();

        assert!(error.contains(
            "First difference at 'value': node type differs (inferred: \"group\", file: \"primitive\")"
        ));
    }

    #[test]
    fn test_check_parquet_schema_reports_field_count_before_field_name() {
        let inferred = SchemaDescriptor::new(Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![
                    Arc::new(
                        Type::primitive_type_builder("a", PhysicalType::INT32)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                    Arc::new(
                        Type::primitive_type_builder("b", PhysicalType::INT32)
                            .with_repetition(Repetition::REQUIRED)
                            .build()
                            .unwrap(),
                    ),
                ])
                .build()
                .unwrap(),
        ));
        let file = primitive_schema("schema", "b", PhysicalType::INT32, None);

        let error = check_parquet_schema(&inferred, &file, "actual.parquet", "inferred.parquet")
            .unwrap_err()
            .to_string();

        assert!(
            error.contains(
                "First difference at '<root>': field count differs (inferred: 2, file: 1)"
            )
        );
    }
}
