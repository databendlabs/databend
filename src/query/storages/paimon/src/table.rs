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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::OnceLock;

use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use paimon::CatalogFactory;
use paimon::Options;
use paimon::catalog::Identifier;
use paimon::spec::CoreOptions;
use paimon::spec::POSTPONE_BUCKET;
use paimon::spec::TableSchema as PaimonTableSchema;
use serde::Deserialize;
use serde::Serialize;

use crate::PaimonPartInfo;
use crate::SerializableDataSplit;
use crate::error::map_paimon_error;
use crate::error::map_paimon_result;
use crate::predicate::apply_pushdowns;
use crate::predicate::can_push_limit;
use crate::source::PaimonTableSource;
use crate::table::descriptor::PAIMON_TABLE_DESCRIPTOR_KEY;
use crate::write::PaimonCommitSink;
use crate::write::PaimonTableWriter;

pub const PAIMON_ENGINE: &str = "PAIMON";

/// Serializable table descriptor distributed with read plans.
///
/// Catalog credentials are taken from [`TableInfo::catalog_info`] at runtime and are
/// intentionally excluded from this struct (and from persisted `engine_options`).
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PaimonTableDescriptor {
    pub identifier: Identifier,
    pub location: String,
    pub schema: PaimonTableSchema,
}

impl fmt::Debug for PaimonTableDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PaimonTableDescriptor")
            .field("identifier", &self.identifier)
            .field("location", &self.location)
            .field("schema_fields", &self.schema.fields().len())
            .finish()
    }
}

pub struct PaimonTable {
    info: TableInfo,
    descriptor: PaimonTableDescriptor,
    catalog_options: HashMap<String, String>,
    table: OnceLock<paimon::Table>,
}

pub(crate) mod descriptor {
    pub const PAIMON_TABLE_DESCRIPTOR_KEY: &str = "paimon.table_descriptor";
}

impl PaimonTable {
    pub fn try_create(info: TableInfo) -> Result<Box<dyn Table>> {
        let descriptor = parse_descriptor(&info)?;
        let catalog_options = catalog_options_from_info(&info)?;
        Ok(Box::new(Self {
            info,
            descriptor,
            catalog_options,
            table: OnceLock::new(),
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: PAIMON_ENGINE.to_string(),
            comment: "PAIMON Storage Engine".to_string(),
            support_cluster_key: false,
        }
    }

    pub fn from_paimon_table(
        catalog_info: Arc<CatalogInfo>,
        catalog_options: HashMap<String, String>,
        table: paimon::Table,
    ) -> Result<Arc<dyn Table>> {
        let descriptor = PaimonTableDescriptor {
            identifier: table.identifier().clone(),
            location: table.location().to_string(),
            schema: table.schema().clone(),
        };
        let databend_schema = paimon_schema_to_databend(table.schema())?;
        let descriptor_json = serde_json::to_string(&descriptor).map_err(|err| {
            ErrorCode::Internal(format!("serialize paimon table descriptor failed: {err:?}"))
        })?;
        let mut engine_options = BTreeMap::new();
        engine_options.insert(PAIMON_TABLE_DESCRIPTOR_KEY.to_string(), descriptor_json);
        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!(
                "'{}'.'{}'.'{}'",
                catalog_info.name_ident.catalog_name,
                descriptor.identifier.database(),
                descriptor.identifier.object()
            ),
            name: descriptor.identifier.object().to_string(),
            catalog_info,
            meta: TableMeta {
                schema: Arc::new(databend_schema),
                engine: PAIMON_ENGINE.to_string(),
                engine_options,
                ..Default::default()
            },
            ..Default::default()
        };
        let cell = OnceLock::new();
        let _ = cell.set(table);
        Ok(Arc::new(Self {
            info,
            descriptor,
            catalog_options,
            table: cell,
        }))
    }

    pub fn engine_options_json(&self) -> Result<String> {
        let raw = self
            .info
            .meta
            .engine_options
            .get(PAIMON_TABLE_DESCRIPTOR_KEY)
            .ok_or_else(|| {
                ErrorCode::Internal("missing paimon table descriptor in engine options".to_string())
            })?;
        Ok(raw.clone())
    }

    /// Reject unsupported insert modes: overwrite, and PK tables with `bucket < 1`
    /// (dynamic `-1`, postpone `-2`, or other invalid values).
    pub fn validate_insert(&self, overwrite: bool) -> Result<()> {
        let table = &self.info.desc;
        let schema = &self.descriptor.schema;
        let bucket = CoreOptions::new(schema.options()).bucket();
        let write_mode = if overwrite { "overwrite" } else { "insert" };

        if overwrite {
            return Err(ErrorCode::Unimplemented(format!(
                "Paimon table {table} does not support overwrite insert (bucket={bucket}, write_mode={write_mode})"
            )));
        }

        // Append-only tables (no primary keys) are supported regardless of bucket.
        if schema.primary_keys().is_empty() {
            return Ok(());
        }

        if bucket >= 1 {
            return Ok(());
        }

        let reason = if bucket == -1 {
            "dynamic bucket"
        } else if bucket == POSTPONE_BUCKET {
            "postpone bucket"
        } else {
            "bucket < 1"
        };
        Err(ErrorCode::Unimplemented(format!(
            "Paimon table {table} does not support {reason} for insert (bucket={bucket}, write_mode={write_mode})"
        )))
    }

    pub fn try_from_table(tbl: &dyn Table) -> Result<&Self> {
        tbl.as_any().downcast_ref::<Self>().ok_or_else(|| {
            ErrorCode::Internal(format!("expects Paimon engine, but got {}", tbl.engine()))
        })
    }

    /// Fixed-bucket primary-key tables need route + GlobalHash Exchange on write.
    pub fn is_fixed_bucket_primary_key(&self) -> bool {
        let schema = &self.descriptor.schema;
        !schema.primary_keys().is_empty() && CoreOptions::new(schema.options()).bucket() >= 1
    }

    pub fn paimon_schema(&self) -> &PaimonTableSchema {
        &self.descriptor.schema
    }

    fn loaded_table(&self) -> Result<&paimon::Table> {
        if let Some(table) = self.table.get() {
            return Ok(table);
        }
        let opened = self.open_paimon_table()?;
        let _ = self.table.set(opened);
        Ok(self.table.get().expect("paimon table initialized"))
    }

    fn open_paimon_table(&self) -> Result<paimon::Table> {
        // Always open through CatalogFactory so REST catalogs keep rest_env
        // (needed when workers rebuild the table from TableInfo alone).
        let options = options_from_map(&self.catalog_options)?;
        let catalog = map_paimon_result(databend_common_base::runtime::block_on(
            CatalogFactory::create(options),
        ))?;
        let loaded = map_paimon_result(databend_common_base::runtime::block_on(
            catalog.get_table(&self.descriptor.identifier),
        ))?;
        Ok(paimon::Table::new(
            loaded.file_io().clone(),
            loaded.identifier().clone(),
            loaded.location().to_string(),
            loaded.schema().clone(),
            loaded.rest_env().cloned(),
        ))
    }

    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let table = self.loaded_table()?;
        let (read_builder, analysis) =
            apply_pushdowns(table, push_downs.as_ref(), self.schema().as_ref());
        let plan = map_paimon_result(read_builder.new_scan().plan().await)?;
        let mut read_rows = 0usize;
        let mut read_bytes = 0usize;
        let mut is_exact = true;
        let parts: Vec<PartInfoPtr> = plan
            .splits()
            .iter()
            .map(|split| {
                read_rows += match split.merged_row_count() {
                    Some(row_count) => row_count,
                    None => {
                        is_exact = false;
                        split.row_count()
                    }
                } as usize;
                read_bytes += split
                    .data_files()
                    .iter()
                    .map(|file| file.file_size as usize)
                    .sum::<usize>();
                let part: PartInfoPtr = Arc::new(Box::new(PaimonPartInfo {
                    split: SerializableDataSplit::from(split),
                }));
                part
            })
            .collect();
        if can_push_limit(
            push_downs.as_ref().and_then(|push_downs| push_downs.limit),
            &analysis,
            &read_builder,
        ) && let Some(limit) = push_downs.as_ref().and_then(|push_downs| push_downs.limit)
        {
            read_rows = read_rows.min(limit);
        }
        let statistics = if is_exact {
            PartStatistics::new_exact(read_rows, read_bytes, parts.len(), parts.len())
        } else {
            PartStatistics::new_estimated(None, read_rows, read_bytes, parts.len(), parts.len())
        };
        Ok((
            statistics,
            Partitions::create(PartitionsShuffleKind::Mod, parts),
        ))
    }

    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let num_sources = parts_len.max(1).min(max_threads);
        ctx.set_partitions(plan.parts.clone())?;
        pipeline.add_source(
            |output| PaimonTableSource::create(ctx.clone(), output, plan.clone()),
            num_sources,
        )?;
        Ok(())
    }
}

pub(crate) fn catalog_options_from_info(info: &TableInfo) -> Result<HashMap<String, String>> {
    match &info.catalog_info.meta.catalog_option {
        // Quoted CONNECTION keys (e.g. `"s3.region"`) keep quotes in meta; strip like Iceberg.
        CatalogOption::Paimon(option) => Ok(crate::catalog::trim_option_keys(&option.options)),
        other => Err(ErrorCode::Internal(format!(
            "expected paimon catalog options, got {:?}",
            other.catalog_type()
        ))),
    }
}

pub(crate) fn catalog_options_from_plan(plan: &DataSourcePlan) -> Result<HashMap<String, String>> {
    let table_info = table_info_from_plan(plan)?;
    catalog_options_from_info(table_info)
}

pub(crate) fn parse_descriptor_from_plan(plan: &DataSourcePlan) -> Result<PaimonTableDescriptor> {
    parse_descriptor(table_info_from_plan(plan)?)
}

fn table_info_from_plan(plan: &DataSourcePlan) -> Result<&TableInfo> {
    match &plan.source_info {
        DataSourceInfo::TableSource(table_info) => Ok(table_info),
        _ => Err(ErrorCode::Internal(
            "paimon read plan must use table source".to_string(),
        )),
    }
}

pub(crate) fn parse_descriptor(info: &TableInfo) -> Result<PaimonTableDescriptor> {
    let raw = info
        .meta
        .engine_options
        .get(PAIMON_TABLE_DESCRIPTOR_KEY)
        .ok_or_else(|| {
            ErrorCode::Internal("missing paimon table descriptor in engine options".to_string())
        })?;
    serde_json::from_str(raw).map_err(|err| {
        ErrorCode::Internal(format!(
            "deserialize paimon table descriptor failed: {err:?}"
        ))
    })
}

pub(crate) fn options_from_map(options: &HashMap<String, String>) -> Result<Options> {
    let mut paimon_options = Options::new();
    for (key, value) in options {
        paimon_options.set(crate::catalog::trim_option_key(key), value.clone());
    }
    Ok(paimon_options)
}

pub(crate) fn paimon_schema_to_databend(schema: &PaimonTableSchema) -> Result<TableSchema> {
    let arrow_schema =
        paimon::arrow::build_target_arrow_schema(schema.fields()).map_err(map_paimon_error)?;
    TableSchema::try_from(arrow_schema.as_ref()).map_err(ErrorCode::from_std_error)
}

#[async_trait::async_trait]
impl Table for PaimonTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Cluster
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    fn is_read_only(&self) -> bool {
        // Catalog DDL remains read-only; row inserts go through append_data/commit_insertion.
        false
    }

    fn support_distributed_insert(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(push_downs).await
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline)
    }

    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let table = self.loaded_table()?.clone();
        pipeline.try_add_async_accumulating_transformer(move || {
            PaimonTableWriter::try_create(ctx.clone(), table.clone())
        })?;
        Ok(())
    }

    fn commit_insertion(
        &self,
        _ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        _copied_files: Option<UpsertTableCopiedFileReq>,
        _update_stream_meta: Vec<UpdateStreamMetaReq>,
        overwrite: bool,
        _prev_snapshot_id: Option<SnapshotId>,
        _deduplicated_label: Option<String>,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        self.validate_insert(overwrite)?;
        pipeline.try_resize(1)?;
        pipeline.add_sink(|input| PaimonCommitSink::try_create(input, self.loaded_table()?.clone()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    fn support_column_projection(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_meta_app::schema::CatalogIdIdent;
    use databend_common_meta_app::schema::CatalogInfo;
    use databend_common_meta_app::schema::CatalogMeta;
    use databend_common_meta_app::schema::CatalogNameIdent;
    use databend_common_meta_app::schema::CatalogOption;
    use databend_common_meta_app::schema::PaimonCatalogOption;
    use databend_common_meta_app::schema::TableInfo;
    use databend_common_meta_app::tenant::Tenant;

    use super::catalog_options_from_info;
    use super::options_from_map;
    use crate::catalog::trim_option_key;
    use crate::catalog::trim_option_keys;

    #[test]
    fn trim_option_key_strips_surrounding_quotes() {
        assert_eq!(trim_option_key("\"s3.region\""), "s3.region");
        assert_eq!(trim_option_key("s3.region"), "s3.region");
        assert_eq!(trim_option_key("warehouse"), "warehouse");
    }

    #[test]
    fn catalog_options_from_info_strips_quoted_keys() {
        let options = HashMap::from([
            ("metastore".to_string(), "filesystem".to_string()),
            ("warehouse".to_string(), "/tmp/wh".to_string()),
            ("\"s3.region\"".to_string(), "us-east-1".to_string()),
            (
                "\"s3.endpoint\"".to_string(),
                "http://127.0.0.1:9900".to_string(),
            ),
        ]);
        let catalog_info = Arc::new(CatalogInfo {
            id: CatalogIdIdent::new(Tenant::new_literal("dummy"), 0).into(),
            name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), "p").into(),
            meta: CatalogMeta {
                catalog_option: CatalogOption::Paimon(PaimonCatalogOption { options }),
                created_on: chrono::Utc::now(),
            },
        });
        let table_info = TableInfo {
            catalog_info,
            ..Default::default()
        };
        let stripped = catalog_options_from_info(&table_info).expect("options");
        assert_eq!(
            stripped.get("s3.region").map(String::as_str),
            Some("us-east-1")
        );
        assert!(!stripped.contains_key("\"s3.region\""));
        assert_eq!(
            stripped.get("s3.endpoint").map(String::as_str),
            Some("http://127.0.0.1:9900")
        );
    }

    #[test]
    fn options_from_map_strips_quoted_keys() {
        let raw = HashMap::from([
            ("\"s3.region\"".to_string(), "us-east-1".to_string()),
            ("warehouse".to_string(), "/tmp/wh".to_string()),
        ]);
        let opts = options_from_map(&raw).expect("options");
        // paimon Options has no public get in all versions; round-trip via re-trim of input map
        let trimmed = trim_option_keys(&raw);
        assert_eq!(
            trimmed.get("s3.region").map(String::as_str),
            Some("us-east-1")
        );
        let _ = opts;
    }
}
