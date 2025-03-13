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

use std::sync::Arc;
use std::time::Instant;

use async_recursion::async_recursion;
use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_catalog::catalog_kind::CATALOG_HIVE;
use databend_common_catalog::partition_columns::get_pushdown_without_partition_columns;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use databend_common_storage::init_operator;
use databend_common_storage::DataOperator;
use databend_common_storages_parquet::ParquetRSPruner;
use databend_common_storages_parquet::ParquetRSReaderBuilder;
use databend_storages_common_pruner::partition_prunner::PartitionPruner;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::table::ChangeType;
use futures::TryStreamExt;
use log::info;
use log::trace;
use opendal::EntryMode;
use opendal::Operator;

use super::hive_catalog::HiveCatalog;
use super::hive_table_options::HiveTableOptions;
use crate::hive_table_source::HiveTableSource;
use crate::utils::HiveFetchPartitionScalars;
use crate::HivePartInfo;
use crate::HivePartitionFiller;

pub const HIVE_TABLE_ENGINE: &str = "hive";
pub const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

pub struct HiveTable {
    table_info: TableInfo,
    table_options: HiveTableOptions,
    dal: Operator,
}

impl HiveTable {
    pub fn try_create(table_info: TableInfo) -> Result<HiveTable> {
        let table_options = table_info.engine_options().try_into()?;
        let storage_params = table_info.meta.storage_params.clone();
        let dal = match storage_params {
            Some(sp) => init_operator(&sp)?,
            None => DataOperator::instance().operator(),
        };

        Ok(HiveTable {
            table_info,
            table_options,
            dal,
        })
    }

    fn partition_fields(&self) -> Vec<TableField> {
        self.schema()
            .fields()
            .iter()
            .filter(|field| {
                self.table_options
                    .partition_keys
                    .as_ref()
                    .map(|ks| ks.contains(&field.name))
                    .unwrap_or_default()
            })
            .cloned()
            .collect()
    }

    fn no_partition_schema(&self) -> Arc<TableSchema> {
        let non_partition_fields = self
            .schema()
            .fields()
            .iter()
            .filter(|field| {
                !self
                    .table_options
                    .partition_keys
                    .as_ref()
                    .map(|ks| ks.contains(&field.name))
                    .unwrap_or_default()
            })
            .cloned()
            .collect();
        Arc::new(TableSchema::new(non_partition_fields))
    }

    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(parts_len, max_threads);
        let table_schema = self.no_partition_schema();

        let arrow_schema = table_schema.as_ref().into();
        let leaf_fields = Arc::new(table_schema.leaf_fields());

        let mut read_options = ParquetReadOptions::default();

        if !ctx.get_settings().get_enable_parquet_page_index()? {
            read_options = read_options.with_prune_pages(false);
        }

        if !ctx.get_settings().get_enable_parquet_rowgroup_pruning()? {
            read_options = read_options.with_prune_row_groups(false);
        }

        if !ctx.get_settings().get_enable_parquet_prewhere()? {
            read_options = read_options.with_do_prewhere(false);
        }

        let pruner = ParquetRSPruner::try_create(
            ctx.get_function_context()?,
            table_schema.clone(),
            leaf_fields,
            &plan.push_downs,
            read_options,
            self.table_options
                .partition_keys
                .clone()
                .unwrap_or_default(),
        )?;

        let op = self.dal.clone();

        let partition_keys = self
            .table_options
            .partition_keys
            .clone()
            .unwrap_or_default();

        let partition_field_indexes: Result<Vec<FieldIndex>> = partition_keys
            .iter()
            .map(|name| self.schema().index_of(name))
            .collect();
        let partition_field_indexes = partition_field_indexes?;
        let push_downs = if let Some(ref p) = plan.push_downs {
            Some(get_pushdown_without_partition_columns(
                p.clone(),
                &partition_field_indexes[..],
            )?)
        } else {
            None
        };
        let mut builder =
            ParquetRSReaderBuilder::create(ctx.clone(), op, table_schema, arrow_schema)?
                .with_options(read_options)
                .with_push_downs(push_downs.as_ref())
                .with_pruner(Some(pruner))
                .with_partition_columns(partition_keys);

        let parquet_reader = Arc::new(builder.build_full_reader(false)?);

        let output_schema = Arc::new(DataSchema::from(plan.schema()));
        pipeline.add_source(
            |output| {
                HiveTableSource::create(
                    ctx.clone(),
                    output,
                    output_schema.clone(),
                    parquet_reader.clone(),
                    self.partition_fields(),
                )
            },
            max_threads.max(1),
        )
    }

    fn get_column_schemas(&self, columns: Vec<String>) -> Result<Arc<TableSchema>> {
        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            let schema = self.table_info.schema();
            let data_field = schema.field_with_name(&column)?;
            fields.push(data_field.clone());
        }

        Ok(Arc::new(TableSchema::new(fields)))
    }

    #[async_backtrace::framed]
    async fn get_query_locations_from_partition_table(
        &self,
        ctx: Arc<dyn TableContext>,
        partition_keys: Vec<String>,
        filter_expression: Option<Expr<String>>,
    ) -> Result<Vec<(String, Option<String>)>> {
        let hive_catalog = ctx.get_catalog(CATALOG_HIVE).await?;
        let hive_catalog = hive_catalog.as_any().downcast_ref::<HiveCatalog>().unwrap();

        // todo may use get_partition_names_ps to filter
        let table_info = self.table_info.desc.split('.').collect::<Vec<&str>>();
        let mut partition_names = hive_catalog
            .get_partition_names(table_info[0].to_string(), table_info[1].to_string(), -1)
            .await?;

        let partition_num = partition_names.len();
        if partition_num < 100000 {
            trace!(
                "get {} partitions from hive metastore:{:?}",
                partition_num,
                partition_names
            );
        } else {
            trace!("get {} partitions from hive metastore", partition_num);
        }

        if let Some(expr) = filter_expression {
            let partition_schemas = self.get_column_schemas(partition_keys.clone())?;
            let partition_pruner = PartitionPruner::try_create(
                ctx.get_function_context()?,
                expr,
                partition_schemas,
                self.table_info.schema(),
            )?;
            partition_names =
                partition_pruner.prune::<String, HiveFetchPartitionScalars>(partition_names)?;
        }

        trace!(
            "after partition prune, {} partitions:{:?}",
            partition_names.len(),
            partition_names
        );

        let partitions = hive_catalog
            .get_partitions(
                table_info[0].to_string(),
                table_info[1].to_string(),
                partition_names.clone(),
            )
            .await?;
        let res = partitions
            .into_iter()
            .map(|p| convert_hdfs_path(&p.sd.unwrap().location.unwrap(), true))
            .zip(partition_names.into_iter().map(Some))
            .collect::<Vec<_>>();
        Ok(res)
    }

    // return items: (hdfs_location, option<part info>) where part info likes 'c_region=Asia/c_nation=China'
    #[async_backtrace::framed]
    async fn get_query_locations(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: &Option<PushDownInfo>,
    ) -> Result<Vec<(String, Option<String>)>> {
        let path = self.table_options.location.as_ref().ok_or_else(|| {
            ErrorCode::TableInfoError(format!("{}, table location is empty", self.table_info.name))
        })?;

        if let Some(partition_keys) = &self.table_options.partition_keys {
            if !partition_keys.is_empty() {
                let filter_expression = push_downs.as_ref().and_then(|p| {
                    p.filters
                        .as_ref()
                        .map(|filter| filter.filter.as_expr(&BUILTIN_FUNCTIONS))
                });

                return self
                    .get_query_locations_from_partition_table(
                        ctx.clone(),
                        partition_keys.clone(),
                        filter_expression,
                    )
                    .await;
            }
        }

        let location = convert_hdfs_path(path, true);
        Ok(vec![(location, None)])
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn list_files_from_dirs(
        &self,
        dirs: Vec<(String, Option<String>)>,
    ) -> Result<Vec<HivePartInfo>> {
        let sem = Arc::new(Semaphore::new(60));

        let mut tasks = Vec::with_capacity(dirs.len());
        for (dir, partition) in dirs {
            let sem_t = sem.clone();
            let operator_t = self.dal.clone();
            let dir_t = dir.to_string();
            let task = databend_common_base::runtime::spawn(async move {
                list_files_from_dir(operator_t, dir_t, sem_t).await
            });
            tasks.push((task, partition));
        }

        let mut all_files = vec![];
        for (task, _) in tasks {
            let files = task.await.unwrap()?;
            all_files.extend_from_slice(&files);
        }

        Ok(all_files)
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let start = Instant::now();
        let dirs = self.get_query_locations(ctx.clone(), &push_downs).await?;
        trace!("{} query locations: {:?}", dirs.len(), dirs);

        let dir_len = dirs.len();
        let filler = HivePartitionFiller::create(self.partition_fields());
        let mut partitions = self.list_files_from_dirs(dirs).await?;
        for partition in partitions.iter_mut() {
            partition.partitions = filler.extract_scalars(&partition.filename)?;
        }

        trace!("{} hive files: {:?}", partitions.len(), partitions);

        info!(
            "read partition, partition num:{}, elapsed:{:?}",
            partitions.len(),
            start.elapsed()
        );

        let estimated_read_rows: f64 = partitions
            .iter()
            .map(|s| s.filesize as f64 / (self.schema().num_fields() * 8) as f64)
            .sum();

        let read_bytes = partitions.iter().map(|s| s.filesize as usize).sum();
        let stats = PartStatistics::new_estimated(
            None,
            estimated_read_rows as _,
            read_bytes,
            partitions.len(),
            dir_len,
        );
        let partitions = partitions
            .into_iter()
            .map(HivePartInfo::into_part_ptr)
            .collect();

        Ok((
            stats,
            Partitions::create(PartitionsShuffleKind::Seq, partitions),
        ))
    }
}

#[async_trait::async_trait]
impl Table for HiveTable {
    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Cluster
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        todo!()
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn has_exact_total_row_count(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs).await
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
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

    fn commit_insertion(
        &self,
        _ctx: Arc<dyn TableContext>,
        _pipeline: &mut Pipeline,
        _copied_files: Option<UpsertTableCopiedFileReq>,
        _update_stream_meta: Vec<UpdateStreamMetaReq>,
        _overwrite: bool,
        _prev_snapshot_id: Option<SnapshotId>,
        _deduplicated_label: Option<String>,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(format!(
            "commit_insertion operation for table {} is not implemented, table engine is {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    #[async_backtrace::framed]
    async fn truncate(&self, _ctx: Arc<dyn TableContext>, _pipeline: &mut Pipeline) -> Result<()> {
        Err(ErrorCode::Unimplemented(format!(
            "truncate for table {} is not implemented",
            self.name()
        )))
    }

    #[async_backtrace::framed]
    async fn purge(
        &self,
        _ctx: Arc<dyn TableContext>,
        _instant: Option<NavigationPoint>,
        _limit: Option<usize>,
        _keep_last_snapshot: bool,
        _dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        Ok(None)
    }

    async fn table_statistics(
        &self,
        _ctx: Arc<dyn TableContext>,
        _require_fresh: bool,
        _change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        Ok(None)
    }

    fn support_prewhere(&self) -> bool {
        true
    }
}

// Dummy Impl
struct HiveSource {
    finish: bool,
    schema: DataSchemaRef,
}

impl HiveSource {
    #[allow(dead_code)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, HiveSource {
            finish: false,
            schema,
        })
    }
}

impl SyncSource for HiveSource {
    const NAME: &'static str = "HiveSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        Ok(Some(DataBlock::empty_with_schema(self.schema.clone())))
    }
}

// convert hdfs path format to opendal path formatted
//
// there are two rules:
// 1. erase the schema related info from hdfs path, for example, hdfs://namenode:8020/abc/a is converted to /abc/a
// 2. if the path is dir, append '/' if necessary
// org.apache.hadoop.fs.Path#Path(String pathString) shows how to parse hdfs path
pub fn convert_hdfs_path(hdfs_path: &str, is_dir: bool) -> String {
    let mut start = 0;
    let slash = hdfs_path.find('/');
    let colon = hdfs_path.find(':');
    if let Some(colon) = colon {
        match slash {
            Some(slash) => {
                if colon < slash {
                    start = colon + 1;
                }
            }
            None => {
                start = colon + 1;
            }
        }
    }

    let mut path = &hdfs_path[start..];
    start = 0;
    if path.starts_with("//") && path.len() > 2 {
        path = &path[2..];
        let next_slash = path.find('/');
        start = next_slash.unwrap_or(path.len());
    }
    path = &path[start..];

    let end_with_slash = path.ends_with('/');
    let mut format_path = path.to_string();
    if is_dir && !end_with_slash {
        format_path.push('/')
    }
    format_path
}

#[async_recursion(#[recursive::recursive])]
async fn list_files_from_dir(
    operator: Operator,
    location: String,
    sem: Arc<Semaphore>,
) -> Result<Vec<HivePartInfo>> {
    let (files, dirs) = do_list_files_from_dir(operator.clone(), location, sem.clone()).await?;
    let mut all_files = files;
    let mut tasks = Vec::with_capacity(dirs.len());
    for dir in dirs {
        let sem_t = sem.clone();
        let operator_t = operator.clone();
        let task = databend_common_base::runtime::spawn(async move {
            list_files_from_dir(operator_t, dir, sem_t).await
        });
        tasks.push(task);
    }

    // let dir_files = tasks.map(|task| task.await.unwrap()).flatten().collect::<Vec<_>>();
    // all_files.extend(dir_files);

    for task in tasks {
        let files = task.await.unwrap()?;
        all_files.extend(files);
    }

    Ok(all_files)
}

async fn do_list_files_from_dir(
    operator: Operator,
    location: String,
    sem: Arc<Semaphore>,
) -> Result<(Vec<HivePartInfo>, Vec<String>)> {
    let _a = sem.acquire().await.unwrap();
    let mut m = operator.lister_with(&location).await?;

    let mut all_files = vec![];
    let mut all_dirs = vec![];
    while let Some(de) = m.try_next().await? {
        let meta = de.metadata();

        let path = de.path();
        let file_offset = path.rfind('/').unwrap_or_default() + 1;
        if path[file_offset..].starts_with('.') || path[file_offset..].starts_with('_') {
            continue;
        }
        // Ignore the location itself
        if path.trim_matches('/') == location.trim_matches('/') {
            continue;
        }

        match meta.mode() {
            EntryMode::FILE => {
                let mut length = meta.content_length();
                if length == 0 {
                    length = operator.stat(path).await?.content_length();
                }
                let location = path.to_string();
                all_files.push(HivePartInfo::create(location, vec![], length));
            }
            EntryMode::DIR => {
                all_dirs.push(path.to_string());
            }
            _ => {
                return Err(ErrorCode::ReadTableDataError(format!(
                    "{} couldn't get file mode",
                    path
                )));
            }
        }
    }
    Ok((all_files, all_dirs))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::convert_hdfs_path;

    #[test]
    fn test_convert_hdfs_path() {
        let mut m = HashMap::new();
        m.insert("hdfs://namenode:8020/user/a", "/user/a/");
        m.insert("hdfs://namenode:8020/user/a/", "/user/a/");
        m.insert("hdfs://namenode:8020/", "/");
        m.insert("hdfs://namenode:8020", "/");
        m.insert("/user/a", "/user/a/");
        m.insert("/", "/");

        for (hdfs_path, expected_path) in &m {
            let path = convert_hdfs_path(hdfs_path, true);
            assert_eq!(path, *expected_path);
        }
    }
}
