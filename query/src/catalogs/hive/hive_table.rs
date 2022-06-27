// Copyright 2022 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing_futures::Instrument;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::ObjectMode;

use super::hive_table_options::HiveTableOptions;
use crate::catalogs::hive::HivePartInfo;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::SyncSource;
use crate::pipelines::new::processors::SyncSourcer;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::storages::hive::HiveParquetBlockReader;
use crate::storages::Table;
use crate::storages::TableStatistics;

///! Dummy implementation for HIVE TABLE

pub const HIVE_TABLE_ENGIE: &str = "hive";

pub struct HiveTable {
    table_info: TableInfo,
    table_options: HiveTableOptions,
}

impl HiveTable {
    pub fn try_create(table_info: TableInfo) -> Result<HiveTable> {
        let table_options = table_info.engine_options().try_into()?;
        Ok(HiveTable {
            table_info,
            table_options,
        })
    }

    #[inline]
    pub fn do_read(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        let block_reader = self.create_block_reader(&ctx, push_downs)?;

        let iter = std::iter::from_fn(move || match ctx.clone().try_get_partitions(1) {
            Err(_) => None,
            Ok(parts) if parts.is_empty() => None,
            Ok(parts) => Some(parts),
        })
        .flatten();

        let part_stream = futures::stream::iter(iter);

        let stream = part_stream
            .then(move |part| {
                let block_reader = block_reader.clone();
                async move { block_reader.read(part).await }
            })
            .instrument(common_tracing::tracing::Span::current());

        Ok(Box::pin(stream))
    }

    fn create_block_reader(
        &self,
        ctx: &Arc<QueryContext>,
        push_downs: &Option<Extras>,
    ) -> Result<Arc<HiveParquetBlockReader>> {
        let projection = if let Some(Extras {
            projection: Some(prj),
            ..
        }) = push_downs
        {
            prj.clone()
        } else {
            (0..self.table_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        let operator = ctx.get_storage_operator()?;
        let table_schema = self.table_info.schema();
        // todo, support csv, orc format
        HiveParquetBlockReader::create(operator, table_schema, projection)
    }

    async fn do_read_partitions(
        &self,
        ctx: Arc<QueryContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        if let Some(partition_keys) = &self.table_options.partition_keys {
            if !partition_keys.is_empty() {
                return Err(ErrorCode::UnImplement(format!(
                    "{}, not suport query for partitioned hive table, partitions:{:?}",
                    self.table_info.name, self.table_options.partition_keys,
                )));
            }
        }
        let path = match &self.table_options.location {
            Some(path) => path,
            None => {
                return Err(ErrorCode::TableInfoError(format!(
                    "{}, table location is empty",
                    self.table_info.name
                )))
            }
        };
        let location = convert_hdfs_path(path, true);

        let operator = ctx.get_storage_operator()?;
        let object = operator.object(&location);
        let mut m = object.list().await?;

        // todo:  use rowgroup level partition
        let mut partitions = vec![];
        while let Some(de) = m.try_next().await? {
            let path = de.path();
            match de.mode() {
                ObjectMode::FILE => {
                    // skip hidden files
                    if !(path.starts_with('.') || path.starts_with('_')) {
                        partitions.push(HivePartInfo::create(path.to_string()));
                    }
                }
                // todo: read data from dirs recursively
                ObjectMode::DIR => {
                    return Err(ErrorCode::UnImplement(format!(
                        "not suport to read data from dir {}",
                        path
                    )));
                }
                _ => {
                    return Err(ErrorCode::ReadTableDataError(format!(
                        "{} couldn't get file mode",
                        path
                    )));
                }
            }
        }

        Ok((Default::default(), partitions))
    }
}

#[async_trait::async_trait]
impl Table for HiveTable {
    fn is_local(&self) -> bool {
        false
    }

    fn as_any(&self) -> &(dyn std::any::Any + 'static) {
        todo!()
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn benefit_column_prune(&self) -> bool {
        true
    }

    fn has_exact_total_row_count(&self) -> bool {
        false
    }

    async fn read_partitions(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs).await
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        None
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        return self.do_read(ctx, &plan.push_downs);
    }

    fn read2(
        &self,
        ctx: Arc<QueryContext>,
        _: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let output = OutputPort::create();
        let schema = self.table_info.schema();
        pipeline.add_pipe(NewPipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output.clone()],
            processors: vec![HiveSource::create(ctx, output, schema)?],
        });

        Ok(())
    }

    async fn append_data(
        &self,
        _ctx: Arc<QueryContext>,
        _stream: SendableDataBlockStream,
    ) -> Result<SendableDataBlockStream> {
        Err(ErrorCode::UnImplement(format!(
            "append operation for table {} is not implemented, table engine is {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    async fn commit_insertion(
        &self,
        _ctx: Arc<QueryContext>,
        _catalog_name: &str,
        _operations: Vec<DataBlock>,
        _overwrite: bool,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "commit_insertion operation for table {} is not implemented, table engine is {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    async fn truncate(
        &self,
        _ctx: Arc<QueryContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(format!(
            "truncate for table {} is not implemented",
            self.name()
        )))
    }

    async fn optimize(&self, _ctx: Arc<QueryContext>, _keep_last_snapshot: bool) -> Result<()> {
        Ok(())
    }

    async fn statistics(&self, _ctx: Arc<QueryContext>) -> Result<Option<TableStatistics>> {
        Ok(None)
    }
}

// Dummy Impl
struct HiveSource {
    finish: bool,
    schema: DataSchemaRef,
}

impl HiveSource {
    pub fn create(
        ctx: Arc<QueryContext>,
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

// convert hdfs path format to opendal path formated
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
        start = match next_slash {
            Some(slash) => slash,
            None => path.len(),
        };
    }
    path = &path[start..];

    let end_with_slash = path.ends_with('/');
    let mut format_path = path.to_string();
    if is_dir && !end_with_slash {
        format_path.push('/')
    }
    format_path
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
            let path = convert_hdfs_path(*hdfs_path, true);
            assert_eq!(path, *expected_path);
        }
    }
}
