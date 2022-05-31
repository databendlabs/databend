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

use std::any::Any;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_types::UserIdentity;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing_futures::Instrument;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;

use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::BlockReader;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::FuseTable;
use crate::storages::result::result_locations::ResultLocations;
use crate::storages::Table;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "t", content = "c")]
pub enum ResultStorageInfo {
    FuseSegment(SegmentInfo),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResultQueryInfo {
    pub query_id: String,
    pub schema: DataSchemaRef,
    pub user: UserIdentity,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResultTableMeta {
    pub query: ResultQueryInfo,
    pub storage: ResultStorageInfo,
}

impl ResultTableMeta {
    fn gen_table_info(&self) -> TableInfo {
        let name = format!("result_{}", self.query.query_id);
        TableInfo {
            desc: name.to_string(),
            name,
            meta: TableMeta {
                schema: self.query.schema.clone(),
                ..Default::default()
            },
            ..Default::default()
        }
    }
}

pub struct ResultTable {
    #[allow(dead_code)]
    query_id: String,
    #[allow(dead_code)]
    meta: ResultTableMeta,
    pub(crate) locations: ResultLocations,
    table_info: TableInfo,
}

impl ResultTable {
    pub async fn try_get(ctx: Arc<QueryContext>, query_id: &str) -> Result<Arc<ResultTable>> {
        let locations = ResultLocations::new(query_id);
        let data_accessor = ctx.get_storage_operator()?;
        let location = locations.get_meta_location();
        let obj = data_accessor.object(&location);
        let data = match obj.read().await {
            Ok(d) => Ok(d),
            Err(e) => {
                if !obj.is_exist().await? {
                    Err(ErrorCode::HttpNotFound(format!(
                        "result for query_id {} not exists",
                        &query_id
                    )))
                } else {
                    Err(ErrorCode::from_std_error(e))
                }
            }
        }?;
        let meta: ResultTableMeta = serde_json::from_slice(&data)?;
        let table_info = meta.gen_table_info();

        Ok(Arc::new(Self {
            query_id: query_id.to_string(),
            meta,
            locations,
            table_info,
        }))
    }

    fn create_block_reader(
        &self,
        ctx: &Arc<QueryContext>,
        _push_downs: &Option<Extras>,
    ) -> Result<Arc<BlockReader>> {
        let projection = (0..self.get_table_info().schema().fields().len())
            .into_iter()
            .collect::<Vec<usize>>();

        let operator = ctx.get_storage_operator()?;
        let table_schema = self.get_table_info().schema();
        BlockReader::create(operator, table_schema, projection)
    }
}

#[async_trait::async_trait]
impl Table for ResultTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        ctx: Arc<QueryContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let data_accessor = ctx.get_storage_operator()?;
        let meta_location = self.locations.get_meta_location();
        let meta_data = data_accessor.object(&meta_location).read().await?;
        let meta: ResultTableMeta = serde_json::from_slice(&meta_data)?;
        match meta.storage {
            ResultStorageInfo::FuseSegment(seg) => {
                Ok(FuseTable::all_columns_partitions(&seg.blocks, usize::MAX))
            }
        }
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let block_reader = self.create_block_reader(&ctx, &None)?;
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

    // todo: support
    fn read2(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
        _pipeline: &mut NewPipeline,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(
            "result table not support read2() yet!",
        ))
    }
}
