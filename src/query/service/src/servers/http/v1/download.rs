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

use async_stream::stream;
use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_formats::ClickhouseFormatType;
use common_formats::FileFormatOptionsExt;
use common_storages_fuse_result::ResultTable;
use futures::StreamExt;

use crate::sessions::QueryContext;
use crate::storages::Table;
use crate::stream::ReadDataBlockStream;

pub type SendableVu8Stream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Vec<u8>>> + Send>>;

#[async_trait::async_trait]
pub trait Downloader {
    async fn download(
        &self,
        ctx: Arc<QueryContext>,
        format: ClickhouseFormatType,
        limit: Option<usize>,
    ) -> Result<SendableVu8Stream>;
}

#[async_trait::async_trait]
impl Downloader for ResultTable {
    async fn download(
        &self,
        ctx: Arc<QueryContext>,
        format: ClickhouseFormatType,
        limit: Option<usize>,
    ) -> Result<SendableVu8Stream> {
        let push_downs = match limit {
            Some(limit) if limit > 0 => Some(PushDownInfo {
                limit: Some(limit),
                ..PushDownInfo::default()
            }),
            _ => None,
        };

        let (_, parts) = self
            .read_partitions(ctx.clone(), push_downs.clone())
            .await?;
        ctx.try_set_partitions(parts)?;
        let mut block_stream = self
            .read_data_block_stream(ctx.clone(), &DataSourcePlan {
                catalog: "".to_string(),
                source_info: DataSourceInfo::TableSource(Default::default()),
                scan_fields: None,
                parts: Default::default(),
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs,
            })
            .await?;
        let mut output_format = FileFormatOptionsExt::get_output_format_from_settings_clickhouse(
            format,
            self.schema(),
            &ctx.get_settings(),
        )?;

        let prefix = Ok(output_format.serialize_prefix()?);
        let stream = stream! {
            yield prefix;
            while let Some(block) = block_stream.next().await {
                match block{
                    Ok(block) => {
                        yield output_format.serialize_block(&block);
                    },
                    Err(err) => yield(Err(err)),
                };
            };
            yield output_format.finalize();
        };
        Ok(Box::pin(stream))
    }
}
