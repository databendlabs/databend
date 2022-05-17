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
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use futures::StreamExt;

use crate::formats::output_format::OutputFormat;
use crate::formats::output_format::OutputFormatType;
use crate::sessions::QueryContext;
use crate::storages::result::ResultTable;
use crate::storages::Table;

pub type SendableVu8Stream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Vec<u8>>> + Send>>;

impl ResultTable {
    pub async fn download(
        &self,
        ctx: Arc<QueryContext>,
        fmt: OutputFormatType,
    ) -> Result<SendableVu8Stream> {
        let (_, parts) = self.read_partitions(ctx.clone(), None).await?;
        ctx.try_set_partitions(parts)?;
        let mut block_stream = self
            .read(ctx.clone(), &ReadDataSourcePlan {
                catalog: "".to_string(),
                source_info: SourceInfo::TableSource(Default::default()),
                scan_fields: None,
                parts: Default::default(),
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs: None,
            })
            .await?;
        let fmt_setting = ctx.get_format_settings()?;
        let mut output_format = fmt.with_default_setting();

        let stream = stream! {
            while let Some(block) = block_stream.next().await {
                match block{
                    Ok(block) => {
                        yield output_format.serialize_block(&block, &fmt_setting);
                    },
                    Err(err) => yield(Err(err)),
                };
            };
            yield output_format.finalize();
        };
        Ok(Box::pin(stream))
    }
}
