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

use std::str::FromStr;
use std::sync::Arc;

use async_stream::stream;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use futures::StreamExt;

use crate::servers::http::formats::tsv_output::block_to_tsv;
use crate::sessions::QueryContext;
use crate::storages::result::ResultTable;
use crate::storages::Table;

pub type SendableVu8Stream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<Vec<u8>>> + Send>>;

#[derive(Clone, Copy)]
pub enum DownloadFormatType {
    Tsv,
}

impl Default for DownloadFormatType {
    fn default() -> Self {
        Self::Tsv
    }
}

impl FromStr for DownloadFormatType {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s.to_uppercase().as_str() {
            "TSV" => Ok(DownloadFormatType::Tsv),
            _ => Err("Unknown file format type, must be one of { TSV }".to_string()),
        }
    }
}

fn serialize_block(block: DataBlock, fmt: DownloadFormatType) -> Result<Vec<u8>> {
    match fmt {
        DownloadFormatType::Tsv => block_to_tsv(&block, &FormatSettings::default()),
    }
}

impl ResultTable {
    pub async fn download(
        &self,
        ctx: Arc<QueryContext>,
        fmt: DownloadFormatType,
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
        let stream = stream! {
            while let Some(block) = block_stream.next().await {
                match block{
                    Ok(block) => {
                        yield(serialize_block(block, fmt))
                    },
                    Err(err) => yield(Err(err)),
                };
            };
        };
        Ok(Box::pin(stream))
    }
}
