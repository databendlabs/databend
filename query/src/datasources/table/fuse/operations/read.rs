//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use async_stream::stream;
use common_datavalues::DataSchema;
use common_exception::Result;
use common_planners::Extras;
use common_streams::ParquetSource;
use common_streams::SendableDataBlockStream;
use common_streams::Source;
use futures::StreamExt;

use crate::datasources::table::fuse::FuseTable;
use crate::sessions::QueryContext;

impl FuseTable {
    #[inline]
    pub async fn do_read(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        let default_proj = || {
            (0..self.table_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        let projection = if let Some(Extras {
            projection: Some(prj),
            ..
        }) = push_downs
        {
            prj.clone()
        } else {
            default_proj()
        };

        // TODO we need a configuration to specify the unit of dequeue operation
        let bite_size = 1;
        let ctx_clone = ctx.clone();
        let iter =
            std::iter::from_fn(
                move || match ctx_clone.clone().try_get_partitions(bite_size) {
                    Err(_) => None,
                    Ok(parts) if parts.is_empty() => None,
                    Ok(parts) => Some(parts),
                },
            )
            .flatten();
        let da = ctx.get_data_accessor()?;
        let arrow_schema = self.table_info.schema().to_arrow();
        let table_schema = Arc::new(DataSchema::from(arrow_schema));

        let mut iter = futures::stream::iter(iter);
        let stream = stream! {
            while let Some(part) = iter.next().await {
                let mut source = ParquetSource::new(
                    da.clone(),
                    part.name.clone(),
                    table_schema.clone(),
                    projection.clone(),
                );
                loop {
                    let block = source.read().await;
                    match block {
                        Ok(None) => break,
                        Ok(Some(b)) =>  yield(Ok(b)),
                        Err(e) => yield(Err(e)),
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
}
