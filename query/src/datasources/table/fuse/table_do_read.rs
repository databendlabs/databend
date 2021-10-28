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

use common_context::IOContext;
use common_context::TableIOContext;
use common_exception::Result;
use common_planners::Extras;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use super::io;
use crate::datasources::table::fuse::FuseTable;
use crate::sessions::DatabendQueryContext;

impl FuseTable {
    #[inline]
    pub async fn do_read(
        &self,
        io_ctx: Arc<TableIOContext>,
        push_downs: &Option<Extras>,
    ) -> Result<SendableDataBlockStream> {
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");

        let default_proj = || {
            (0..self.table_info.schema().fields().len())
                .into_iter()
                .collect::<Vec<usize>>()
        };

        let projection = if let Some(push_down) = push_downs {
            if let Some(prj) = &push_down.projection {
                prj.clone()
            } else {
                default_proj()
            }
        } else {
            default_proj()
        };

        // TODO we need a configuration to specify the unit of dequeue operation
        let bite_size = 1;
        let iter = {
            std::iter::from_fn(move || match ctx.clone().try_get_partitions(bite_size) {
                Err(_) => None,
                Ok(parts) if parts.is_empty() => None,
                Ok(parts) => Some(parts),
            })
            .flatten()
        };
        let da = io_ctx.get_data_accessor()?;
        let arrow_schema = self.table_info.schema().to_arrow();

        let stream = futures::stream::iter(iter);
        let stream = stream.then(move |part| {
            io::do_read(part, da.clone(), projection.clone(), arrow_schema.clone())
        });
        Ok(Box::pin(stream))
    }
}
