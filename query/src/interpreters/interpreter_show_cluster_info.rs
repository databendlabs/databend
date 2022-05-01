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
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ShowClusterInfoPlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::FuseTable;

pub struct ShowClusterInfoInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowClusterInfoPlan,
}

impl ShowClusterInfoInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowClusterInfoPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowClusterInfoInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowClusterInfoInterpreter {
    fn name(&self) -> &str {
        "ShowClusterInfoInterpreter"
    }

    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let database = self.plan.db.as_str();
        let table = self.plan.table.as_str();
        let table = self.ctx.get_table(database, table).await?;
        //let schema = table.schema();
        let table = table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::UnsupportedEngineParams(format!(
                "expecting fuse table, but got table of engine type: {}",
                table.get_table_info().meta.engine
            ))
        })?;

        let cluster_keys = match self.plan.cluster_keys.clone() {
            Some(keys) => keys,
            None => {
                if table.order_keys.is_empty() {
                    return Err(ErrorCode::BadArguments("expecting table with cluster keys"));
                }
                table.order_keys.clone()
            }
        };

        let cluster_keys_str = cluster_keys
            .iter()
            .map(|expr| expr.column_name())
            .collect::<Vec<_>>()
            .join(", ");

        let snapshot = table.read_table_snapshot(self.ctx.as_ref()).await?;

        let total_block_count = if let Some(snapshot) = snapshot {
            let reader = MetaReaders::segment_info_reader(self.ctx.as_ref());
            let mut blocks = Vec::new();
            for (x, ver) in &snapshot.segments {
                let res = reader.read(x, None, *ver).await?;
                let mut block = res.blocks.clone();
                blocks.append(&mut block);
            }
            blocks.len() as u64
        } else {
            0u64
        };

        let desc_schema = self.plan.schema();

        let block = DataBlock::create(desc_schema.clone(), vec![
            Series::from_data(vec![cluster_keys_str]),
            Series::from_data(vec![total_block_count]),
        ]);

        Ok(Box::pin(DataBlockStream::create(desc_schema, None, vec![
            block,
        ])))
    }
}
