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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use common_catalog::catalog::CATALOG_DEFAULT;
use common_datablocks::DataBlock;
use common_datavalues::prelude::Series;
use common_datavalues::prelude::SeriesFrom;
use common_exception::Result;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::system::ClusteringHistoryTable;

pub struct InterpreterClusteringHistory {
    ctx: Arc<QueryContext>,
}

impl InterpreterClusteringHistory {
    pub fn create(ctx: Arc<QueryContext>) -> Self {
        InterpreterClusteringHistory { ctx }
    }

    pub async fn write_log(
        &self,
        start: SystemTime,
        db_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let start_time = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_micros() as i64;
        let end_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_micros() as i64;
        let reclustered_bytes = self.ctx.get_scan_progress_value().bytes as u64;
        let reclustered_rows = self.ctx.get_scan_progress_value().rows as u64;

        let table = self
            .ctx
            .get_table(CATALOG_DEFAULT, "system", "clustering_history")
            .await?;
        let schema = table.get_table_info().meta.schema.clone();

        let block = DataBlock::create(schema.clone(), vec![
            Series::from_data(vec![start_time]),
            Series::from_data(vec![end_time]),
            Series::from_data(vec![db_name]),
            Series::from_data(vec![table_name]),
            Series::from_data(vec![reclustered_bytes]),
            Series::from_data(vec![reclustered_rows]),
        ]);
        let blocks = vec![Ok(block)];
        let input_stream = futures::stream::iter::<Vec<Result<DataBlock>>>(blocks);

        let clustering_history_table: &ClusteringHistoryTable =
            table.as_any().downcast_ref().unwrap();
        clustering_history_table
            .append_data(self.ctx.clone(), Box::pin(input_stream))
            .await?;
        Ok(())
    }
}
