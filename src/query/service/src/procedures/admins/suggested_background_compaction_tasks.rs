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
use arrow_array::{LargeBinaryArray, RecordBatch};
use tracing::{debug, info};
use background_service::Suggestion;
use common_arrow::arrow::array::{BooleanArray, UInt64Array};
use common_exception::Result;
use common_meta_app::schema::TableStatistics;
use crate::procedures::admins::suggested_background_tasks::SuggestedBackgroundTasksProcedure;
use crate::sessions::QueryContext;

const SUGGEST_TABLES_NEED_COMPACTION: &str = "
SELECT t.database as database, d.database_id as database_id, t.name as table, t.table_id as table_id,
  IF(
      t.number_of_blocks > 500 AND t.number_of_blocks / t.number_of_segments < 500,
      TRUE,
      FALSE
    ) AS segment_advice,
    IF(
      t.num_rows >  1 * 1000 * 100 AND t.number_of_blocks > 500 AND t.data_size / t.number_of_blocks < 50 * 1024 * 1024,
      TRUE,
      FALSE
    ) AS block_advice,
    t.num_rows as row_count,
    t.data_size as bytes_uncompressed,
    t.data_compressed_size as bytes_compressed,
    t.index_size as index_size,
    t.number_of_segments as segment_count,
    t.number_of_blocks as block_count
FROM system.tables as t
JOIN system.databases as d
ON t.database = d.name
WHERE t.database != 'system'
    AND t.database != 'information_schema'
    AND t.engine = 'FUSE'
    AND t.num_rows > 1 * 1000 * 100
    AND t.data_size IS NOT NULL
    AND t.number_of_segments IS NOT NULL
    AND t.number_of_blocks IS NOT NULL
    AND t.number_of_segments > 1
    AND t.number_of_blocks > 500
    AND t.data_size > 1

    AND t.number_of_blocks / t.number_of_segments < 500 OR t.data_size / t.number_of_blocks < 50 * 1024 * 1024
    AND t.table_id NOT IN (
        SELECT
          table_id
        FROM
          system.background_tasks
        WHERE
          state = 'DONE'
          AND table_id = t.table_id
          AND type = 'COMPACTION'
          AND updated_on > t.updated_on
    )
    ;
";

impl SuggestedBackgroundTasksProcedure {
    pub async fn get_suggested_compaction_tasks(ctx: Arc<QueryContext>) -> Result<Vec<Suggestion>> {
        let resps = Self::do_get_all_suggested_compaction_tables(
            ctx,
        ).await?;
        let mut suggestions = vec![];
        for records in resps {
            debug!(?records, "target_tables");
            let db_names = records
                .column(0)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            let db_ids = records
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let tb_names = records
                .column(2)
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            let tb_ids = records
                .column(3)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let segment_advice = records
                .column(4)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let block_advice = records
                .column(5)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let row_count = records
                .column(6)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let bytes_uncompressed = records
                .column(7)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let bytes_compressed = records
                .column(8)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let index_size = records
                .column(9)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let segment_count = records
                .column(10)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let block_count = records
                .column(11)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            for i in 0..records.num_rows() {
                let db_name : String = String::from_utf8_lossy(db_names.value(i).to_vec().as_slice()).to_string();
                let db_id = db_ids.value(i);
                let table_name = String::from_utf8_lossy(tb_names.value(i).to_vec().as_slice()).to_string();
                let table_id = tb_ids.value(i);
                let need_compact_segment = segment_advice.value(i);
                let need_compact_block = block_advice.value(i);
                let number_of_rows = row_count.value(i);
                let data_bytes = bytes_uncompressed.value(i);
                let compressed_data_bytes = bytes_compressed.value(i);
                let index_data_bytes = index_size.value(i);
                let number_of_segments = segment_count.value(i);
                let number_of_blocks = block_count.value(i);
                let suggestion = Suggestion::Compaction {
                    db_id,
                    db_name,
                    table_id,
                    table_name,
                    need_compact_segment,
                    need_compact_block,
                    table_stats: TableStatistics {
                        number_of_rows,
                        data_bytes,
                        compressed_data_bytes,
                        number_of_segments: Some(number_of_segments),
                        number_of_blocks: Some(number_of_blocks),
                        index_data_bytes
                    },
                };
                suggestions.push(suggestion);
            }
        }
        Ok(suggestions)
    }

    pub async fn do_get_all_suggested_compaction_tables(
        ctx: Arc<QueryContext>
    ) -> Result<Vec<RecordBatch>> {
        let res = RealBackgroundService::do_execute_sql(ctx, SUGGEST_TABLES_NEED_COMPACTION.to_string()).await?;
        let num_of_tables = res.as_ref().map_or_else(|| 0, |r| r.num_rows());
        info!(
            job = "compaction",
            background = true,
            tables = num_of_tables,
            "get all suggested tables"
        );
        let res = res.map(|r| vec![r]).unwrap_or_else(|| vec![]);
        Ok(res)
    }

}