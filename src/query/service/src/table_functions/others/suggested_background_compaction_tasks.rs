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

use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::DataBlock;
use databend_common_meta_app::schema::TableStatistics;
use databend_enterprise_background_service::Suggestion;
use log::info;

use super::suggested_background_tasks::SuggestedBackgroundTasksSource;
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

    AND ((t.num_rows >  1 * 1000 * 100 AND t.number_of_blocks > 500 AND t.data_size / t.number_of_blocks < 50 * 1024 * 1024) OR (t.number_of_blocks > 500 AND t.number_of_blocks / t.number_of_segments < 500))
    AND NOT EXISTS (
      SELECT
          1
        FROM
          system.background_tasks AS processed
        WHERE
          processed.state = 'DONE'
          AND processed.table_id = t.table_id
          AND processed.type = 'COMPACTION'
          AND TO_UNIX_TIMESTAMP(t.updated_on) < TO_UNIX_TIMESTAMP(processed.updated_on)
    )
    ;
";

impl SuggestedBackgroundTasksSource {
    pub async fn get_suggested_compaction_tasks(ctx: Arc<QueryContext>) -> Result<Vec<Suggestion>> {
        let resps = Self::do_get_all_suggested_compaction_tables(ctx).await?;
        let mut suggestions = vec![];
        for records in resps {
            info!(records :? =(&records); "target_tables");

            let records = records.consume_convert_to_full();
            let db_names =
                StringType::try_downcast_column(records.columns()[0].value.as_column().unwrap())
                    .unwrap();

            let db_ids =
                UInt64Type::try_downcast_column(records.columns()[1].value.as_column().unwrap())
                    .unwrap();

            let tb_names =
                StringType::try_downcast_column(records.columns()[2].value.as_column().unwrap())
                    .unwrap();

            let tb_ids =
                UInt64Type::try_downcast_column(records.columns()[3].value.as_column().unwrap())
                    .unwrap();

            let segment_advice =
                BooleanType::try_downcast_column(records.columns()[4].value.as_column().unwrap())
                    .unwrap();

            let block_advice =
                BooleanType::try_downcast_column(records.columns()[5].value.as_column().unwrap())
                    .unwrap();

            let row_count =
                UInt64Type::try_downcast_column(records.columns()[6].value.as_column().unwrap())
                    .unwrap();

            let bytes_uncompressed =
                UInt64Type::try_downcast_column(records.columns()[7].value.as_column().unwrap())
                    .unwrap();

            let bytes_compressed =
                UInt64Type::try_downcast_column(records.columns()[8].value.as_column().unwrap())
                    .unwrap();

            let index_size =
                UInt64Type::try_downcast_column(records.columns()[9].value.as_column().unwrap())
                    .unwrap();

            let segment_count =
                UInt64Type::try_downcast_column(records.columns()[10].value.as_column().unwrap())
                    .unwrap();

            let block_count =
                UInt64Type::try_downcast_column(records.columns()[11].value.as_column().unwrap())
                    .unwrap();

            for i in 0..records.num_rows() {
                let db_name: String = db_names.index(i).unwrap().to_string();
                let db_id = db_ids[i];
                let table_name = tb_names.index(i).unwrap().to_string();
                let table_id = tb_ids[i];
                let need_compact_segment = segment_advice.get_bit(i);
                let need_compact_block = block_advice.get_bit(i);
                let number_of_rows = row_count[i];
                let data_bytes = bytes_uncompressed[i];
                let compressed_data_bytes = bytes_compressed[i];
                let index_data_bytes = index_size[i];
                let number_of_segments = segment_count[i];
                let number_of_blocks = block_count[i];
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
                        index_data_bytes,
                    },
                };
                suggestions.push(suggestion);
            }
        }
        Ok(suggestions)
    }

    pub async fn do_get_all_suggested_compaction_tables(
        ctx: Arc<QueryContext>,
    ) -> Result<Vec<DataBlock>> {
        let res = SuggestedBackgroundTasksSource::do_execute_sql(
            ctx,
            SUGGEST_TABLES_NEED_COMPACTION.to_string(),
        )
        .await?;
        let num_of_tables = res.as_ref().map_or_else(|| 0, |r| r.num_rows());
        info!(
            job = "compaction",
            background = true,
            tables = num_of_tables,
            sql = SUGGEST_TABLES_NEED_COMPACTION;
            "get all suggested tables"
        );
        let res = res.map(|r| vec![r]).unwrap_or_else(Vec::new);
        Ok(res)
    }
}
