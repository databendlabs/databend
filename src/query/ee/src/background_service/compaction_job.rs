// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::format;
use arrow_array::{BooleanArray, RecordBatch, StringArray, UInt64Array};
use tracing::debug;
use tracing::field::debug;
use common_catalog::table::TableStatistics;
use common_config::InnerConfig;
use crate::background_service::configs::JobConfig;
use crate::background_service::job::Job;
use common_exception::Result;
use common_expression::types::{Float32Type, Float64Type};
use common_meta_app::background::CompactionStats;
use crate::background_service::RealBackgroundService;

// TODO(zhihanz) add more configs to filter out tables need to be compacted
const GET_ALL_TARGET_TABLES: &str = "
SELECT t.database, d.database_id, t.name as table, t.table_id
FROM system.tables as t
JOIN system.databases as d
ON t.database = d.name
WHERE t.database != 'system'
    AND t.database != 'information_schema'
    AND t.engine = 'FUSE'
    AND t.num_rows > 0
    AND t.data_compressed_size > 0;
";

// TODO(zhihanz): make the advice threshold configurable
const GET_COMPACT_ADVICE: &str = "
select
IF(segment_count > 10 and block_count / segment_count < 100, TRUE, FALSE) AS segment_advice,
IF(bytes_uncompressed / block_count / 1024 / 1024 < 50, TRUE, FALSE) AS block_advice,
row_count, bytes_uncompressed, bytes_compressed, index_size,
block_count, segment_count,
block_count/segment_count,
humanize_size(bytes_uncompressed / block_count) AS per_block_uncompressed_size_string
from fuse_snapshot('{}', '{}') order by timestamp ASC LIMIT 1;
";

const SEGMENT_COMPACTION_SQL: &str = "
OPTIMIZE TABLE {}.{} COMPACT SEGMENT{};
";

const BLOCK_COMPACTION_SQL: &str = "
OPTIMIZE TABLE {}.{} COMPACT{};
";

#[derive(Clone)]
struct CompactionJob {
    config: JobConfig,
    conf: InnerConfig,
}

#[async_trait]
impl Job for CompactionJob {
    async fn run(&self) {
        self.do_compaction_job().await?;
    }

    fn get_config(&self) -> &JobConfig {
        &self.config
    }
}

//Service
// optimize table limit
// vacuum
impl CompactionJob {
    async fn do_compaction_job(svc: &RealBackgroundService) -> Result<()> {
        Ok(())
    }

    async fn do_get_all_target_tables(svc: &RealBackgroundService) -> Result<RecordBatch> {
        let res = svc.execute_sql(GET_ALL_TARGET_TABLES).await?;
        debug!("type": "compaction", "background": true, "rows": res.num_rows(), "get all target tables");
        Ok(res)
    }

    async fn do_check_table(svc: &RealBackgroundService, database: String, table: String) -> Result<(bool, bool, TableStatistics)> {
        let sql = Self::get_compaction_advice_sql(database, table);
        debug!("type": "compaction", "background": true, "check_sql": sql.as_str(), "check_target_table");
        let res = svc.execute_sql(sql.as_str()).await?;
        let need_segment_compact = res.column(0).as_any().downcast_ref::<BooleanArray>().unwrap().value(0);
        let need_block_compact = res.column(1).as_any().downcast_ref::<BooleanArray>().unwrap().value(0);

        let table_statistics = TableStatistics {
            num_rows: Some(res.column(2).as_any().downcast_ref::<UInt64Array>().unwrap().value(0)),
            data_size: Some(res.column(3).as_any().downcast_ref::<UInt64Array>().unwrap().value(0)),
            data_size_compressed: Some(res.column(4).as_any().downcast_ref::<UInt64Array>().unwrap().value(0)),
            index_size: Some(res.column(5).as_any().downcast_ref::<UInt64Array>().unwrap().value(0)),
        };

        Ok((need_segment_compact, need_block_compact, table_statistics))
    }

    async fn do_segment_compaction(&self, svc: &RealBackgroundService, database: String, table: String) -> Result<()> {
        let limit = if let Some(s) = self.conf.background.compaction.segment_limit {
            format!(" LIMIT {}", s)
        } else {
            "".to_string()
        };
        let sql = format!(SEGMENT_COMPACTION_SQL, database, table, limit);
        debug!("type": "compaction", "background": true, "segment_compaction_sql": sql.as_str(), "segment_compaction");
        svc.execute_sql(sql.as_str()).await?;
        Ok(())
    }

    async fn do_block_compaction(&self, svc: &RealBackgroundService, database: String, table: String) -> Result<()> {
        let limit = if let Some(s) = self.conf.background.compaction.block_limit {
            format!(" LIMIT {}", s)
        } else {
            "".to_string()
        };
        let sql = format!(BLOCK_COMPACTION_SQL, database, table, limit);
        debug!("type": "compaction", "background": true, "block_compaction_sql": sql.as_str(), "block_compaction");
        svc.execute_sql(sql.as_str()).await?;
        Ok(())
    }

    pub fn get_compaction_advice_sql(database: String, table: String) -> String {
        format!(GET_COMPACT_ADVICE, database, table)
    }
}