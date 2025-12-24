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

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::FuseTable;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::TableMetaFunc;
use crate::table_functions::TableMetaFuncTemplate;

pub struct ClusteringStatistics;

pub type ClusteringStatisticsFunc = TableMetaFuncTemplate<ClusteringStatistics>;

#[async_trait::async_trait]
impl TableMetaFunc for ClusteringStatistics {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("segment_name", TableDataType::String),
            TableField::new("block_name", TableDataType::String),
            TableField::new("min", TableDataType::String.wrap_nullable()),
            TableField::new("max", TableDataType::String.wrap_nullable()),
            TableField::new(
                "level",
                TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
            ),
            TableField::new("pages", TableDataType::String.wrap_nullable()),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let Some(cluster_key_id) = tbl.cluster_key_id() else {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table {}",
                tbl.get_table_info().desc,
            )));
        };

        let limit = limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(snapshot.summary.block_count as usize, limit);

        let mut segment_name = Vec::with_capacity(len);
        let mut block_name = StringColumnBuilder::with_capacity(len);
        let mut max = Vec::with_capacity(len);
        let mut min = Vec::with_capacity(len);
        let mut level = Vec::with_capacity(len);
        let mut pages = Vec::with_capacity(len);

        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), tbl.schema());

        let mut row_num = 0;
        let chunk_size =
            std::cmp::min(ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);
        let format_vec = |v: &[Scalar]| -> String {
            format!(
                "[{}]",
                v.iter()
                    .map(|item| format!("{}", item))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        'FOR: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for (i, segment) in segments.into_iter().enumerate() {
                let segment = segment?;
                segment_name.extend(std::iter::repeat_n(
                    chunk[i].0.clone(),
                    segment.blocks.len(),
                ));

                for block in segment.blocks.iter() {
                    let block = block.as_ref();
                    block_name.put_and_commit(&block.location.0);

                    let cluster_stats = block.cluster_stats.as_ref();
                    let clustered = block
                        .cluster_stats
                        .as_ref()
                        .is_some_and(|v| v.cluster_key_id == cluster_key_id);

                    if clustered {
                        // Safe to unwrap
                        let cluster_stats = cluster_stats.unwrap();
                        min.push(Some(format_vec(cluster_stats.min())));
                        max.push(Some(format_vec(cluster_stats.max())));
                        level.push(Some(cluster_stats.level));
                        pages.push(cluster_stats.pages.as_ref().map(|v| format_vec(v)));
                    } else {
                        min.push(None);
                        max.push(None);
                        level.push(None);
                        pages.push(None);
                    }

                    row_num += 1;
                    if row_num >= limit {
                        break 'FOR;
                    }
                }
            }
        }

        Ok(DataBlock::new(
            vec![
                StringType::from_data(segment_name).into(),
                Column::String(block_name.build()).into(),
                StringType::from_opt_data(min).into(),
                StringType::from_opt_data(max).into(),
                Int32Type::from_opt_data(level).into(),
                StringType::from_opt_data(pages).into(),
            ],
            row_num,
        ))
    }
}
