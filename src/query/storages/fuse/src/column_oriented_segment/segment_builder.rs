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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::aggregates::eval_aggr;
use databend_storages_common_table_meta::meta::format::encode;
use databend_storages_common_table_meta::meta::supported_stat_type;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::MetaEncoding;
use databend_storages_common_table_meta::meta::Statistics;

use super::schema::segment_schema;
use super::segment::ColumnOrientedSegment;
use super::AbstractSegment;

pub trait SegmentBuilder: Send + Sync + 'static {
    type Segment: AbstractSegment;
    fn block_count(&self) -> usize;
    fn add_block(&mut self, block_meta: BlockMeta) -> Result<()>;
    fn build(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Result<Self::Segment>;
}

pub struct ColumnOrientedSegmentBuilder {
    row_count: Vec<u64>,
    block_size: Vec<u64>,
    file_size: Vec<u64>,
    cluster_stats: Vec<Option<ClusterStatistics>>,
    location: (Vec<String>, Vec<u64>),
    bloom_filter_index_location: (Vec<String>, Vec<u64>, MutableBitmap),
    bloom_filter_index_size: Vec<u64>,
    inverted_index_size: Vec<Option<u64>>,
    compression: Vec<u8>,
    create_on: Vec<Option<i64>>,
    column_stats: HashMap<ColumnId, ColStatBuilder>,
    column_meta: HashMap<ColumnId, ColMetaBuilder>,

    segment_schema: TableSchema,
    table_schema: TableSchemaRef,
    block_per_segment: usize,
}

struct ColStatBuilder {
    min: ColumnBuilder,
    max: ColumnBuilder,
    null_count: Vec<u64>,
    in_memory_size: Vec<u64>,
    distinct_of_values: Vec<Option<u64>>,
}

impl ColStatBuilder {
    fn new(data_type: &TableDataType, block_per_segment: usize) -> Self {
        let data_type: DataType = data_type.into();
        Self {
            min: ColumnBuilder::with_capacity(&data_type, block_per_segment),
            max: ColumnBuilder::with_capacity(&data_type, block_per_segment),
            null_count: Vec::with_capacity(block_per_segment),
            in_memory_size: Vec::with_capacity(block_per_segment),
            distinct_of_values: Vec::with_capacity(block_per_segment),
        }
    }
}
#[derive(Default)]
struct ColMetaBuilder {
    offset: Vec<u64>,
    length: Vec<u64>,
    num_values: Vec<u64>,
}

impl ColumnOrientedSegmentBuilder {
    pub fn new(table_schema: TableSchemaRef, block_per_segment: usize) -> Self {
        let segment_schema = segment_schema(&table_schema);

        let mut column_stats = HashMap::new();
        let mut column_meta = HashMap::new();

        for field in table_schema.leaf_fields() {
            if supported_stat_type(&field.data_type().into()) {
                column_stats.insert(
                    field.column_id(),
                    ColStatBuilder::new(field.data_type(), block_per_segment),
                );
            }
            column_meta.insert(field.column_id(), ColMetaBuilder::default());
        }

        Self {
            row_count: Vec::with_capacity(block_per_segment),
            block_size: Vec::with_capacity(block_per_segment),
            file_size: Vec::with_capacity(block_per_segment),
            cluster_stats: Vec::with_capacity(block_per_segment),
            location: (
                Vec::with_capacity(block_per_segment),
                Vec::with_capacity(block_per_segment),
            ),
            bloom_filter_index_location: (
                Vec::with_capacity(block_per_segment),
                Vec::with_capacity(block_per_segment),
                MutableBitmap::with_capacity(block_per_segment),
            ),
            bloom_filter_index_size: Vec::with_capacity(block_per_segment),
            inverted_index_size: Vec::with_capacity(block_per_segment),
            compression: Vec::with_capacity(block_per_segment),
            create_on: Vec::with_capacity(block_per_segment),
            column_stats,
            column_meta,
            segment_schema,
            table_schema,
            block_per_segment,
        }
    }
}

impl SegmentBuilder for ColumnOrientedSegmentBuilder {
    type Segment = ColumnOrientedSegment;
    fn block_count(&self) -> usize {
        self.row_count.len()
    }

    fn add_block(&mut self, block_meta: BlockMeta) -> Result<()> {
        self.row_count.push(block_meta.row_count);
        self.block_size.push(block_meta.block_size);
        self.file_size.push(block_meta.file_size);
        self.cluster_stats.push(block_meta.cluster_stats);
        self.location.0.push(block_meta.location.0);
        self.location.1.push(block_meta.location.1);
        self.bloom_filter_index_location.0.push(
            block_meta
                .bloom_filter_index_location
                .as_ref()
                .map(|l| l.0.clone())
                .unwrap_or_default(),
        );
        self.bloom_filter_index_location.1.push(
            block_meta
                .bloom_filter_index_location
                .as_ref()
                .map(|l| l.1)
                .unwrap_or_default(),
        );
        self.bloom_filter_index_location
            .2
            .push(block_meta.bloom_filter_index_location.is_some());
        self.bloom_filter_index_size
            .push(block_meta.bloom_filter_index_size);
        self.inverted_index_size
            .push(block_meta.inverted_index_size);
        self.compression.push(block_meta.compression.to_u8());
        self.create_on
            .push(block_meta.create_on.map(|t| t.timestamp()));
        for (col_id, col_stat) in self.column_stats.iter_mut() {
            let stat = &block_meta.col_stats[col_id];
            col_stat.min.push(stat.min.as_ref());
            col_stat.max.push(stat.max.as_ref());
            col_stat.null_count.push(stat.null_count);
            col_stat.in_memory_size.push(stat.in_memory_size);
            col_stat.distinct_of_values.push(stat.distinct_of_values);
        }
        for (col_id, col_meta) in self.column_meta.iter_mut() {
            let meta = &block_meta.col_metas[col_id].as_parquet().unwrap();
            col_meta.offset.push(meta.offset);
            col_meta.length.push(meta.len);
            col_meta.num_values.push(meta.num_values);
        }
        Ok(())
    }

    fn build(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Result<Self::Segment> {
        let mut this = std::mem::replace(
            self,
            ColumnOrientedSegmentBuilder::new(self.table_schema.clone(), self.block_per_segment),
        );
        let summary = this.build_summary(thresholds, default_cluster_key_id)?;
        let cluster_stats = this.cluster_stats;
        let mut cluster_stats_binary = Vec::with_capacity(cluster_stats.len());
        for stats in cluster_stats {
            if let Some(stats) = stats {
                cluster_stats_binary.push(Some(encode(&MetaEncoding::MessagePack, &stats)?));
            } else {
                cluster_stats_binary.push(None);
            }
        }
        let mut columns = vec![
            UInt64Type::from_data(this.row_count),
            UInt64Type::from_data(this.block_size),
            UInt64Type::from_data(this.file_size),
            BinaryType::from_opt_data(cluster_stats_binary),
            Column::Tuple(vec![
                StringType::from_data(this.location.0),
                UInt64Type::from_data(this.location.1),
            ]),
            Column::Nullable(Box::new(NullableColumn::new(
                Column::Tuple(vec![
                    StringType::from_data(this.bloom_filter_index_location.0),
                    UInt64Type::from_data(this.bloom_filter_index_location.1),
                ]),
                this.bloom_filter_index_location.2.into(),
            ))),
            UInt64Type::from_data(this.bloom_filter_index_size),
            UInt64Type::from_opt_data(this.inverted_index_size),
            UInt8Type::from_data(this.compression),
            Int64Type::from_opt_data(this.create_on),
        ];
        let mut column_stats = this.column_stats;
        let mut column_meta = this.column_meta;
        for field in this.table_schema.leaf_fields() {
            let col_id = field.column_id();
            if supported_stat_type(&field.data_type().into()) {
                let col_stat = column_stats.remove(&col_id).unwrap();
                columns.push(Column::Tuple(vec![
                    col_stat.min.build(),
                    col_stat.max.build(),
                    UInt64Type::from_data(col_stat.null_count),
                    UInt64Type::from_data(col_stat.in_memory_size),
                    UInt64Type::from_opt_data(col_stat.distinct_of_values),
                ]));
            }

            let col_meta = column_meta.remove(&col_id).unwrap();
            columns.push(Column::Tuple(vec![
                UInt64Type::from_data(col_meta.offset),
                UInt64Type::from_data(col_meta.length),
                UInt64Type::from_data(col_meta.num_values),
            ]));
        }
        let segment = ColumnOrientedSegment {
            block_metas: DataBlock::new_from_columns(columns),
            summary,
            segment_schema: this.segment_schema.clone(),
        };
        Ok(segment)
    }
}

impl ColumnOrientedSegmentBuilder {
    pub fn build_summary(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Result<Statistics> {
        let row_count = self.row_count.iter().sum();
        let block_count = self.row_count.len() as u64;

        let perfect_block_count = self
            .row_count
            .iter()
            .zip(self.block_size.iter())
            .zip(self.cluster_stats.iter())
            .filter(|((row_count, block_size), cluster_stats)| {
                thresholds.check_large_enough(**row_count as usize, **block_size as usize)
                    || cluster_stats.as_ref().is_some_and(|v| v.level != 0)
            })
            .count() as u64;

        let uncompressed_byte_size = self.block_size.iter().sum();
        let compressed_byte_size = self.file_size.iter().sum();

        let index_size = self.bloom_filter_index_size.iter().sum::<u64>()
            + self
                .inverted_index_size
                .iter()
                .map(|v| v.unwrap_or_default())
                .sum::<u64>();

        let mut col_stats = HashMap::new();
        let mut self_column_stats = HashMap::new();

        for (col_id, mut col_stat) in std::mem::take(&mut self.column_stats).into_iter() {
            let mins = col_stat.min.build();
            let maxs = col_stat.max.build();
            let (mins_of_mins, _) =
                eval_aggr("min", vec![], &[mins.clone()], block_count as usize)?;
            let min = mins_of_mins
                .index(0)
                .map(|x| x.to_owned())
                .unwrap_or(Scalar::Null);
            let (maxs_of_maxs, _) =
                eval_aggr("max", vec![], &[maxs.clone()], block_count as usize)?;
            let max = maxs_of_maxs
                .index(0)
                .map(|x| x.to_owned())
                .unwrap_or(Scalar::Null);
            let null_count = col_stat.null_count.iter().sum();
            let in_memory_size = col_stat.in_memory_size.iter().sum();
            col_stats.insert(
                col_id,
                ColumnStatistics::new(min, max, null_count, in_memory_size, None),
            );
            col_stat.min = ColumnBuilder::from_column(mins);
            col_stat.max = ColumnBuilder::from_column(maxs);
            self_column_stats.insert(col_id, col_stat);
        }
        self.column_stats = self_column_stats;

        let cluster_stats = reduce_cluster_statistics(&self.cluster_stats, default_cluster_key_id);

        Ok(Statistics {
            row_count,
            block_count,
            perfect_block_count,
            uncompressed_byte_size,
            compressed_byte_size,
            index_size,
            col_stats,
            cluster_stats,
        })
    }
}

fn reduce_cluster_statistics<T: Borrow<Option<ClusterStatistics>>>(
    blocks_cluster_stats: &[T],
    default_cluster_key_id: Option<u32>,
) -> Option<ClusterStatistics> {
    if blocks_cluster_stats.is_empty() || default_cluster_key_id.is_none() {
        return None;
    }

    let cluster_key_id = default_cluster_key_id.unwrap();
    let len = blocks_cluster_stats.len();
    let mut min_stats = Vec::with_capacity(len);
    let mut max_stats = Vec::with_capacity(len);
    let mut levels = Vec::with_capacity(len);

    for cluster_stats in blocks_cluster_stats.iter() {
        if let Some(stat) = cluster_stats.borrow() {
            if stat.cluster_key_id != cluster_key_id {
                return None;
            }

            min_stats.push(stat.min());
            max_stats.push(stat.max());
            levels.push(stat.level);
        } else {
            return None;
        }
    }

    let min = min_stats
        .into_iter()
        .min_by(|x, y| x.iter().cmp_by(y.iter(), cmp_with_null))
        .unwrap();
    let max = max_stats
        .into_iter()
        .max_by(|x, y| x.iter().cmp_by(y.iter(), cmp_with_null))
        .unwrap();
    let level = levels.into_iter().max().unwrap_or(0);

    Some(ClusterStatistics::new(
        cluster_key_id,
        min.clone(),
        max.clone(),
        level,
        None,
    ))
}

fn cmp_with_null(v1: &Scalar, v2: &Scalar) -> Ordering {
    match (v1.is_null(), v2.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => v1.cmp(v2),
    }
}
