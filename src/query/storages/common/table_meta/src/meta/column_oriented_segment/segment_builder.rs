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
use databend_common_exception::span::Span;
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
use databend_common_expression::cast_scalar;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::aggregates::eval_aggr;
use log::warn;

use super::AbstractSegment;
use super::schema::segment_schema;
use super::segment::ColumnOrientedSegment;
use crate::meta::AdditionalStatsMeta;
use crate::meta::BlockMeta;
use crate::meta::ClusterStatistics;
use crate::meta::ColumnStatistics;
use crate::meta::Location;
use crate::meta::MetaEncoding;
use crate::meta::Statistics;
use crate::meta::VirtualBlockMeta;
use crate::meta::format::encode;
use crate::meta::supported_stat_type;

pub trait SegmentBuilder: Send + Sync + 'static {
    type Segment: AbstractSegment;
    fn block_count(&self) -> usize;
    fn add_block(&mut self, block_meta: BlockMeta) -> Result<()>;
    fn build(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
        additional_stats_meta: Option<AdditionalStatsMeta>,
    ) -> Result<Self::Segment>;
    fn new(table_schema: TableSchemaRef, block_per_segment: usize) -> Self;
}

pub struct ColumnOrientedSegmentBuilder {
    row_count: Vec<u64>,
    block_size: Vec<u64>,
    file_size: Vec<u64>,
    cluster_stats: Vec<Option<ClusterStatistics>>,
    location: (Vec<String>, Vec<u64>),
    bloom_filter_index_location: LocationsWithOption,
    bloom_filter_index_size: Vec<u64>,
    inverted_index_size: Vec<Option<u64>>,
    virtual_block_meta: Vec<Option<VirtualBlockMeta>>,
    compression: Vec<u8>,
    create_on: Vec<Option<i64>>,
    column_stats: HashMap<ColumnId, ColStatBuilder>,
    column_meta: HashMap<ColumnId, ColMetaBuilder>,

    segment_schema: TableSchema,
    table_schema: TableSchemaRef,
    block_per_segment: usize,
}

impl ColumnOrientedSegmentBuilder {
    pub fn segment_schema(&self) -> TableSchemaRef {
        self.table_schema.clone()
    }
}

struct ColStatBuilder {
    data_type: DataType,
    min: ColumnBuilder,
    max: ColumnBuilder,
    null_count: Vec<u64>,
    in_memory_size: Vec<u64>,
    distinct_of_values: Vec<Option<u64>>,
}

impl ColStatBuilder {
    fn new(data_type: &TableDataType, block_per_segment: usize) -> Self {
        let data_type: DataType = data_type.into();
        let nullable_type = data_type.wrap_nullable();
        Self {
            data_type: data_type.clone(),
            // min/max stats are nullable in segment schema; keep builders nullable so cast failures
            // can drop stats without panicking on non-nullable builders.
            min: ColumnBuilder::with_capacity(&nullable_type, block_per_segment),
            max: ColumnBuilder::with_capacity(&nullable_type, block_per_segment),
            null_count: Vec::with_capacity(block_per_segment),
            in_memory_size: Vec::with_capacity(block_per_segment),
            distinct_of_values: Vec::with_capacity(block_per_segment),
        }
    }
}

fn cast_stat_scalar(
    column_id: ColumnId,
    value: &Scalar,
    data_type: &DataType,
    block_loc: &str,
    label: &str,
) -> Scalar {
    if value.is_null() {
        return Scalar::Null;
    }
    match cast_scalar(Span::None, value.clone(), data_type, &BUILTIN_FUNCTIONS) {
        Ok(cast) => cast,
        Err(err) => {
            warn!(
                "failed to cast {} stat for column id {} in block {} to {}: {}, dropping stat",
                label, column_id, block_loc, data_type, err
            );
            Scalar::Null
        }
    }
}
#[derive(Default)]
struct ColMetaBuilder {
    offset: Vec<u64>,
    length: Vec<u64>,
    num_values: Vec<u64>,
}

impl SegmentBuilder for ColumnOrientedSegmentBuilder {
    type Segment = ColumnOrientedSegment;
    fn block_count(&self) -> usize {
        self.row_count.len()
    }

    fn add_block(&mut self, block_meta: BlockMeta) -> Result<()> {
        let block_loc = block_meta.location.0.clone();
        self.row_count.push(block_meta.row_count);
        self.block_size.push(block_meta.block_size);
        self.file_size.push(block_meta.file_size);
        self.cluster_stats.push(block_meta.cluster_stats);
        self.location.0.push(block_meta.location.0);
        self.location.1.push(block_meta.location.1);
        self.bloom_filter_index_location
            .add_location(block_meta.bloom_filter_index_location.as_ref());
        self.bloom_filter_index_size
            .push(block_meta.bloom_filter_index_size);
        self.inverted_index_size
            .push(block_meta.inverted_index_size);
        self.virtual_block_meta.push(block_meta.virtual_block_meta);
        self.compression.push(block_meta.compression.to_u8());
        self.create_on
            .push(block_meta.create_on.map(|t| t.timestamp()));
        for (col_id, col_stat) in self.column_stats.iter_mut() {
            let stat = &block_meta.col_stats[col_id];
            let min = cast_stat_scalar(*col_id, &stat.min, &col_stat.data_type, &block_loc, "min");
            let max = cast_stat_scalar(*col_id, &stat.max, &col_stat.data_type, &block_loc, "max");
            col_stat.min.push(min.as_ref());
            col_stat.max.push(max.as_ref());
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
        additional_stats_meta: Option<AdditionalStatsMeta>,
    ) -> Result<Self::Segment> {
        let mut this = std::mem::replace(
            self,
            ColumnOrientedSegmentBuilder::new(self.table_schema.clone(), self.block_per_segment),
        );
        let summary =
            this.build_summary(thresholds, default_cluster_key_id, additional_stats_meta)?;
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
                    StringType::from_data(this.bloom_filter_index_location.locations),
                    UInt64Type::from_data(this.bloom_filter_index_location.versions),
                ]),
                this.bloom_filter_index_location.validity.into(),
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

    fn new(table_schema: TableSchemaRef, block_per_segment: usize) -> Self {
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
            bloom_filter_index_location: LocationsWithOption::new_with_capacity(block_per_segment),
            bloom_filter_index_size: Vec::with_capacity(block_per_segment),
            inverted_index_size: Vec::with_capacity(block_per_segment),
            virtual_block_meta: Vec::with_capacity(block_per_segment),
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

impl ColumnOrientedSegmentBuilder {
    pub fn build_summary(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
        additional_stats_meta: Option<AdditionalStatsMeta>,
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

        let mut index_size = self.bloom_filter_index_size.iter().sum::<u64>()
            + self
                .inverted_index_size
                .iter()
                .map(|v| v.unwrap_or_default())
                .sum::<u64>();

        let mut virtual_block_count = 0;
        for virtual_block_meta in self.virtual_block_meta.iter().flatten() {
            virtual_block_count += 1;
            index_size += virtual_block_meta.virtual_column_size;
        }

        let mut col_stats = HashMap::new();
        let mut self_column_stats = HashMap::new();

        for (col_id, mut col_stat) in std::mem::take(&mut self.column_stats).into_iter() {
            let mins = col_stat.min.build();
            let maxs = col_stat.max.build();
            let (mins_of_mins, _) = eval_aggr(
                "min",
                vec![],
                &[mins.clone().into()],
                block_count as usize,
                vec![],
            )?;
            let min = mins_of_mins
                .index(0)
                .map(|x| x.to_owned())
                .unwrap_or(Scalar::Null);
            let (maxs_of_maxs, _) = eval_aggr(
                "max",
                vec![],
                &[maxs.clone().into()],
                block_count as usize,
                vec![],
            )?;
            let max = maxs_of_maxs
                .index(0)
                .map(|x| x.to_owned())
                .unwrap_or(Scalar::Null);
            let null_count = col_stat.null_count.iter().sum();
            let in_memory_size = col_stat.in_memory_size.iter().sum();
            let distinct_of_values = col_stat
                .distinct_of_values
                .iter()
                .try_fold(0, |acc, ndv| ndv.map(|v| acc + v));
            col_stats.insert(
                col_id,
                ColumnStatistics::new(min, max, null_count, in_memory_size, distinct_of_values),
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
            bloom_index_size: None,
            ngram_index_size: None,
            inverted_index_size: None,
            vector_index_size: None,
            virtual_column_size: None,
            col_stats,
            virtual_col_stats: None,
            cluster_stats,
            virtual_block_count: Some(virtual_block_count),
            additional_stats_meta,
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

struct LocationsWithOption {
    locations: Vec<String>,
    versions: Vec<u64>,
    validity: MutableBitmap,
}

impl LocationsWithOption {
    fn new_with_capacity(capacity: usize) -> Self {
        Self {
            locations: Vec::with_capacity(capacity),
            versions: Vec::with_capacity(capacity),
            validity: MutableBitmap::with_capacity(capacity),
        }
    }

    fn add_location(&mut self, location: Option<&Location>) {
        if let Some(location) = location {
            self.locations.push(location.0.clone());
            self.versions.push(location.1);
            self.validity.push(true);
        } else {
            self.locations.push(String::new());
            self.versions.push(0);
            self.validity.push(false);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_expression::BlockThresholds;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;

    use super::ColumnOrientedSegmentBuilder;
    use super::SegmentBuilder;
    use crate::meta::column_oriented_segment::stat_name;
    use crate::meta::BlockMeta;
    use crate::meta::ColumnMeta;
    use crate::meta::ColumnMetaV0;
    use crate::meta::ColumnStatistics;
    use crate::meta::Compression;

    #[test]
    fn test_drop_stats_on_cast_failure() -> databend_common_exception::Result<()> {
        let schema = Arc::new(TableSchema::new(vec![TableField::new_from_column_id(
            "a",
            TableDataType::Number(NumberDataType::Int64),
            0,
        )]));
        let mut builder = ColumnOrientedSegmentBuilder::new(schema, 1);

        let mut col_stats = HashMap::new();
        col_stats.insert(
            0,
            ColumnStatistics::new(
                Scalar::String("bad".to_string()),
                Scalar::String("bad".to_string()),
                0,
                0,
                None,
            ),
        );
        let mut col_metas = HashMap::new();
        col_metas.insert(0, ColumnMeta::Parquet(ColumnMetaV0::new(0, 0, 1)));

        let block_meta = BlockMeta::new(
            1,
            1,
            1,
            col_stats,
            col_metas,
            None,
            ("dummy".to_string(), 0),
            None,
            0,
            None,
            None,
            None,
            None,
            None,
            Compression::None,
            None,
        );
        builder.add_block(block_meta)?;

        let segment = builder.build(BlockThresholds::default(), None, None)?;
        let stat_idx = segment.segment_schema.index_of(&stat_name(0))?;
        let entry = segment.block_metas.get_by_offset(stat_idx);
        let databend_common_expression::BlockEntry::Column(
            databend_common_expression::Column::Tuple(cols),
        ) = entry
        else {
            panic!("stat column should be a tuple");
        };

        assert!(matches!(cols[0].index(0), Some(ScalarRef::Null)));
        assert!(matches!(cols[1].index(0), Some(ScalarRef::Null)));
        Ok(())
    }
}
