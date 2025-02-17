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
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;

use super::schema::segment_schema;
use super::segment::ColumnOrientedSegment;
use crate::meta::format::encode;
use crate::meta::AbstractSegment;
use crate::meta::BlockMeta;
use crate::meta::MetaEncoding;
use crate::meta::Statistics;

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
    cluster_stats: Vec<Option<Vec<u8>>>,
    location: (Vec<String>, Vec<u64>),
    bloom_filter_index_location: (Vec<String>, Vec<u64>, MutableBitmap),
    bloom_filter_index_size: Vec<u64>,
    inverted_index_size: Vec<Option<u64>>,
    compression: Vec<u8>,
    create_on: Vec<Option<i64>>,
    column_stats: HashMap<ColumnId, ColStatBuilder>,
    column_meta: HashMap<ColumnId, ColMetaBuilder>,

    summary: Statistics,
    segment_schema: TableSchema,
    table_schema: TableSchemaRef,
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
            column_stats.insert(
                field.column_id(),
                ColStatBuilder::new(&field.data_type(), block_per_segment),
            );
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
            summary: Default::default(),
            segment_schema,
            table_schema,
        }
    }
}

impl SegmentBuilder for ColumnOrientedSegmentBuilder {
    type Segment = ColumnOrientedSegment;
    fn block_count(&self) -> usize {
        self.row_count.len()
    }

    fn add_block(&mut self, block_meta: BlockMeta) -> Result<()> {
        let cluster_stats = block_meta
            .cluster_stats
            .as_ref()
            .map(|stats| encode(&MetaEncoding::MessagePack, stats))
            .transpose()?;
        self.row_count.push(block_meta.row_count);
        self.block_size.push(block_meta.block_size);
        self.file_size.push(block_meta.file_size);
        self.cluster_stats.push(cluster_stats);
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
        let mut columns = vec![
            UInt64Type::from_data(std::mem::take(&mut self.row_count)),
            UInt64Type::from_data(std::mem::take(&mut self.block_size)),
            UInt64Type::from_data(std::mem::take(&mut self.file_size)),
            BinaryType::from_opt_data(std::mem::take(&mut self.cluster_stats)),
            Column::Tuple(vec![
                StringType::from_data(std::mem::take(&mut self.location.0)),
                UInt64Type::from_data(std::mem::take(&mut self.location.1)),
            ]),
            Column::Nullable(Box::new(NullableColumn::new(
                Column::Tuple(vec![
                    StringType::from_data(std::mem::take(&mut self.bloom_filter_index_location.0)),
                    UInt64Type::from_data(std::mem::take(&mut self.bloom_filter_index_location.1)),
                ]),
                std::mem::take(&mut self.bloom_filter_index_location.2).into(),
            ))),
            UInt64Type::from_data(std::mem::take(&mut self.bloom_filter_index_size)),
            UInt64Type::from_opt_data(std::mem::take(&mut self.inverted_index_size)),
            UInt8Type::from_data(std::mem::take(&mut self.compression)),
            Int64Type::from_opt_data(std::mem::take(&mut self.create_on)),
        ];
        let mut column_stats = std::mem::take(&mut self.column_stats);
        let mut column_meta = std::mem::take(&mut self.column_meta);
        for field in self.table_schema.leaf_fields() {
            let col_id = field.column_id();
            let col_stat = column_stats.remove(&col_id).unwrap();
            let col_meta = column_meta.remove(&col_id).unwrap();
            columns.push(Column::Tuple(vec![
                col_stat.min.build(),
                col_stat.max.build(),
                UInt64Type::from_data(col_stat.null_count),
                UInt64Type::from_data(col_stat.in_memory_size),
                UInt64Type::from_opt_data(col_stat.distinct_of_values),
            ]));
            columns.push(Column::Tuple(vec![
                UInt64Type::from_data(col_meta.offset),
                UInt64Type::from_data(col_meta.length),
                UInt64Type::from_data(col_meta.num_values),
            ]));
        }
        let segment = ColumnOrientedSegment {
            block_metas: DataBlock::new_from_columns(columns),
            summary: std::mem::take(&mut self.summary),
            segment_schema: self.segment_schema.clone(),
        };
        Ok(segment)
    }
}
