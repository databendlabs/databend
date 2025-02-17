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
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::BinaryArray;
use arrow::array::Int64Array;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::StructArray;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::buffer::NullBuffer;
use arrow::datatypes::SchemaRef;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;

use super::schema::col_stats_fields;
use super::schema::location_fields;
use super::schema::segment_schema;
use super::segment::ColumnOrientedSegment;
use crate::meta::format::encode;
use crate::meta::AbstractSegment;
use crate::meta::BlockMeta;
use crate::meta::ColumnStatistics;
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
    pub row_count: Vec<u64>,
    pub block_size: Vec<u64>,
    pub file_size: Vec<u64>,
    pub location_path: Vec<String>,
    pub location_version: Vec<u64>,
    pub bloom_filter_index_location_path: Vec<String>,
    pub bloom_filter_index_location_version: Vec<u64>,
    pub bloom_filter_index_location_bitmap: Vec<bool>,
    pub bloom_filter_index_size: Vec<u64>,
    pub inverted_index_size: Vec<Option<u64>>,
    pub compression: Vec<u8>,
    pub create_on: Vec<Option<i64>>,
    pub cluster_stats: Vec<Option<Vec<u8>>>,
    pub column_stats: HashMap<ColumnId, Box<dyn ColumnStatBuilderMarker>>,

    pub summary: Statistics,
    pub segment_schema: SchemaRef,
    pub table_schema: TableSchemaRef,
}

impl ColumnOrientedSegmentBuilder {
    pub fn new(table_schema: TableSchemaRef) -> Self {
        let mut column_stats = HashMap::new();
        for field in table_schema.leaf_fields() {
            let builder = match field.data_type() {
                TableDataType::Number(NumberDataType::UInt8) => {
                    Box::new(ColumnStatBuilder::<u8>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Number(NumberDataType::UInt16) => {
                    Box::new(ColumnStatBuilder::<u16>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Number(NumberDataType::UInt32) => {
                    Box::new(ColumnStatBuilder::<u32>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Number(NumberDataType::UInt64) => {
                    Box::new(ColumnStatBuilder::<u64>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Number(NumberDataType::Int8) => {
                    Box::new(ColumnStatBuilder::<i8>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Number(NumberDataType::Int16) => {
                    Box::new(ColumnStatBuilder::<i16>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Number(NumberDataType::Int32) => {
                    Box::new(ColumnStatBuilder::<i32>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Number(NumberDataType::Int64) => {
                    Box::new(ColumnStatBuilder::<i64>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Number(NumberDataType::Float32) => {
                    Box::new(ColumnStatBuilder::<f32>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Number(NumberDataType::Float64) => {
                    Box::new(ColumnStatBuilder::<f64>::new(field.data_type()))
                        as Box<dyn ColumnStatBuilderMarker>
                }
                TableDataType::Null => todo!(),
                TableDataType::EmptyArray => todo!(),
                TableDataType::EmptyMap => todo!(),
                TableDataType::Boolean => todo!(),
                TableDataType::Binary => todo!(),
                TableDataType::String => todo!(),
                TableDataType::Decimal(decimal_data_type) => todo!(),
                TableDataType::Timestamp => todo!(),
                TableDataType::Date => todo!(),
                TableDataType::Nullable(table_data_type) => todo!(),
                TableDataType::Array(table_data_type) => todo!(),
                TableDataType::Map(table_data_type) => todo!(),
                TableDataType::Bitmap => todo!(),
                TableDataType::Tuple {
                    fields_name,
                    fields_type,
                } => todo!(),
                TableDataType::Variant => todo!(),
                TableDataType::Geometry => todo!(),
                TableDataType::Geography => todo!(),
                TableDataType::Interval => todo!(),
            };
            column_stats.insert(field.column_id(), builder);
        }
        Self {
            row_count: vec![],
            block_size: vec![],
            file_size: vec![],
            location_path: vec![],
            location_version: vec![],
            bloom_filter_index_location_path: vec![],
            bloom_filter_index_location_version: vec![],
            bloom_filter_index_location_bitmap: vec![],
            bloom_filter_index_size: vec![],
            inverted_index_size: vec![],
            compression: vec![],
            create_on: vec![],
            cluster_stats: vec![],
            summary: Statistics::default(),
            segment_schema: segment_schema(table_schema.clone()),
            table_schema,
            column_stats,
        }
    }
}

impl SegmentBuilder for ColumnOrientedSegmentBuilder {
    type Segment = ColumnOrientedSegment;
    fn block_count(&self) -> usize {
        self.row_count.len()
    }

    fn add_block(&mut self, block_meta: BlockMeta) -> Result<()> {
        let bloom_filter_index_location_exist = block_meta.bloom_filter_index_location.is_some();
        let bloom_filter_index_location =
            block_meta.bloom_filter_index_location.unwrap_or_default();

        let cluster_stats = block_meta
            .cluster_stats
            .map(|stats| encode(&MetaEncoding::MessagePack, &stats))
            .transpose()?;

        self.row_count.push(block_meta.row_count);
        self.block_size.push(block_meta.block_size);
        self.file_size.push(block_meta.file_size);
        self.location_path.push(block_meta.location.0);
        self.location_version.push(block_meta.location.1);
        self.bloom_filter_index_location_path
            .push(bloom_filter_index_location.0);
        self.bloom_filter_index_location_version
            .push(bloom_filter_index_location.1);
        self.bloom_filter_index_location_bitmap
            .push(bloom_filter_index_location_exist);
        self.bloom_filter_index_size
            .push(block_meta.bloom_filter_index_size);
        self.inverted_index_size
            .push(block_meta.inverted_index_size);
        self.compression.push(block_meta.compression.to_u8());
        self.create_on
            .push(block_meta.create_on.map(|t| t.timestamp()));
        self.cluster_stats.push(cluster_stats);
        for (column_id, builder) in &mut self.column_stats {
            let column_stat = block_meta.col_stats.get(column_id);
            builder.add_stat(column_stat);
        }
        Ok(())
    }

    fn build(
        &mut self,
        thresholds: BlockThresholds,
        default_cluster_key_id: Option<u32>,
    ) -> Result<Self::Segment> {
        let cluster_stats = std::mem::take(&mut self.cluster_stats);
        let cluster_stats = cluster_stats
            .iter()
            .map(|stats| stats.as_ref().map(|stats| stats.as_slice()))
            .collect::<Vec<_>>();
        let mut columns = vec![
            Arc::new(UInt64Array::from(std::mem::take(&mut self.row_count))) as ArrayRef,
            Arc::new(UInt64Array::from(std::mem::take(&mut self.block_size))) as ArrayRef,
            Arc::new(UInt64Array::from(std::mem::take(&mut self.file_size))) as ArrayRef,
            Arc::new(StructArray::try_new(
                location_fields(),
                vec![
                    Arc::new(StringArray::from(std::mem::take(&mut self.location_path)))
                        as ArrayRef,
                    Arc::new(UInt64Array::from(std::mem::take(
                        &mut self.location_version,
                    ))) as ArrayRef,
                ],
                None,
            )?),
            Arc::new(StructArray::try_new(
                location_fields(),
                vec![
                    Arc::new(StringArray::from(std::mem::take(
                        &mut self.bloom_filter_index_location_path,
                    ))) as ArrayRef,
                    Arc::new(UInt64Array::from(std::mem::take(
                        &mut self.bloom_filter_index_location_version,
                    ))) as ArrayRef,
                ],
                Some(NullBuffer::from(std::mem::take(
                    &mut self.bloom_filter_index_location_bitmap,
                ))),
            )?),
            Arc::new(UInt64Array::from(std::mem::take(
                &mut self.bloom_filter_index_size,
            ))) as ArrayRef,
            Arc::new(UInt64Array::from(std::mem::take(
                &mut self.inverted_index_size,
            ))) as ArrayRef,
            Arc::new(UInt8Array::from(std::mem::take(&mut self.compression))) as ArrayRef,
            Arc::new(Int64Array::from(std::mem::take(&mut self.create_on))) as ArrayRef,
            Arc::new(BinaryArray::from_opt_vec(cluster_stats)) as ArrayRef,
        ];
        for field in self.table_schema.leaf_fields() {
            let builder = self.column_stats.get_mut(&field.column_id()).unwrap();
            columns.push(builder.build()?);
        }
        let block_metas = RecordBatch::try_new(self.segment_schema.clone(), columns)?;
        Ok(ColumnOrientedSegment {
            block_metas,
            summary: std::mem::take(&mut self.summary),
        })
    }
}

struct ColumnStatBuilder<T> {
    min: Vec<T>,
    max: Vec<T>,
    null_count: Vec<u64>,
    in_memory_size: Vec<u64>,
    distinct_of_values: Vec<Option<u64>>,
    bitmap: Vec<bool>,
    col_type: TableDataType,
}

impl<T> ColumnStatBuilder<T> {
    pub fn new(col_type: &TableDataType) -> Self {
        Self {
            min: vec![],
            max: vec![],
            null_count: vec![],
            in_memory_size: vec![],
            distinct_of_values: vec![],
            bitmap: vec![],
            col_type: col_type.clone(),
        }
    }
}

pub trait ColumnStatBuilderMarker: Send + Sync + 'static {
    fn add_stat(&mut self, column_stat: Option<&ColumnStatistics>);
    fn build(&self) -> Result<ArrayRef>;
}

impl<T: Send + Sync + 'static> ColumnStatBuilderMarker for ColumnStatBuilder<T> {
    fn add_stat(&mut self, column_stat: Option<&ColumnStatistics>) {
        todo!()
    }

    fn build(&self) -> Result<ArrayRef> {
        let columns = vec![];
        let array = StructArray::try_new(
            col_stats_fields(&self.col_type),
            columns,
            Some(NullBuffer::from(self.bitmap.clone())),
        )?;
        Ok(Arc::new(array) as ArrayRef)
    }
}
