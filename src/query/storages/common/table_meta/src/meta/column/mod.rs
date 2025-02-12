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

mod block_meta;
mod cluster_statistics;
mod schema;
mod segment;
mod segment_builder;

pub use block_meta::AbstractBlockMeta;
pub use cluster_statistics::AbstractClusterStatistics;
pub use segment::AbstractSegment;
pub use segment::ColumnOrientedSegment;
pub use segment_builder::ColumnOrientedSegmentBuilder;
pub use segment_builder::SegmentBuilder;

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use databend_common_expression::TableSchema;
    use databend_common_expression::TableSchemaRef;
    use databend_common_expression::TableSchemaRefExt;

    use super::*;
    use crate::meta::BlockMeta;

    fn generate_block_metas() -> Vec<BlockMeta> {
        todo!()
    }

    fn generate_schema() -> TableSchemaRef {
        todo!()
    }

    #[test]
    fn test_column_oriented_segment() {
        let block_metas = generate_block_metas();
        let schema = generate_schema();
        let num_blocks = block_metas.len();
        let mut segment_builder = ColumnOrientedSegmentBuilder::new(schema);
        for block_meta in block_metas {
            segment_builder.add_block(block_meta);
        }
        assert_eq!(segment_builder.block_count(), num_blocks);
        let segment = segment_builder.build(Default::default(), None).unwrap();
        let serialized = segment.serialize().unwrap();
        let deserialized = segment.deserialize(Bytes::from(serialized)).unwrap();
    }
}
