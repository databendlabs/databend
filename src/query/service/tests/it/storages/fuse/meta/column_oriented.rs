//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use bytes::Bytes;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_storages_fuse::io::BlockWriter;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::statistics::RowOrientedSegmentBuilder;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::meta::AbstractSegment;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnOrientedSegmentBuilder;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::SegmentBuilder;
use opendal::Operator;

fn generate_test_case() -> (TableSchemaRef, Vec<BlockMeta>) {
    let schema = TestFixture::default_table_schema();
    let mut block_metas = Vec::new();
    block_metas.push(BlockMeta {
        row_count: 10,
        block_size: 10,
        file_size: 10,
        col_stats: Default::default(),
        col_metas: Default::default(),
        cluster_stats: None,
        location: Default::default(),
        bloom_filter_index_location: None,
        bloom_filter_index_size: 0,
        inverted_index_size: None,
        compression: Compression::None,
        create_on: None,
    });
    (schema, block_metas)
}

#[test]
fn test_column_oriented_segment() {
    let (schema, block_metas) = generate_test_case();

    let column_oriented_segment = {
        let mut segment_builder = ColumnOrientedSegmentBuilder::new(schema);
        for block_meta in block_metas.iter() {
            segment_builder.add_block(block_meta.clone());
        }
        assert_eq!(segment_builder.block_count(), block_metas.len());
        let segment = segment_builder.build(Default::default(), None).unwrap();
        let serialized = segment.serialize().unwrap();
        let deserialized = segment.deserialize(Bytes::from(serialized)).unwrap();
        deserialized
    };

    let row_oriented_segment = {
        let mut segment_builder = RowOrientedSegmentBuilder::default();
        for block_meta in block_metas.iter() {
            segment_builder.add_block(block_meta.clone()).unwrap();
        }
        segment_builder.build(Default::default(), None).unwrap()
    };
}
