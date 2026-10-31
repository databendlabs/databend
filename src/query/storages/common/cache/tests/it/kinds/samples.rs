// Copyright 2026 Datafuse Labs.
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

//! A real value of every kind.

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;

use ::parquet::arrow::ArrowWriter;
use ::parquet::file::metadata::ParquetMetaDataReader;
use arrow::array::ArrayRef;
use arrow::array::Int32Array;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::table::Table;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Column;
use databend_common_expression::Scalar;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorColumn;
use databend_common_meta_app::schema::TableInfo;
use databend_common_storage::MetaHLL;
use databend_storages_common_cache::CacheDef;
use databend_storages_common_cache::CachedColumnArray;
use databend_storages_common_cache::CachedVectorIndex;
use databend_storages_common_cache::ColumnData;
use databend_storages_common_cache::IcebergTableCacheValue;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::iceberg;
use databend_storages_common_cache::kinds::inverted;
use databend_storages_common_cache::kinds::parquet;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::spatial;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::kinds::vector;
use databend_storages_common_cache::kinds::virtual_column;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::BundleOpenSlice;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::HNSWIndex;
use databend_storages_common_index::IndexFile;
use databend_storages_common_index::IndexMeta;
use databend_storages_common_index::InvertedIndexBundleFooter;
use databend_storages_common_index::InvertedIndexLookupBytes;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_index::InvertedIndexPayloadBytes;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_index::filters::FilterBuilder;
use databend_storages_common_index::filters::FilterImpl;
use databend_storages_common_index::filters::Xor8Builder;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::column_oriented_segment::AbstractSegment;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegment;
use databend_storages_common_table_meta::meta::column_oriented_segment::ColumnOrientedSegmentBuilder;
use databend_storages_common_table_meta::meta::column_oriented_segment::SegmentBuilder;
use databend_storages_common_table_meta::meta::column_oriented_segment::VirtualBlockInput;
use databend_storages_common_table_meta::meta::column_oriented_segment::deserialize_column_oriented_segment;

use crate::mem_bytes_accuracy::block_meta;
use crate::mem_bytes_accuracy::block_metas;
use crate::mem_bytes_accuracy::index_meta;
use crate::mem_bytes_accuracy::schema;
use crate::mem_bytes_accuracy::summary;

/// How closely `mem_bytes` follows the bytes a value holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Accuracy {
    /// Within the margin of the other measured values.
    Measured,
    /// Parts of the value are sized by a typical figure (`dyn Table`, `dyn PartInfo`) or left
    /// out; only the order of magnitude is checked.
    Estimated,
    /// The sample stands in for a value whose size it cannot reproduce; not checked.
    Unchecked,
}

/// A real value of a kind; `seed` makes values of one kind differ.
pub trait Sample: CacheDef {
    const ACCURACY: Accuracy = Accuracy::Measured;

    fn sample(seed: u32) -> Self::Value;
}

fn bytes(seed: u32, len: usize) -> Bytes {
    Bytes::from(vec![seed as u8; len])
}

impl Sample for table::Snapshot {
    fn sample(seed: u32) -> Arc<TableSnapshot> {
        let segments = (0..16)
            .map(|i| (format!("1/2/_sg/{seed:08}{i:024}_v4.mpk"), 4))
            .collect();
        let snapshot = TableSnapshot::try_new(
            None,
            None,
            schema(4),
            summary(4),
            segments,
            None,
            None,
            TableMetaTimestamps::new(None, chrono::Duration::hours(1)),
        )
        .unwrap();
        Arc::new(snapshot)
    }
}

impl Sample for table::SnapshotStats {
    fn sample(seed: u32) -> Arc<TableSnapshotStatistics> {
        let hll = (0..4).map(|i| (i + seed, MetaHLL::new())).collect();
        Arc::new(TableSnapshotStatistics::new(
            hll,
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            Default::default(),
            0,
        ))
    }
}

impl Sample for table::PruneResult {
    const ACCURACY: Accuracy = Accuracy::Estimated;

    fn sample(_seed: u32) -> (PartStatistics, Partitions) {
        let partitions = Partitions::create(PartitionsShuffleKind::Seq, vec![]);
        (PartStatistics::default(), partitions)
    }
}

impl Sample for segment::Info {
    fn sample(seed: u32) -> Arc<CompactSegmentInfo> {
        let segment = SegmentInfo::new(block_metas(2, 4 + seed % 4), summary(2));
        Arc::new(CompactSegmentInfo::try_from(&segment).unwrap())
    }
}

impl Sample for segment::ColumnOriented {
    /// A full segment as cached: read back with every column from a file written once. The
    /// value keeps the read buffer, which `mem_bytes` does not see; it is a small part of a
    /// full segment.
    fn sample(_seed: u32) -> Arc<ColumnOrientedSegment> {
        static FILE: OnceLock<(Vec<u8>, HashSet<String>)> = OnceLock::new();
        let (file, columns) = FILE.get_or_init(|| {
            let mut builder = ColumnOrientedSegmentBuilder::new(Arc::new(schema(4)), 1000);
            for i in 0..1000 {
                let mut meta = block_meta(4, i);
                meta.create_on = Some(chrono::Utc::now());
                builder.add_block(meta, VirtualBlockInput::None).unwrap();
            }
            let written = builder
                .build(BlockThresholds::default(), None, None)
                .unwrap();
            let columns = written
                .segment_schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            (written.serialize().unwrap(), columns)
        });
        let (block_metas, segment_schema, summary) =
            deserialize_column_oriented_segment(Bytes::from(file.clone()), columns, true).unwrap();
        Arc::new(ColumnOrientedSegment {
            block_metas,
            summary: summary.unwrap(),
            segment_schema,
        })
    }
}

impl Sample for segment::BlockMetas {
    fn sample(seed: u32) -> Arc<Vec<Arc<BlockMeta>>> {
        Arc::new(block_metas(2, 4 + seed % 4))
    }
}

impl Sample for segment::Stats {
    fn sample(seed: u32) -> Arc<SegmentStatistics> {
        let hlls = (0..4).map(|_| vec![seed as u8; 64]).collect();
        Arc::new(SegmentStatistics::new(hlls, vec![]))
    }
}

impl Sample for bloom::IndexMeta {
    fn sample(seed: u32) -> Arc<BloomIndexMeta> {
        Arc::new(BloomIndexMeta {
            columns: index_meta(4 + seed % 4).columns,
        })
    }
}

impl Sample for bloom::Filter {
    fn sample(seed: u32) -> Arc<FilterImpl> {
        let mut builder = Xor8Builder::create();
        let keys: Vec<u64> = (0..256).map(|i| i + seed as u64 * 1000).collect();
        builder.add_keys(&keys);
        Arc::new(FilterImpl::Xor(builder.build().unwrap()))
    }
}

impl Sample for inverted::Meta {
    fn sample(seed: u32) -> InvertedIndexMeta {
        let postings = vec![seed as u8; 64];
        let slices = BTreeMap::from([(PathBuf::from("segment.idx"), vec![BundleOpenSlice {
            range: 0..4,
            bytes: Arc::from(&postings[..4]),
        }])]);
        let object = InvertedIndexBundleFooter::build(
            [("segment.idx", postings.as_slice())],
            BTreeMap::new(),
            slices,
            b"managed".to_vec(),
            b"meta".to_vec(),
        )
        .unwrap();
        let footer = InvertedIndexBundleFooter::open(&object).unwrap();
        let footer_bytes = Bytes::copy_from_slice(&object[footer.footer_start as usize..]);
        InvertedIndexMeta::new(object.len() as u64, footer_bytes)
    }
}

impl Sample for inverted::Lookup {
    fn sample(seed: u32) -> InvertedIndexLookupBytes {
        InvertedIndexLookupBytes::new(bytes(seed, 4096))
    }
}

impl Sample for inverted::Payload {
    fn sample(seed: u32) -> InvertedIndexPayloadBytes {
        InvertedIndexPayloadBytes::new(bytes(seed, 4096))
    }
}

impl Sample for inverted::FullPayload {
    fn sample(seed: u32) -> InvertedIndexPayloadBytes {
        InvertedIndexPayloadBytes::new(bytes(seed, 8192))
    }
}

impl Sample for vector::Meta {
    fn sample(seed: u32) -> Arc<VectorIndexMeta> {
        Arc::new(VectorIndexMeta::Legacy(index_meta(4 + seed % 4)))
    }
}

impl Sample for vector::Index {
    /// About 10% over what the index holds; the cache errs toward charging more.
    const ACCURACY: Accuracy = Accuracy::Estimated;

    /// An index as cached: opened from its stored components, built once.
    fn sample(_seed: u32) -> CachedVectorIndex {
        const ROWS: usize = 256;
        const DIM: usize = 16;
        static COMPONENTS: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
        let components = COMPONENTS.get_or_init(|| {
            let values = (0..ROWS * DIM)
                .map(|i| F32::from((i as f32 * 0.17).sin()))
                .collect::<Vec<_>>();
            let column = Column::Vector(VectorColumn::Float32((values.into(), DIM)));
            let (_, entries) = HNSWIndex::build(16, 64, 0, column, DistanceType::L2).unwrap();
            entries
                .into_iter()
                .map(|entry| match entry {
                    BlockEntry::Const(Scalar::Binary(bytes), _, _) => bytes,
                    _ => unreachable!("HNSW build produces binary components"),
                })
                .collect()
        });
        let index = HNSWIndex::open(DistanceType::L2, DIM, ROWS, components, None).unwrap();
        CachedVectorIndex::new(index)
    }
}

impl Sample for vector::Refine {
    fn sample(seed: u32) -> IndexFile {
        IndexFile::create(format!("{seed}_fp16_data"), bytes(seed, 4096))
    }
}

impl Sample for spatial::Meta {
    fn sample(seed: u32) -> Arc<IndexMeta> {
        Arc::new(index_meta(4 + seed % 4))
    }
}

impl Sample for spatial::Index {
    fn sample(seed: u32) -> IndexFile {
        IndexFile::create(format!("geom_{seed}"), bytes(seed, 4096))
    }
}

impl Sample for virtual_column::Meta {
    fn sample(seed: u32) -> Arc<VirtualColumnFileMeta> {
        Arc::new(VirtualColumnFileMeta {
            string_table: (0..8).map(|i| format!("path_{seed}_{i}")).collect(),
            column_metas: vec![],
            typed_shared_column_metas: Default::default(),
            virtual_column_nodes: HashMap::new(),
        })
    }
}

impl Sample for parquet::Meta {
    fn sample(seed: u32) -> Arc<::parquet::file::metadata::ParquetMetaData> {
        let column: ArrayRef = Arc::new(Int32Array::from(vec![seed as i32; 64]));
        let batch = RecordBatch::try_from_iter([("a", column)]).unwrap();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let meta = ParquetMetaDataReader::new()
            .parse_and_finish(&Bytes::from(file))
            .unwrap();
        Arc::new(meta)
    }
}

/// The smallest table an iceberg cache value can hold.
struct FakeTable(TableInfo);

impl Table for FakeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.0
    }
}

impl Sample for iceberg::Table {
    /// A real table holds iceberg metadata the fake one does not have.
    const ACCURACY: Accuracy = Accuracy::Unchecked;

    fn sample(seed: u32) -> IcebergTableCacheValue {
        let info = TableInfo {
            name: format!("t{seed}"),
            ..Default::default()
        };
        IcebergTableCacheValue::new(Arc::new(FakeTable(info)), None)
    }
}

impl Sample for column::Data {
    fn sample(seed: u32) -> ColumnData {
        ColumnData::from_bytes(bytes(seed, 4096))
    }
}

impl Sample for column::Array {
    fn sample(seed: u32) -> (CachedColumnArray, usize) {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![seed as i32; 1024]));
        let size = array.get_array_memory_size();
        (CachedColumnArray::Arrow(array), size)
    }
}
