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

use std::ops::Range;
use std::sync::Arc;

use arrow_array::RecordBatch;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchema;
use futures::FutureExt;
use opendal::Buffer;
use vortex::VortexSessionDefault;
use vortex::array::stream::ArrayStreamExt;
use vortex::buffer::Alignment;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::expr::root;
use vortex::expr::select;
use vortex::file::OpenOptionsSessionExt;
use vortex::io::VortexReadAt;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::runtime::current::CurrentThreadRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

pub fn read_vortex(table_schema: &TableSchema, read_buffer: &[u8]) -> Result<DataBlock> {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let projection = table_schema
        .fields()
        .iter()
        .map(|f| Arc::<str>::from(f.name().as_str()))
        .collect::<Vec<_>>();

    let file = session
        .open_options()
        .open_buffer(read_buffer.to_vec())
        .map_err(|e| ErrorCode::Internal(format!("Failed to open vortex file: {e}")))?;
    read_vortex_file(table_schema, runtime, file, projection)
}

pub fn read_vortex_with_ranges(
    table_schema: &TableSchema,
    file_size: u64,
    prefetched_ranges: Vec<(Range<u64>, Buffer)>,
) -> Result<DataBlock> {
    let runtime = CurrentThreadRuntime::new();
    let session = VortexSession::default().with_handle(runtime.handle());
    let projection = table_schema
        .fields()
        .iter()
        .map(|f| Arc::<str>::from(f.name().as_str()))
        .collect::<Vec<_>>();

    let read_at = PrefetchedVortexReadAt::new(file_size, prefetched_ranges);
    let file = runtime
        .block_on(async move { session.open_options().open_read_at(read_at).await })
        .map_err(|e| ErrorCode::Internal(format!("Failed to open vortex file: {e}")))?;

    read_vortex_file(table_schema, runtime, file, projection)
}

fn read_vortex_file(
    table_schema: &TableSchema,
    runtime: CurrentThreadRuntime,
    file: vortex::file::VortexFile,
    projection: Vec<Arc<str>>,
) -> Result<DataBlock> {
    let array = runtime.block_on(async move {
        let mut scan = file
            .scan()
            .map_err(|e| ErrorCode::Internal(format!("Failed to create vortex scan: {e}")))?;
        scan = scan.with_projection(select(projection, root()));

        scan.into_array_stream()
            .map_err(|e| ErrorCode::Internal(format!("Failed to create vortex array stream: {e}")))?
            .read_all()
            .await
            .map_err(|e| ErrorCode::Internal(format!("Failed to read vortex array stream: {e}")))
    })?;

    let batch = RecordBatch::try_from(array.as_ref())
        .map_err(|e| ErrorCode::Internal(format!("Failed to convert vortex array: {e}")))?;

    let data_schema: DataSchema = table_schema.into();
    DataBlock::from_record_batch(&data_schema, &batch)
        .map_err(|e| ErrorCode::Internal(format!("Failed to convert record batch: {e}")))
}

#[derive(Clone)]
struct PrefetchedChunk {
    range: Range<u64>,
    data: ByteBuffer,
}

#[derive(Clone)]
struct PrefetchedVortexReadAt {
    file_size: u64,
    chunks: Arc<Vec<PrefetchedChunk>>,
}

impl PrefetchedVortexReadAt {
    fn new(file_size: u64, mut prefetched_ranges: Vec<(Range<u64>, Buffer)>) -> Self {
        prefetched_ranges.sort_by_key(|(range, _)| (range.start, range.end));
        let chunks = prefetched_ranges
            .into_iter()
            .map(|(range, data)| PrefetchedChunk {
                range,
                data: ByteBuffer::copy_from(data.to_vec()),
            })
            .collect();

        Self {
            file_size,
            chunks: Arc::new(chunks),
        }
    }

    fn find_range(&self, offset: u64, end: u64) -> Option<&PrefetchedChunk> {
        self.chunks
            .iter()
            .find(|chunk| offset >= chunk.range.start && end <= chunk.range.end)
    }
}

impl VortexReadAt for PrefetchedVortexReadAt {
    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> futures::future::BoxFuture<'static, VortexResult<ByteBuffer>> {
        let this = self.clone();
        async move {
            let end = offset + length as u64;
            let Some(chunk) = this.find_range(offset, end) else {
                return Err(vortex::error::vortex_err!(
                    "Requested vortex range {}..{} is not prefetched",
                    offset,
                    end
                ));
            };
            let start = (offset - chunk.range.start) as usize;
            let stop = start + length;
            Ok(chunk.data.slice_unaligned(start..stop).aligned(alignment))
        }
        .boxed()
    }

    fn size(&self) -> futures::future::BoxFuture<'static, VortexResult<u64>> {
        let size = self.file_size;
        async move { Ok(size) }.boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use arrow_array::builder::ListBuilder;
    use arrow_array::builder::StringViewBuilder;
    use databend_common_exception::ErrorCode;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::block_debug::assert_block_value_eq;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::read_vortex;
    use crate::fuse_vortex::write_vortex;

    fn test_schema() -> TableSchema {
        TableSchema::new(vec![
            TableField::new("id", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("name", TableDataType::String),
        ])
    }

    fn projection_test_schema() -> TableSchema {
        TableSchema::new(vec![
            TableField::new("id", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("name", TableDataType::String),
            TableField::new("city", TableDataType::String),
        ])
    }

    #[test]
    fn test_read_vortex_round_trip() {
        let schema = test_schema();
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1i32, 2, 3]),
            StringType::from_data(vec!["alice", "bob", "eve"]),
        ]);

        let decoded = round_trip(&schema, &block);
        assert_block_value_eq(&block, &decoded);
    }

    #[test]
    fn test_read_vortex_projection_subset() {
        let write_schema = projection_test_schema();
        let read_schema = TableSchema::new(vec![
            TableField::new("id", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("city", TableDataType::String),
        ]);

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1i32, 2, 3]),
            StringType::from_data(vec!["alice", "bob", "eve"]),
            StringType::from_data(vec!["shanghai", "beijing", "hangzhou"]),
        ]);

        let decoded = round_trip_with_schema(&write_schema, &read_schema, &block);
        let expected = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1i32, 2, 3]),
            StringType::from_data(vec!["shanghai", "beijing", "hangzhou"]),
        ]);
        assert_block_value_eq(&expected, &decoded);
    }

    #[test]
    fn test_read_vortex_projection_reordered_columns() {
        let write_schema = projection_test_schema();
        let read_schema = TableSchema::new(vec![
            TableField::new("city", TableDataType::String),
            TableField::new("id", TableDataType::Number(NumberDataType::Int32)),
        ]);

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1i32, 2, 3]),
            StringType::from_data(vec!["alice", "bob", "eve"]),
            StringType::from_data(vec!["shanghai", "beijing", "hangzhou"]),
        ]);

        let decoded = round_trip_with_schema(&write_schema, &read_schema, &block);
        let expected = DataBlock::new_from_columns(vec![
            StringType::from_data(vec!["shanghai", "beijing", "hangzhou"]),
            Int32Type::from_data(vec![1i32, 2, 3]),
        ]);
        assert_block_value_eq(&expected, &decoded);
    }

    #[test]
    fn test_read_vortex_projection_empty() {
        let write_schema = projection_test_schema();
        let read_schema = TableSchema::new(vec![]);

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1i32, 2, 3]),
            StringType::from_data(vec!["alice", "bob", "eve"]),
            StringType::from_data(vec!["shanghai", "beijing", "hangzhou"]),
        ]);

        let decoded = round_trip_with_schema(&write_schema, &read_schema, &block);
        assert_eq!(decoded.num_columns(), 0);
        assert_eq!(decoded.num_rows(), block.num_rows());
    }

    #[test]
    fn test_read_vortex_empty_block() {
        let schema = test_schema();
        let data_schema: DataSchema = (&schema).into();
        let block = DataBlock::empty_with_schema(&data_schema);

        let decoded = round_trip(&schema, &block);
        assert_eq!(decoded.num_rows(), 0);
        assert_eq!(decoded.num_columns(), block.num_columns());
        assert_block_value_eq(&block, &decoded);
    }

    #[test]
    fn test_read_vortex_nullable_round_trip() {
        let schema = TableSchema::new(vec![
            TableField::new(
                "id",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int32))),
            ),
            TableField::new(
                "name",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ]);
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_opt_data(vec![Some(1i32), None, Some(3)]),
            StringType::from_opt_data(vec![Some("alice"), None, Some("eve")]),
        ]);

        let decoded = round_trip(&schema, &block);
        assert_block_value_eq(&block, &decoded);
    }

    #[test]
    fn test_read_vortex_string_view_mixed_lengths_nullable_round_trip() {
        let schema = TableSchema::new(vec![
            TableField::new(
                "id",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int32))),
            ),
            TableField::new(
                "name",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ]);
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_opt_data(vec![Some(1), Some(2), Some(3), Some(4), None, Some(6)]),
            StringType::from_opt_data(vec![
                Some(""),
                Some("a"),
                Some("abcdefghijkl"),
                Some("abcdefghijklm"),
                None,
                Some("a very very long string beyond inline storage for string view testing"),
            ]),
        ]);

        let decoded = round_trip(&schema, &block);
        assert_eq!(decoded.num_rows(), block.num_rows());
        assert_block_value_eq(&block, &decoded);
    }

    #[test]
    fn test_read_vortex_string_view_multibyte_utf8_round_trip() {
        let schema = TableSchema::new(vec![TableField::new(
            "name",
            TableDataType::Nullable(Box::new(TableDataType::String)),
        )]);
        let block = DataBlock::new_from_columns(vec![StringType::from_opt_data(vec![
            Some("中文"),
            Some("emoji🙂🚀"),
            Some("áéíóú"),
            Some("👨‍👩‍👧‍👦"),
            Some(""),
            None,
        ])]);

        let decoded = round_trip(&schema, &block);
        assert_block_value_eq(&block, &decoded);
    }

    #[test]
    fn test_read_vortex_string_view_large_block_round_trip() {
        let schema = test_schema();
        let rows = 20_000usize;
        let names: Vec<String> = (0..rows)
            .map(|i| {
                if i % 2 == 0 {
                    format!("row_{i}")
                } else {
                    format!("row_{i}_{}", "x".repeat(96 + (i % 31)))
                }
            })
            .collect();
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data((0..rows as i32).collect()),
            StringType::from_data(names),
        ]);

        let decoded = round_trip(&schema, &block);
        assert_block_value_eq(&block, &decoded);
    }

    #[test]
    fn test_read_vortex_non_nullable_schema_rejects_null() {
        let schema = TableSchema::new(vec![TableField::new("name", TableDataType::String)]);
        let block =
            DataBlock::new_from_columns(vec![StringType::from_opt_data(vec![Some("ok"), None])]);
        let mut buffer = Vec::new();

        let result = write_vortex(&schema, block, &mut buffer);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_vortex_nested_array_nullable_string_round_trip() {
        let mut builder = ListBuilder::new(StringViewBuilder::new());

        builder.values().append_value("a");
        builder.values().append_null();
        builder.values().append_value("abcdefghijklmnop");
        builder.append(true);

        builder.append(true); // empty list
        builder.append(false); // null list

        builder.values().append_value("中文");
        builder.values().append_value("emoji🙂");
        builder.values().append_null();
        builder.append(true);

        let array: ArrayRef = Arc::new(builder.finish());
        let data_type = DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::String))));
        let column = Column::from_arrow_rs(array, &data_type).unwrap();
        let block = DataBlock::new_from_columns(vec![column]);
        let schema = TableSchema::new(vec![TableField::new(
            "tags",
            TableDataType::Array(Box::new(TableDataType::Nullable(Box::new(
                TableDataType::String,
            )))),
        )]);

        let decoded = round_trip(&schema, &block);
        assert_block_value_eq(&block, &decoded);
    }

    #[test]
    fn test_read_vortex_string_view_fuzz_seeded_round_trip() {
        let schema = TableSchema::new(vec![TableField::new(
            "name",
            TableDataType::Nullable(Box::new(TableDataType::String)),
        )]);

        const BASE_SEED: u64 = 20_260_224;
        const ROUNDS: u64 = 5;
        const ROWS_PER_ROUND: usize = 1_000;

        for round in 0..ROUNDS {
            let mut rng = StdRng::seed_from_u64(BASE_SEED + round);
            let values: Vec<Option<String>> = (0..ROWS_PER_ROUND)
                .map(|_| {
                    if rng.gen_range(0..10) == 0 {
                        None
                    } else {
                        Some(random_string(&mut rng))
                    }
                })
                .collect();
            let block = DataBlock::new_from_columns(vec![StringType::from_opt_data(values)]);

            let decoded = round_trip(&schema, &block);
            assert_block_value_eq(&block, &decoded);
        }
    }

    fn round_trip(schema: &TableSchema, block: &DataBlock) -> DataBlock {
        round_trip_with_schema(schema, schema, block)
    }

    fn round_trip_with_schema(
        write_schema: &TableSchema,
        read_schema: &TableSchema,
        block: &DataBlock,
    ) -> DataBlock {
        let mut buffer = Vec::new();
        let _ = write_vortex(write_schema, block.clone(), &mut buffer).unwrap();
        read_vortex(read_schema, &buffer).unwrap()
    }

    fn random_string(rng: &mut StdRng) -> String {
        match rng.gen_range(0..6) {
            0 => String::new(),
            1 => (0..rng.gen_range(1..=32))
                .map(|_| (b'a' + rng.gen_range(0..26)) as char)
                .collect(),
            2 => (0..rng.gen_range(129..=260))
                .map(|_| (b'a' + rng.gen_range(0..26)) as char)
                .collect(),
            3 => {
                const CHINESE: [&str; 7] = ["中", "文", "数", "据", "库", "测", "试"];
                let mut s = String::new();
                for _ in 0..rng.gen_range(2..=16) {
                    s.push_str(CHINESE[rng.gen_range(0..CHINESE.len())]);
                }
                s
            }
            4 => {
                const EMOJI: [&str; 6] = ["🙂", "🚀", "🧪", "🍎", "⚙️", "📦"];
                let mut s = String::new();
                for _ in 0..rng.gen_range(1..=12) {
                    s.push_str(EMOJI[rng.gen_range(0..EMOJI.len())]);
                }
                s
            }
            _ => {
                let n = rng.gen_range(1..=16);
                let mut s = String::new();
                for _ in 0..n {
                    s.push('e');
                    s.push('\u{0301}');
                }
                s
            }
        }
    }

    #[test]
    fn test_read_vortex_invalid_buffer() {
        let schema = test_schema();
        let err = read_vortex(&schema, b"not-a-vortex-file").unwrap_err();

        assert_eq!(err.code(), ErrorCode::INTERNAL);
        assert!(err.message().contains("Failed to"));
    }
}
