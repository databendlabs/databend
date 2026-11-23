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

use databend_common_column::binary::BinaryColumn;
use databend_common_column::binary::BinaryColumnBuilder;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::buffer::Buffer;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VectorColumn;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::number::F32;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct DummyMeta(usize);

#[typetag::serde(name = "compact_test_dummy_meta")]
impl BlockMetaInfo for DummyMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        DummyMeta::downcast_ref_from(info)
            .map(|other| other == self)
            .unwrap_or(false)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

fn build_array_i64(rows: usize, elems_per_row: usize) -> Column {
    let mut offsets = Vec::with_capacity(rows + 1);
    offsets.push(0u64);
    let mut vals = Vec::with_capacity(rows * elems_per_row);
    for r in 0..rows {
        for e in 0..elems_per_row {
            vals.push((r * 100 + e) as i64);
        }
        offsets.push(vals.len() as u64);
    }
    Column::Array(Box::new(ArrayColumn::new(
        Int64Type::from_data(vals),
        offsets.into(),
    )))
}

fn build_array_string(rows: usize, elems_per_row: usize) -> Column {
    let mut builder = ArrayColumnBuilder::<AnyType> {
        builder: ColumnBuilder::with_capacity(&DataType::String, rows * elems_per_row),
        offsets: {
            let mut o = Vec::with_capacity(rows + 1);
            o.push(0);
            o
        },
    };
    for r in 0..rows {
        let mut row_vals = Vec::with_capacity(elems_per_row);
        for e in 0..elems_per_row {
            row_vals.push(format!("one_id_{r:06}_{e:02}_padding_over_12_bytes"));
        }
        let col = StringType::from_data(row_vals);
        builder.push(col);
    }
    Column::Array(Box::new(builder.build()))
}

fn build_binary_col(rows: usize) -> BinaryColumn {
    let mut builder = BinaryColumnBuilder::with_capacity(rows, rows * 24);
    for r in 0..rows {
        let payload = format!("bin_payload_{r:012}");
        builder.put_slice(payload.as_bytes());
        builder.commit_row();
    }
    builder.build()
}

fn build_mixed_block(rows: usize) -> DataBlock {
    let ids = Int64Type::from_data((0..rows as i64).collect::<Vec<_>>());
    let long_strs = StringType::from_data(
        (0..rows)
            .map(|r| format!("row_id_{r:08}_000000000000000000000000"))
            .collect::<Vec<_>>(),
    );
    let inline_strs = StringType::from_data(
        (0..rows)
            .map(|r| format!("s{}", r % 10))
            .collect::<Vec<_>>(),
    );
    let arr_i64 = build_array_i64(rows, 4);
    let arr_str = build_array_string(rows, 2);

    let mut validity = MutableBitmap::with_capacity(rows);
    for r in 0..rows {
        validity.push(r % 7 != 0);
    }
    let validity: Bitmap = validity.into();
    let nullable_arr_str = arr_str.wrap_nullable(Some(validity));
    let bin = Column::Binary(build_binary_col(rows));
    let dec = Column::Decimal(DecimalColumn::Decimal128(
        Buffer::from((0..rows as i128).collect::<Vec<_>>()),
        DecimalSize::default_128(),
    ));
    let tuple = Column::Tuple(vec![
        Int64Type::from_data((0..rows as i64).collect::<Vec<_>>()),
        build_array_i64(rows, 2),
    ]);
    let vec_col = Column::Vector(VectorColumn::Float32((
        Buffer::from(
            (0..rows * 2)
                .map(|i| F32::from(i as f32))
                .collect::<Vec<_>>(),
        ),
        2,
    )));

    DataBlock::new_with_meta(
        vec![
            BlockEntry::Column(ids),
            BlockEntry::Column(long_strs),
            BlockEntry::Column(inline_strs),
            BlockEntry::Column(arr_i64),
            BlockEntry::Column(nullable_arr_str),
            BlockEntry::Column(bin),
            BlockEntry::Column(dec),
            BlockEntry::Column(tuple),
            BlockEntry::Column(vec_col),
        ],
        rows,
        Some(Box::new(DummyMeta(42))),
    )
}

#[test]
fn test_maybe_gc_releases_all_sliced_column_types_and_preserves_meta() {
    let total_rows = 6 * 1024;
    let slice_start = 2 * 1024;
    let slice_len = 1024;

    let parent = build_mixed_block(total_rows);
    let sliced = parent.slice(slice_start..slice_start + slice_len);
    drop(parent);

    // Before maybe_gc: sliced columns still retain parent backing storage.
    let Column::Number(NumberColumn::Int64(ids_before)) =
        sliced.get_by_offset(0).as_column().unwrap()
    else {
        unreachable!()
    };
    assert_eq!(ids_before.backing_len(), total_rows);

    let Column::String(inline_before) = sliced.get_by_offset(2).as_column().unwrap() else {
        unreachable!()
    };
    assert_eq!(inline_before.views().backing_len(), total_rows);

    let Column::Array(arr_i64_before) = sliced.get_by_offset(3).as_column().unwrap() else {
        unreachable!()
    };
    assert_eq!(arr_i64_before.values().len(), total_rows * 4);
    assert_eq!(arr_i64_before.offsets().backing_len(), total_rows + 1);

    let Column::Nullable(nullable_before) = sliced.get_by_offset(4).as_column().unwrap() else {
        unreachable!()
    };
    let Column::Array(arr_str_before) = nullable_before.column() else {
        unreachable!()
    };
    let Column::String(arr_str_vals_before) = arr_str_before.values() else {
        unreachable!()
    };
    let orig_arr_str_buf_len = arr_str_vals_before.total_buffer_len();
    assert_eq!(arr_str_before.values().len(), total_rows * 2);

    let expected = sliced.clone();
    let gced = sliced.maybe_gc();

    // Logical equality and metadata preserved.
    assert_eq!(gced.num_rows(), slice_len);
    for col_idx in 0..gced.num_columns() {
        assert_eq!(
            gced.get_by_offset(col_idx),
            expected.get_by_offset(col_idx),
            "column {col_idx} data mismatch after maybe_gc"
        );
    }
    let meta = gced.get_meta().expect("maybe_gc must preserve block meta");
    assert_eq!(DummyMeta::downcast_ref_from(meta), Some(&DummyMeta(42)));

    // Verify backing storage compacted across every column type.
    let Column::Number(NumberColumn::Int64(ids)) = gced.get_by_offset(0).as_column().unwrap()
    else {
        unreachable!()
    };
    assert_eq!(ids.backing_len(), slice_len);

    let Column::String(long_s) = gced.get_by_offset(1).as_column().unwrap() else {
        unreachable!()
    };
    assert_eq!(long_s.views().backing_len(), slice_len);
    assert!(long_s.total_buffer_len() <= long_s.total_bytes_len());

    let Column::String(inline_s) = gced.get_by_offset(2).as_column().unwrap() else {
        unreachable!()
    };
    assert_eq!(inline_s.views().backing_len(), slice_len);

    let Column::Array(arr_i64) = gced.get_by_offset(3).as_column().unwrap() else {
        unreachable!()
    };
    assert_eq!(arr_i64.values().len(), slice_len * 4);
    assert_eq!(arr_i64.offsets().backing_len(), slice_len + 1);
    assert_eq!(*arr_i64.offsets().first().unwrap(), 0);

    let Column::Nullable(nullable_after) = gced.get_by_offset(4).as_column().unwrap() else {
        unreachable!()
    };
    let Column::Array(arr_str_after) = nullable_after.column() else {
        unreachable!()
    };
    assert_eq!(arr_str_after.values().len(), slice_len * 2);
    assert_eq!(arr_str_after.offsets().backing_len(), slice_len + 1);
    let Column::String(arr_str_vals_after) = arr_str_after.values() else {
        unreachable!()
    };
    assert!(arr_str_vals_after.total_buffer_len() * 4 < orig_arr_str_buf_len);

    let Column::Binary(bin) = gced.get_by_offset(5).as_column().unwrap() else {
        unreachable!()
    };
    assert_eq!(bin.data().backing_len(), bin.total_bytes_len());
    assert_eq!(bin.offsets().backing_len(), slice_len + 1);

    let Column::Decimal(DecimalColumn::Decimal128(dec, _)) =
        gced.get_by_offset(6).as_column().unwrap()
    else {
        unreachable!()
    };
    assert_eq!(dec.backing_len(), slice_len);

    let Column::Tuple(fields) = gced.get_by_offset(7).as_column().unwrap() else {
        unreachable!()
    };
    let Column::Number(NumberColumn::Int64(t0)) = &fields[0] else {
        unreachable!()
    };
    let Column::Array(t1) = &fields[1] else {
        unreachable!()
    };
    assert_eq!(t0.backing_len(), slice_len);
    assert_eq!(t1.values().len(), slice_len * 2);

    let Column::Vector(VectorColumn::Float32((vbuf, _))) =
        gced.get_by_offset(8).as_column().unwrap()
    else {
        unreachable!()
    };
    assert_eq!(vbuf.backing_len(), slice_len * 2);
}

#[test]
fn test_maybe_gc_compacts_large_sliced_bitmap() {
    let total_bits = 256 * 1024; // 32 KiB backing buffer
    let slice_bits = 32 * 1024; // 4 KiB live slice (28 KiB savings >= 16 KiB)

    let mut builder = MutableBitmap::with_capacity(total_bits);
    for i in 0..total_bits {
        builder.push(i % 3 != 0);
    }
    let parent: Bitmap = builder.into();
    let sliced = parent.sliced(64 * 1024, slice_bits);
    assert_eq!(sliced.backing_bytes_len(), total_bits / 8);

    let expected = sliced.clone();
    let gced = Column::Boolean(sliced).maybe_gc();
    let Column::Boolean(bitmap) = gced else {
        unreachable!()
    };
    assert_eq!(bitmap, expected);
    assert_eq!(bitmap.backing_bytes_len(), slice_bits / 8);
}

#[test]
fn test_maybe_gc_blocks_preserves_zero_copy_when_all_live_and_compacts_sparse() {
    let slice_len = 4096;
    let num_slices = 6;
    let total_rows = slice_len * num_slices;

    // 1. All 6 slices live -> zero-copy (exact same backing pointers).
    let parent = build_mixed_block(total_rows);
    let slices = parent.split_by_rows_no_tail(slice_len);
    drop(parent);

    let Column::Number(NumberColumn::Int64(orig_ids)) =
        slices[0].get_by_offset(0).as_column().unwrap()
    else {
        unreachable!()
    };
    let orig_ids_ptr = orig_ids.backing_ptr();

    let kept_all = DataBlock::maybe_gc_blocks(slices);
    for block in &kept_all {
        let Column::Number(NumberColumn::Int64(ids)) = block.get_by_offset(0).as_column().unwrap()
        else {
            unreachable!()
        };
        assert_eq!(ids.backing_ptr(), orig_ids_ptr);
        assert_eq!(ids.backing_len(), total_rows);
    }

    // 2. Drop 4 of 6 slices -> remaining 2 slices compact and preserve meta.
    let mut survivors = kept_all;
    survivors.truncate(2);
    let compacted = DataBlock::maybe_gc_blocks(survivors);
    for block in &compacted {
        let meta = block
            .get_meta()
            .expect("maybe_gc_blocks must preserve block meta");
        assert_eq!(DummyMeta::downcast_ref_from(meta), Some(&DummyMeta(42)));

        let Column::Number(NumberColumn::Int64(ids)) = block.get_by_offset(0).as_column().unwrap()
        else {
            unreachable!()
        };
        assert_eq!(ids.backing_len(), slice_len);

        let Column::String(long_s) = block.get_by_offset(1).as_column().unwrap() else {
            unreachable!()
        };
        assert_eq!(long_s.views().backing_len(), slice_len);
        assert!(long_s.total_buffer_len() <= long_s.total_bytes_len());

        let Column::Array(arr) = block.get_by_offset(3).as_column().unwrap() else {
            unreachable!()
        };
        assert_eq!(arr.values().len(), slice_len * 4);
        assert_eq!(arr.offsets().backing_len(), slice_len + 1);
    }

    // 3. 2-slice block with 1.56x ratio (6400 -> 4096, 36% dead, >= 16 KiB savings) compacts cleanly.
    let parent2 = build_mixed_block(6400);
    let mut two_slices = parent2.split_by_rows_no_tail(slice_len);
    drop(parent2);
    two_slices.truncate(1);
    let compacted2 = DataBlock::maybe_gc_blocks(two_slices);
    let Column::Number(NumberColumn::Int64(ids)) =
        compacted2[0].get_by_offset(0).as_column().unwrap()
    else {
        unreachable!()
    };
    assert_eq!(ids.backing_len(), slice_len);
    let Column::Array(arr) = compacted2[0].get_by_offset(3).as_column().unwrap() else {
        unreachable!()
    };
    assert_eq!(arr.values().len(), slice_len * 4);
}

#[test]
fn test_maybe_gc_is_noop_for_fresh_short_out_of_line_strings() {
    // 13-byte strings are the worst case for BinaryViewColumn's inline-length lower bound
    // (13 - 12 = 1 byte). A freshly built column must never trigger redundant GC.
    let col = StringType::from_data(vec!["1234567890123".to_string(); 8192]);
    let Column::String(before) = col.clone() else {
        unreachable!()
    };
    let orig_ptr = before.views().as_ptr();
    drop(before);

    let Column::String(after) = col.maybe_gc() else {
        unreachable!()
    };
    assert_eq!(after.views().as_ptr(), orig_ptr);
}
