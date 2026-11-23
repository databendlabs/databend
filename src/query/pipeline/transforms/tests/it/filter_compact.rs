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

use std::collections::BTreeSet;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::port::connect;
use databend_common_pipeline_transforms::filters::TransformFilter;

fn build_oversized_block(flags: Vec<i64>) -> DataBlock {
    let rows = flags.len();
    let flag_col = Int64Type::from_data(flags);
    let long_str = StringType::from_data(
        (0..rows)
            .map(|r| format!("row_{r:08}_000000000000000000000000000000"))
            .collect::<Vec<_>>(),
    );

    let mut arr_offsets = Vec::with_capacity(rows + 1);
    arr_offsets.push(0u64);
    let mut arr_vals = Vec::with_capacity(rows * 4);
    for r in 0..rows {
        for e in 0..4 {
            arr_vals.push((r * 10 + e) as i64);
        }
        arr_offsets.push(arr_vals.len() as u64);
    }
    let arr_i64 = Column::Array(Box::new(ArrayColumn::new(
        Int64Type::from_data(arr_vals),
        arr_offsets.into(),
    )));

    let mut str_arr_builder = ArrayColumnBuilder::<AnyType> {
        builder: ColumnBuilder::with_capacity(&DataType::String, rows * 2),
        offsets: {
            let mut o = Vec::with_capacity(rows + 1);
            o.push(0);
            o
        },
    };
    for r in 0..rows {
        let col = StringType::from_data(vec![
            format!("one_id_a_{r:08}_padding_bytes"),
            format!("one_id_b_{r:08}_padding_bytes"),
        ]);
        str_arr_builder.push(col);
    }
    let arr_str = Column::Array(Box::new(str_arr_builder.build()));

    DataBlock::new(
        vec![
            BlockEntry::Column(flag_col),
            BlockEntry::Column(long_str),
            BlockEntry::Column(arr_i64),
            BlockEntry::Column(arr_str),
        ],
        rows,
    )
}

fn run_transform_filter(
    input_block: DataBlock,
    max_block_size: usize,
    enable_selector_executor: bool,
) -> Result<Vec<DataBlock>> {
    let input_port = InputPort::create();
    let output_port = OutputPort::create();
    let upstream_out = OutputPort::create();
    let downstream_in = InputPort::create();
    unsafe {
        connect(&input_port, &upstream_out);
        connect(&downstream_in, &output_port);
    }

    // Predicate: col(0) == 1
    let col_ref = Expr::ColumnRef(ColumnRef {
        span: None,
        id: 0,
        data_type: DataType::Number(NumberDataType::Int64),
        display_name: "flag".to_string(),
    });
    let lit_one = Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(databend_common_expression::types::number::NumberScalar::Int64(1)),
        data_type: DataType::Number(NumberDataType::Int64),
    });
    let pred = check_function(None, "eq", &[], &[col_ref, lit_one], &BUILTIN_FUNCTIONS)?;
    let projections: BTreeSet<usize> = (0..input_block.num_columns()).collect();
    let func_ctx = FunctionContext {
        enable_selector_executor,
        ..Default::default()
    };

    let mut proc = TransformFilter::<false>::create(
        input_port,
        output_port,
        pred,
        projections,
        func_ctx,
        max_block_size,
    );

    let mut pending = Some(input_block);
    let mut results = Vec::new();
    downstream_in.set_need_data();

    loop {
        match proc.event()? {
            Event::NeedData => {
                if let Some(block) = pending.take() {
                    upstream_out.push_data(Ok(block));
                } else {
                    upstream_out.finish();
                }
            }
            Event::Sync => {
                proc.process()?;
            }
            Event::NeedConsume => {
                if downstream_in.has_data() {
                    results.push(downstream_in.pull_data().unwrap()?);
                }
                downstream_in.set_need_data();
            }
            Event::Finished => {
                while downstream_in.has_data() {
                    results.push(downstream_in.pull_data().unwrap()?);
                }
                break;
            }
            Event::Async => unreachable!("TransformFilter is synchronous"),
        }
    }

    Ok(results)
}

fn assert_all_columns_detached(blocks: &[DataBlock], parent_rows: usize) {
    for block in blocks {
        let n = block.num_rows();
        let Column::Number(NumberColumn::Int64(flags)) =
            block.get_by_offset(0).as_column().unwrap()
        else {
            unreachable!()
        };
        assert_eq!(flags.backing_len(), n);
        assert!(flags.iter().all(|&v| v == 1));

        let Column::String(long_s) = block.get_by_offset(1).as_column().unwrap() else {
            unreachable!()
        };
        assert_eq!(long_s.views().backing_len(), n);
        assert!(
            long_s.total_buffer_len() <= long_s.total_bytes_len(),
            "string buffers should not pin parent block: buf={} > live={}",
            long_s.total_buffer_len(),
            long_s.total_bytes_len(),
        );

        let Column::Array(arr_i64) = block.get_by_offset(2).as_column().unwrap() else {
            unreachable!()
        };
        assert_eq!(arr_i64.values().len(), n * 4);
        assert_eq!(arr_i64.offsets().backing_len(), n + 1);
        assert!(arr_i64.values().len() < parent_rows * 4);

        let Column::Array(arr_str) = block.get_by_offset(3).as_column().unwrap() else {
            unreachable!()
        };
        assert_eq!(arr_str.values().len(), n * 2);
        assert_eq!(arr_str.offsets().backing_len(), n + 1);
        let Column::String(inner_str) = arr_str.values() else {
            unreachable!()
        };
        assert!(inner_str.total_buffer_len() <= inner_str.total_bytes_len());
    }
}

#[test]
fn test_transform_filter_100_percent_pass_stays_zero_copy() -> Result<()> {
    let slice_len = 1024;
    let total_rows = 6 * slice_len;
    for selector in [true, false] {
        let block = build_oversized_block(vec![1; total_rows]);
        let outputs = run_transform_filter(block, slice_len, selector)?;
        assert_eq!(outputs.len(), 6);
        assert_eq!(
            outputs.iter().map(DataBlock::num_rows).sum::<usize>(),
            total_rows
        );
        // Zero-copy preserved: every slice still shares the parent 6144-row allocation.
        for out in &outputs {
            let Column::Number(NumberColumn::Int64(flags)) =
                out.get_by_offset(0).as_column().unwrap()
            else {
                unreachable!()
            };
            assert_eq!(flags.backing_len(), total_rows);
            let Column::Array(arr) = out.get_by_offset(2).as_column().unwrap() else {
                unreachable!()
            };
            assert_eq!(arr.values().len(), total_rows * 4);
        }
    }
    Ok(())
}

#[test]
fn test_transform_filter_clustered_p7_layout_compacts_surviving_slices() -> Result<()> {
    // 2 slices 100% pass + 4 slices 0% pass (33% selectivity, mirrors P7 production pattern).
    let slice_len = 1024;
    let total_rows = 6 * slice_len;
    for selector in [true, false] {
        let mut flags = vec![1; 2 * slice_len];
        flags.resize(total_rows, 0);
        let block = build_oversized_block(flags);
        let outputs = run_transform_filter(block, slice_len, selector)?;
        assert_eq!(outputs.len(), 2);
        assert_eq!(
            outputs.iter().map(DataBlock::num_rows).sum::<usize>(),
            2 * slice_len
        );
        assert_all_columns_detached(&outputs, total_rows);
    }
    Ok(())
}

#[test]
fn test_transform_filter_clustered_mtp_layout_compacts_surviving_slices() -> Result<()> {
    // 3 slices 100% pass + 1 slice 50% pass + 2 slices 0% pass (58% selectivity).
    let slice_len = 1024;
    let total_rows = 6 * slice_len;
    for selector in [true, false] {
        let mut flags = vec![1; 3 * slice_len];
        for i in 0..slice_len {
            flags.push(if i % 2 == 0 { 1 } else { 0 });
        }
        flags.resize(total_rows, 0);
        let block = build_oversized_block(flags);
        let outputs = run_transform_filter(block, slice_len, selector)?;
        assert_eq!(outputs.len(), 4);
        assert_eq!(
            outputs.iter().map(DataBlock::num_rows).sum::<usize>(),
            3 * slice_len + slice_len / 2
        );
        assert_all_columns_detached(&outputs, total_rows);
    }
    Ok(())
}

#[test]
fn test_transform_filter_mixed_one_full_and_five_79_percent_compacts_full_slice() -> Result<()> {
    // 1 slice 100% pass (pins parent) + 5 slices ~79% pass (freshly copied by take_with_optimize_size).
    // Even though total surviving rows are 82.5% (> 80%), only 16.7% still reference the parent
    // allocation; maybe_gc_blocks detects this per backing buffer and compacts only that 1 slice.
    let slice_len = 1024;
    let total_rows = 6 * slice_len;
    for selector in [true, false] {
        let mut flags = vec![1; slice_len];
        for _ in 0..5 {
            for i in 0..slice_len {
                flags.push(if i % 5 != 0 { 1 } else { 0 }); // 80% -> <= SELECTIVITY_THRESHOLD -> copied
            }
        }
        let block = build_oversized_block(flags);
        let outputs = run_transform_filter(block, slice_len, selector)?;
        assert_eq!(outputs.len(), 6);
        assert_all_columns_detached(&outputs, total_rows);
    }
    Ok(())
}

#[test]
fn test_transform_filter_sparse_take_ranges_compacts_string_buffers() -> Result<()> {
    // 2 slices 90% pass (take_ranges -> StringBuffersOnly) + 4 slices 0% pass (30% total).
    let slice_len = 1024;
    let total_rows = 6 * slice_len;
    for selector in [true, false] {
        let mut flags = Vec::with_capacity(total_rows);
        for _ in 0..2 {
            for i in 0..slice_len {
                flags.push(if i % 10 != 0 { 1 } else { 0 });
            }
        }
        flags.resize(total_rows, 0);
        let block = build_oversized_block(flags);
        let outputs = run_transform_filter(block, slice_len, selector)?;
        assert_eq!(outputs.len(), 2);
        assert_all_columns_detached(&outputs, total_rows);
    }
    Ok(())
}

#[test]
fn test_transform_filter_two_slice_block_detaches_below_2x_ratio() -> Result<()> {
    // 2 slices: 4096 pass (100%) + 2304 dropped (0%) = 64% live (1.56x backing/live, >= 16 KiB savings).
    // Even though 1.56x is below standalone maybe_gc's 2x/4x threshold, maybe_gc_blocks detaches cleanly.
    let slice_len = 4096;
    let total_rows = 6400;
    for selector in [true, false] {
        let mut flags = vec![1; slice_len];
        flags.resize(total_rows, 0);
        let block = build_oversized_block(flags);
        let outputs = run_transform_filter(block, slice_len, selector)?;
        assert_eq!(outputs.len(), 1);
        assert_all_columns_detached(&outputs, total_rows);
    }
    Ok(())
}

#[test]
fn test_transform_filter_dense_take_ranges_keeps_shared_string_buffers() -> Result<()> {
    // All 6 slices 90% pass -> 90% > 80% of string buffers are live; avoid redundant string copies.
    let slice_len = 1024;
    let total_rows = 6 * slice_len;
    for selector in [true, false] {
        let mut flags = Vec::with_capacity(total_rows);
        for _ in 0..6 {
            for i in 0..slice_len {
                flags.push(if i % 10 != 0 { 1 } else { 0 });
            }
        }
        let block = build_oversized_block(flags);
        let Column::String(orig_s) = block.get_by_offset(1).as_column().unwrap() else {
            unreachable!()
        };
        let orig_buf_len = orig_s.total_buffer_len();

        let outputs = run_transform_filter(block, slice_len, selector)?;
        assert_eq!(outputs.len(), 6);
        for out in &outputs {
            let Column::String(long_s) = out.get_by_offset(1).as_column().unwrap() else {
                unreachable!()
            };
            assert_eq!(
                long_s.total_buffer_len(),
                orig_buf_len,
                "90%-live parent string buffers should stay shared without redundant copy"
            );
        }
    }
    Ok(())
}
