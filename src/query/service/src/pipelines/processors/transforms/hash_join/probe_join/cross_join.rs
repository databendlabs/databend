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
use databend_common_expression::ChunkIndex;
use databend_common_expression::DataBlock;
use databend_common_expression::DataBlockVec;
use databend_common_expression::FilterExecutor;
use databend_common_pipeline::core::check_interrupt;

use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::PendingProbe;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::transforms::merge_with_constant_row;

/// A cursor over one probe block and the current build round. Build chunks remain
/// owned by the build state; only a bounded window, never an unbounded
/// build concatenation or Cartesian product, may be retained.
pub(crate) struct CrossJoinProbe {
    probe: DataBlock,
    build_block_index: usize,
    build_row_index: usize,
    probe_offset: usize,
    remaining_build_rows: usize,
    // Shared by every probe row before advancing to the next build window.
    // Only small chunks need gathering; a single-chunk window is a slice.
    window: Option<DataBlock>,
}

impl CrossJoinProbe {
    fn new(probe: DataBlock, build_rows: usize) -> Self {
        Self {
            probe,
            build_block_index: 0,
            build_row_index: 0,
            probe_offset: 0,
            remaining_build_rows: build_rows,
            window: None,
        }
    }

    fn is_finished(&self, build_blocks: &[DataBlock]) -> bool {
        self.probe.is_empty()
            || (self.window.is_none() && self.build_block_index >= build_blocks.len())
    }

    fn advance_build(&mut self, build_blocks: &[DataBlock], count: usize) {
        self.build_row_index += count;
        self.remaining_build_rows -= count;
        if self.build_row_index == build_blocks[self.build_block_index].num_rows() {
            self.build_block_index += 1;
            self.build_row_index = 0;
        }
    }

    fn skip_empty_chunks(&mut self, build_blocks: &[DataBlock]) -> Result<()> {
        while self.build_block_index < build_blocks.len()
            && build_blocks[self.build_block_index].num_rows() == 0
        {
            check_interrupt()?;
            self.build_block_index += 1;
        }
        Ok(())
    }

    fn build_window(
        &mut self,
        build_blocks: &[DataBlock],
        build_projection: &BTreeSet<usize>,
        max_block_size: usize,
    ) -> Result<DataBlock> {
        let build = &build_blocks[self.build_block_index];
        let available = build.num_rows() - self.build_row_index;
        if available >= max_block_size
            || available == self.remaining_build_rows
            || u32::try_from(max_block_size).is_err()
        {
            // A window fits in this chunk. Keep its shared representation,
            // including a short final window; never gather a single range.
            let count = available.min(max_block_size);
            let block = build
                .slice(self.build_row_index..self.build_row_index + count)
                .project(build_projection);
            self.advance_build(build_blocks, count);
            return Ok(block);
        }

        // Many tiny chunks would otherwise cause tiny output blocks. Gather
        // only this window, once, and reuse it for all rows of this probe.
        let count = max_block_size.min(self.remaining_build_rows);
        let mut blocks = DataBlockVec::default();
        let mut indices = ChunkIndex::default();
        while indices.num_rows() < count {
            check_interrupt()?;
            self.skip_empty_chunks(build_blocks)?;
            let build = &build_blocks[self.build_block_index];
            let rows = (count - indices.num_rows()).min(build.num_rows() - self.build_row_index);
            // Both conversions fit: the window has <= u32::MAX rows and
            // every registered block is a nonempty slice within that window.
            let index = u32::try_from(blocks.block_rows().len()).unwrap();
            let len = u32::try_from(rows).unwrap();
            blocks.push(
                build
                    .slice(self.build_row_index..self.build_row_index + rows)
                    .project(build_projection),
            )?;
            indices.push_merge_range(index, 0, len);
            self.advance_build(build_blocks, rows);
        }
        Ok(blocks.take(&indices))
    }

    fn next_candidate(
        &mut self,
        build_blocks: &[DataBlock],
        build_projection: &BTreeSet<usize>,
        max_block_size: usize,
    ) -> Result<Option<DataBlock>> {
        check_interrupt()?;
        if self.is_finished(build_blocks) {
            return Ok(None);
        }
        if self.window.is_none() {
            self.skip_empty_chunks(build_blocks)?;
            if self.is_finished(build_blocks) {
                return Ok(None);
            }
            if self.probe.num_rows() == 1
                || self.probe.num_rows() < max_block_size.min(self.remaining_build_rows)
            {
                self.window =
                    Some(self.build_window(build_blocks, build_projection, max_block_size)?);
            }
        }
        if let Some(window) = self.window.take() {
            let count = window.num_rows();
            let block = DataBlock::from_iter(
                self.probe
                    .columns()
                    .iter()
                    .map(|entry| {
                        BlockEntry::Const(
                            entry.index(self.probe_offset).unwrap().to_owned(),
                            entry.data_type(),
                            count,
                        )
                    })
                    .chain(window.columns().iter().cloned()),
                count,
            );
            self.probe_offset += 1;
            if self.probe_offset == self.probe.num_rows() {
                self.probe_offset = 0;
            } else {
                self.window = Some(window);
            }
            return Ok(Some(block));
        }

        // A large probe is already a useful contiguous range. Broadcast one
        // build row instead of gathering the build or expanding probe rows.
        let count = max_block_size.min(self.probe.num_rows() - self.probe_offset);
        let probe = self
            .probe
            .slice(self.probe_offset..self.probe_offset + count);
        let build = build_blocks[self.build_block_index]
            .clone()
            .project(build_projection);
        let block = merge_with_constant_row(probe, &build, self.build_row_index);
        self.probe_offset += count;
        if self.probe_offset == self.probe.num_rows() {
            self.probe_offset = 0;
            self.advance_build(build_blocks, 1);
        }
        Ok(Some(block))
    }

    /// Each call evaluates at most one candidate batch of `max_block_size`
    /// pairs, so even a predicate rejecting every pair returns control to the
    /// processor between batches.
    fn next(
        &mut self,
        build_blocks: &[DataBlock],
        build_projection: &BTreeSet<usize>,
        filter: &mut Option<FilterExecutor>,
        max_block_size: usize,
    ) -> Result<Option<DataBlock>> {
        let Some(candidate) =
            self.next_candidate(build_blocks, build_projection, max_block_size)?
        else {
            return Ok(None);
        };
        let Some(filter) = filter else {
            return Ok(Some(candidate));
        };
        match filter.select(&candidate)? {
            0 => Ok(None),
            count if count == candidate.num_rows() => Ok(Some(candidate)),
            count => Ok(Some(
                candidate.take_with_optimize_size(&filter.true_selection()[..count])?,
            )),
        }
    }
}

impl HashJoinProbeState {
    pub(crate) fn cross_join(
        &self,
        input: DataBlock,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let build_rows = unsafe {
            (*self.hash_join_state.build_state.get())
                .generation_state
                .build_num_rows
        };
        let cursor = CrossJoinProbe::new(input.project(&self.probe_projections), build_rows);
        self.next_cross_join(cursor, probe_state)
    }

    pub(crate) fn next_cross_join(
        &self,
        mut cursor: CrossJoinProbe,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        // The probe/build barriers keep these chunks immutable until every
        // probe cursor in this spill round has been exhausted.
        let build_state = unsafe { &*self.hash_join_state.build_state.get() };
        let build_blocks = &build_state.generation_state.chunks;
        let output = cursor.next(
            build_blocks,
            &self.build_projections,
            &mut probe_state.filter_executor,
            probe_state.max_block_size,
        )?;
        if !cursor.is_finished(build_blocks) {
            probe_state.pending = Some(PendingProbe::Cross(cursor));
        }
        Ok(output.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Column;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::Constant;
    use databend_common_expression::Expr;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::BooleanType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::DictColumn;
    use databend_common_expression::types::EncodedColumn;
    use databend_common_expression::types::ForColumn;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::number::NumberScalar;
    use databend_common_functions::BUILTIN_FUNCTIONS;

    use super::*;

    #[test]
    fn cursor_handles_empty_chunks_and_switches_broadcast_side() -> Result<()> {
        // Chunk sizes deliberately switch the broadcast direction. The probe
        // can also be larger than the per-call budget, unlike normal SQL input.
        let build = [vec![], vec![0, 1], vec![2, 3, 4, 5, 6], vec![], vec![7]]
            .map(|values| DataBlock::new_from_columns(vec![Int32Type::from_data(values)]));
        for max_block_size in [1, 3, 7] {
            let probe = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![0, 1, 2])]);
            let mut cursor = CrossJoinProbe::new(probe, 8);
            let mut actual = Vec::new();
            while !cursor.is_finished(&build) {
                if let Some(block) =
                    cursor.next(&build, &BTreeSet::from([0]), &mut None, max_block_size)?
                {
                    assert!(block.num_rows() <= max_block_size);
                    for row in 0..block.num_rows() {
                        actual.push(format!(
                            "{:?}:{:?}",
                            block.get_by_offset(0).index(row),
                            block.get_by_offset(1).index(row)
                        ));
                    }
                }
            }
            let probe = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![0, 1, 2])]);
            let build = DataBlock::new_from_columns(vec![Int32Type::from_data((0..8).collect())]);
            let mut expected = Vec::new();
            for p in 0..3 {
                for b in 0..8 {
                    expected.push(format!(
                        "{:?}:{:?}",
                        probe.get_by_offset(0).index(p),
                        build.get_by_offset(0).index(b)
                    ));
                }
            }
            actual.sort();
            expected.sort();
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    fn int_buffer(block: &DataBlock, offset: usize) -> databend_common_column::buffer::Buffer<i32> {
        Int32Type::try_downcast_column(block.get_by_offset(offset).as_column().unwrap()).unwrap()
    }

    #[test]
    fn unfiltered_single_chunk_preserves_shared_columns() -> Result<()> {
        let build = [DataBlock::new_from_columns(vec![Int32Type::from_data(
            (0..4096).collect(),
        )])];
        let source = int_buffer(&build[0], 0);
        let probe = DataBlock::new_from_columns(vec![Int32Type::from_data((0..32).collect())]);
        let mut cursor = CrossJoinProbe::new(probe, 4096);
        for _ in 0..32 {
            let block = cursor
                .next(&build, &BTreeSet::from([0]), &mut None, 65536)?
                .unwrap();
            assert_eq!(block.num_rows(), 4096);
            assert!(block.get_by_offset(0).as_scalar().is_some());
            assert_eq!(
                int_buffer(&block, 1).as_slice().as_ptr(),
                source.as_slice().as_ptr()
            );
        }
        assert!(cursor.is_finished(&build));
        Ok(())
    }

    #[test]
    fn unfiltered_tiny_chunks_reuse_one_gathered_window() -> Result<()> {
        let build = (0..4096)
            .map(|value| DataBlock::new_from_columns(vec![Int32Type::from_data(vec![value])]))
            .collect::<Vec<_>>();
        let probe = DataBlock::new_from_columns(vec![Int32Type::from_data((0..32).collect())]);
        let mut cursor = CrossJoinProbe::new(probe, 4096);
        let first = cursor
            .next(&build, &BTreeSet::from([0]), &mut None, 65536)?
            .unwrap();
        assert_eq!(first.num_rows(), 4096);
        let window = int_buffer(&first, 1);
        assert_eq!(window.as_slice(), (0..4096).collect::<Vec<_>>());
        for _ in 1..32 {
            let block = cursor
                .next(&build, &BTreeSet::from([0]), &mut None, 65536)?
                .unwrap();
            assert_eq!(block.num_rows(), 4096);
            assert!(block.get_by_offset(0).as_scalar().is_some());
            assert_eq!(
                int_buffer(&block, 1).as_slice().as_ptr(),
                window.as_slice().as_ptr()
            );
        }
        assert!(cursor.is_finished(&build));
        Ok(())
    }

    fn int_value(block: &DataBlock, column: usize, row: usize) -> i32 {
        match block.get_by_offset(column).index(row).unwrap() {
            ScalarRef::Number(NumberScalar::Int32(value)) => value,
            value => panic!("unexpected value {value:?}"),
        }
    }

    fn probe_less_than_build() -> Result<FilterExecutor> {
        let column = |id| {
            Expr::ColumnRef(ColumnRef {
                span: None,
                id,
                data_type: DataType::Number(NumberDataType::Int32),
                display_name: format!("c{id}"),
            })
        };
        let predicate =
            check_function(None, "lt", &[], &[column(0), column(1)], &BUILTIN_FUNCTIONS)?;
        Ok(FilterExecutor::new(
            predicate,
            FunctionContext::default(),
            8,
            None,
            &BUILTIN_FUNCTIONS,
            false,
        ))
    }

    #[test]
    fn windows_visit_every_pair_once() -> Result<()> {
        for pattern in 0..256 {
            let mut offset = 0;
            let build = (0..4)
                .map(|chunk| {
                    let count = (pattern >> (chunk * 2)) & 3;
                    let block = DataBlock::new_from_columns(vec![Int32Type::from_data(
                        (offset..offset + count).collect(),
                    )]);
                    offset += count;
                    block
                })
                .collect::<Vec<_>>();
            for probe_rows in 0..8 {
                for budget in 1..7 {
                    for filtered in [false, true] {
                        let probe = DataBlock::new_from_columns(vec![Int32Type::from_data(
                            (0..probe_rows).collect(),
                        )]);
                        let mut filter = filtered.then(probe_less_than_build).transpose()?;
                        let mut cursor = CrossJoinProbe::new(probe, offset as usize);
                        let mut actual = Vec::new();
                        let mut calls = 0;
                        while !cursor.is_finished(&build) {
                            if let Some(block) =
                                cursor.next(&build, &BTreeSet::from([0]), &mut filter, budget)?
                            {
                                assert!(block.num_rows() <= budget);
                                block.check_valid()?;
                                actual.extend((0..block.num_rows()).map(|row| {
                                    (int_value(&block, 0, row), int_value(&block, 1, row))
                                }));
                            }
                            calls += 1;
                            assert!(calls <= (probe_rows * offset + 1) as usize);
                        }
                        actual.sort_unstable();
                        let expected = (0..probe_rows)
                            .flat_map(|p| (0..offset).map(move |b| (p, b)))
                            .filter(|(p, b)| !filtered || p < b)
                            .collect::<Vec<_>>();
                        assert_eq!(
                            actual, expected,
                            "pattern={pattern}, P={probe_rows}, K={budget}, filtered={filtered}"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn unfiltered_window_preserves_encoding_and_gathers_mixed_types() -> Result<()> {
        let numbers = Int32Type::from_data((0..9).collect());
        let numbers = Column::Encoded(Box::new(EncodedColumn::For(
            ForColumn::try_encode(&numbers).unwrap(),
        )));
        let strings = StringType::from_data(vec!["long value".repeat(128); 9]);
        let strings = Column::Encoded(Box::new(EncodedColumn::Dict(
            DictColumn::try_encode(&strings, 1).unwrap(),
        )));
        let mut source = DataBlock::new_from_columns(vec![
            numbers,
            Int32Type::from_opt_data(vec![
                Some(0),
                None,
                Some(2),
                None,
                Some(4),
                None,
                Some(6),
                None,
                Some(8),
            ]),
            strings,
            BooleanType::from_data(vec![
                true, false, true, false, true, false, true, false, true,
            ]),
        ]);
        source.add_const_column(
            Scalar::Null,
            DataType::Nullable(Box::new(DataType::Boolean)),
        );
        for chunks in [
            vec![source.clone()],
            (0..9).map(|i| source.slice(i..i + 1)).collect(),
        ] {
            let probe = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![0, 1])]);
            let mut cursor = CrossJoinProbe::new(probe, 9);
            let mut total = 0;
            while !cursor.is_finished(&chunks) {
                let Some(block) =
                    cursor.next(&chunks, &BTreeSet::from([0, 1, 2, 3, 4]), &mut None, 16)?
                else {
                    continue;
                };
                block.check_valid()?;
                assert_eq!(block.num_rows(), 9);
                assert!(block.get_by_offset(0).as_scalar().is_some());
                if chunks.len() == 1 {
                    assert!(matches!(
                        block.get_by_offset(1).as_column(),
                        Some(Column::Encoded(_))
                    ));
                    assert!(matches!(
                        block.get_by_offset(3).as_column(),
                        Some(Column::Encoded(_))
                    ));
                }
                assert_eq!(block.get_by_offset(5).as_scalar(), Some(&Scalar::Null));
                for row in 0..9 {
                    for column in 0..5 {
                        assert_eq!(
                            block.get_by_offset(column + 1).index(row),
                            source.get_by_offset(column).index(row)
                        );
                    }
                }
                total += block.num_rows();
            }
            assert_eq!(total, 18);
        }
        Ok(())
    }

    #[test]
    fn dropping_unfiltered_cursor_releases_gathered_window() -> Result<()> {
        let build = (0..9)
            .map(|i| DataBlock::new_from_columns(vec![Int32Type::from_data(vec![i])]))
            .collect::<Vec<_>>();
        let probe = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![0, 1])]);
        let mut cursor = CrossJoinProbe::new(probe, 9);
        let block = cursor
            .next(&build, &BTreeSet::from([0]), &mut None, 16)?
            .unwrap();
        let mut window = int_buffer(&block, 1);
        drop(block);
        assert!(window.get_mut_slice().is_none());
        drop(cursor);
        assert!(window.get_mut_slice().is_some());
        Ok(())
    }

    #[test]
    fn rejected_candidates_yield_without_exhausting_the_input() -> Result<()> {
        let probe = DataBlock::new(vec![], 5);
        let build = [DataBlock::new(vec![], 11)];
        for scalar in [Scalar::Boolean(false), Scalar::Null] {
            let data_type = if scalar.is_null() {
                DataType::Nullable(Box::new(DataType::Boolean))
            } else {
                DataType::Boolean
            };
            let mut filter = Some(FilterExecutor::new(
                Expr::Constant(Constant {
                    span: None,
                    scalar,
                    data_type,
                }),
                FunctionContext::default(),
                3,
                None,
                &BUILTIN_FUNCTIONS,
                false,
            ));
            let mut cursor = CrossJoinProbe::new(probe.clone(), 11);
            let mut batches = 0;
            while !cursor.is_finished(&build) {
                assert!(
                    cursor
                        .next(&build, &BTreeSet::new(), &mut filter, 3)?
                        .is_none()
                );
                batches += 1;
            }
            // Each build row pairs with probe slices of 3 and 2 rows.
            assert_eq!(batches, 11 * 2);
        }
        Ok(())
    }
}
