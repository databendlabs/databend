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

use std::ops::Deref;
use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NullableType;
use databend_common_expression::with_join_hash_method;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::memory::basic::BasicHashJoin;
use crate::pipelines::processors::transforms::memory::basic_state::SCAN_ROW_MARK_NULL;
use crate::pipelines::processors::transforms::memory::basic_state::SCAN_ROW_MATCHED;
use crate::pipelines::processors::transforms::memory::basic_state::SCAN_ROW_UNMATCHED;
use crate::pipelines::processors::transforms::memory::left_join::final_result_block;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::ProbeData;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbeStream;
use crate::pipelines::processors::transforms::new_hash_join::hashtable::basic::ProbedRows;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::performance::PerformanceContext;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

pub struct RightMarkHashJoin {
    pub(crate) basic_hash_join: BasicHashJoin,
    pub(crate) desc: Arc<HashJoinDesc>,
    pub(crate) function_ctx: FunctionContext,
    pub(crate) basic_state: Arc<BasicHashJoinState>,
    pub(crate) performance_context: PerformanceContext,
}

impl RightMarkHashJoin {
    pub fn create(
        ctx: &QueryContext,
        function_ctx: FunctionContext,
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        state: Arc<BasicHashJoinState>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let block_size = settings.get_max_block_size()? as usize;
        let context = PerformanceContext::create(block_size, desc.clone(), function_ctx.clone());
        let basic_hash_join = BasicHashJoin::create(
            &settings,
            function_ctx.clone(),
            method,
            desc.clone(),
            state.clone(),
            0,
        )?;

        Ok(RightMarkHashJoin {
            desc,
            basic_hash_join,
            function_ctx,
            basic_state: state,
            performance_context: context,
        })
    }

    fn update_build_has_null(&self, chunk: &DataBlock) -> Result<()> {
        let keys = self.desc.build_key(chunk, &self.function_ctx)?;
        if keys.is_empty() {
            return Ok(());
        }

        if keys.iter().any(|entry| {
            entry
                .as_column()
                .unwrap()
                .validity()
                .1
                .is_some_and(|validity| validity.null_count() > 0)
        }) {
            let mut has_null = self.desc.marker_join_desc.has_null.write();
            *has_null = true;
        }

        Ok(())
    }
}

impl Join for RightMarkHashJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        if let Some(data) = data.as_ref() {
            self.update_build_has_null(data)?;
        }

        self.basic_hash_join.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.basic_hash_join.final_build::<false>()
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        let row_count = data.num_rows();
        let probe_block = data.clone().project(&self.desc.probe_projection);
        let mut markers = vec![SCAN_ROW_UNMATCHED; row_count];

        if row_count == 0 || *self.basic_state.build_rows == 0 {
            let has_null = *self.desc.marker_join_desc.has_null.read();
            let marker_block = Some(create_marker_block(has_null, &markers));
            return Ok(Box::new(OneBlockJoinStream(Some(final_result_block(
                &self.desc,
                Some(probe_block),
                marker_block,
                row_count,
            )))));
        }

        self.basic_hash_join.finalize_chunks();
        let probe_keys = self.desc.probe_key(&data, &self.function_ctx)?;
        let mut probe_keys = DataBlock::new(probe_keys, data.num_rows());
        init_markers(
            ProjectedBlock::from(probe_keys.columns()),
            row_count,
            &mut markers,
        );
        let valids = self.desc.build_valids_by_keys(&probe_keys)?;
        self.desc.remove_keys_nullable(&mut probe_keys);

        let probe_stream = with_join_hash_method!(|T| match self.basic_state.hash_table.deref() {
            HashJoinHashTable::T(table) => {
                let probe_hash_statistics = &mut self.performance_context.probe_hash_statistics;
                probe_hash_statistics.clear(probe_block.num_rows());
                let probe_data = ProbeData::new(probe_keys, valids, probe_hash_statistics);
                table.probe_matched(probe_data)
            }
            HashJoinHashTable::NestedLoop(_) => unreachable!(),
            HashJoinHashTable::Null => Err(ErrorCode::AbortedQuery(
                "Aborted query, because the hash table is uninitialized.",
            )),
        })?;

        Ok(RightMarkHashJoinStream::create(
            probe_block,
            self.basic_state.clone(),
            probe_stream,
            self.desc.clone(),
            self.function_ctx.clone(),
            &mut self.performance_context.probe_result,
            markers,
        ))
    }
}

struct RightMarkHashJoinStream<'a> {
    desc: Arc<HashJoinDesc>,
    function_ctx: FunctionContext,
    probe_data_block: Option<DataBlock>,
    join_state: Arc<BasicHashJoinState>,
    probe_keys_stream: Box<dyn ProbeStream + 'a>,
    probed_rows: &'a mut ProbedRows,
    markers: Vec<u8>,
}

unsafe impl<'a> Send for RightMarkHashJoinStream<'a> {}
unsafe impl<'a> Sync for RightMarkHashJoinStream<'a> {}

impl<'a> RightMarkHashJoinStream<'a> {
    pub fn create(
        probe_data_block: DataBlock,
        join_state: Arc<BasicHashJoinState>,
        probe_keys_stream: Box<dyn ProbeStream + 'a>,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        probed_rows: &'a mut ProbedRows,
        markers: Vec<u8>,
    ) -> Box<dyn JoinStream + 'a> {
        Box::new(RightMarkHashJoinStream {
            desc,
            function_ctx,
            join_state,
            probed_rows,
            probe_keys_stream,
            probe_data_block: Some(probe_data_block),
            markers,
        })
    }
}

impl<'a> JoinStream for RightMarkHashJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        let Some(probe_data_block) = self.probe_data_block.take() else {
            return Ok(None);
        };

        loop {
            self.probed_rows.clear();
            let max_rows = self.probed_rows.matched_probe.capacity();
            self.probe_keys_stream.advance(self.probed_rows, max_rows)?;

            if self.probed_rows.is_empty() {
                break;
            }
            if self.probed_rows.matched_probe.is_empty() {
                continue;
            }

            match self.desc.other_predicate.as_ref() {
                None => {
                    for probe_idx in &self.probed_rows.matched_probe {
                        self.markers[*probe_idx as usize] = SCAN_ROW_MATCHED;
                    }
                }
                Some(predicate) => {
                    let probe_block = match probe_data_block.num_columns() {
                        0 => None,
                        _ => Some(DataBlock::take(
                            &probe_data_block,
                            self.probed_rows.matched_probe.as_slice(),
                        )?),
                    };
                    let build_block = match self.join_state.columns.is_empty() {
                        true => None,
                        false => Some(DataBlock::take_column_vec(
                            self.join_state.columns.as_slice(),
                            self.join_state.column_types.as_slice(),
                            self.probed_rows.matched_build.as_slice(),
                        )),
                    };
                    let result_block = final_result_block(
                        &self.desc,
                        probe_block,
                        build_block,
                        self.probed_rows.matched_build.len(),
                    );
                    let filter = nullable_filter(&result_block, predicate, &self.function_ctx)?;
                    for (idx, probe_idx) in self.probed_rows.matched_probe.iter().enumerate() {
                        let marker = &mut self.markers[*probe_idx as usize];
                        if unsafe { !filter.validity.get_bit_unchecked(idx) } {
                            if *marker == SCAN_ROW_UNMATCHED {
                                *marker = SCAN_ROW_MARK_NULL;
                            }
                        } else if unsafe { filter.column.get_bit_unchecked(idx) } {
                            *marker = SCAN_ROW_MATCHED;
                        }
                    }
                }
            }
        }

        let has_null = *self.desc.marker_join_desc.has_null.read();
        let marker_block = Some(create_marker_block(has_null, &self.markers));
        Ok(Some(final_result_block(
            &self.desc,
            Some(probe_data_block),
            marker_block,
            self.markers.len(),
        )))
    }
}

pub(crate) fn init_markers(cols: ProjectedBlock, num_rows: usize, markers: &mut [u8]) {
    if !cols
        .iter()
        .any(|entry| entry.data_type().is_nullable_or_null())
    {
        return;
    }

    let mut valids = None;
    for entry in cols.iter() {
        match entry.to_column() {
            Column::Nullable(column) => {
                let bitmap = &column.validity;
                if bitmap.null_count() == 0 {
                    return;
                }
                valids =
                    databend_common_expression::arrow::or_validities(valids, Some(bitmap.clone()));
            }
            Column::Null { .. } => {}
            _ => return,
        }
    }

    if let Some(valids) = valids {
        for (idx, marker) in markers.iter_mut().enumerate().take(num_rows) {
            if !valids.get_bit(idx) {
                *marker = SCAN_ROW_MARK_NULL;
            }
        }
    }
}

pub(crate) fn create_marker_block(has_null: bool, markers: &[u8]) -> DataBlock {
    let mut validity = MutableBitmap::with_capacity(markers.len());
    let mut boolean_bitmap = MutableBitmap::with_capacity(markers.len());

    for marker in markers {
        let marker = if *marker == SCAN_ROW_UNMATCHED && has_null {
            SCAN_ROW_MARK_NULL
        } else {
            *marker
        };
        validity.push(marker != SCAN_ROW_MARK_NULL);
        boolean_bitmap.push(marker == SCAN_ROW_MATCHED);
    }

    let boolean_column = Column::Boolean(boolean_bitmap.into());
    let marker_column = NullableColumn::new_column(boolean_column, validity.into());
    DataBlock::new_from_columns(vec![marker_column])
}

pub(crate) fn nullable_filter(
    block: &DataBlock,
    predicate: &databend_common_expression::Expr,
    function_ctx: &FunctionContext,
) -> Result<NullableColumn<BooleanType>> {
    let evaluator = Evaluator::new(block, function_ctx, &BUILTIN_FUNCTIONS);
    let filter_vector = evaluator
        .run(predicate)?
        .convert_to_full_column(predicate.data_type(), block.num_rows());

    match filter_vector {
        Column::Nullable(_) => {
            Ok(NullableType::<BooleanType>::try_downcast_column(&filter_vector).unwrap())
        }
        other => {
            let validity = Bitmap::new_constant(true, other.len());
            Ok(NullableColumn::new(other, validity).try_downcast().unwrap())
        }
    }
}
