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

use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableColumn;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::partitioned_build::PartitionedBuild;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::pipelines::processors::transforms::new_hash_join::join::OneBlockJoinStream;
use crate::pipelines::processors::transforms::new_hash_join::grace::grace_memory::GraceMemoryJoin;
use crate::pipelines::processors::transforms::wrap_true_validity;

pub struct PartitionedLeftJoin {
    build: PartitionedBuild,
    filter_executor: Option<FilterExecutor>,
}

impl PartitionedLeftJoin {
    pub fn create(
        method: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        function_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Self {
        let filter_executor = desc.other_predicate.as_ref().map(|predicate| {
            FilterExecutor::new(
                predicate.clone(),
                function_ctx.clone(),
                max_block_size,
                None,
                &BUILTIN_FUNCTIONS,
                false,
            )
        });
        PartitionedLeftJoin {
            build: PartitionedBuild::create(method, desc, function_ctx),
            filter_executor,
        }
    }
}

impl Join for PartitionedLeftJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.build.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.build.final_build()?;
        Ok(None)
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() {
            return Ok(Box::new(EmptyJoinStream));
        }

        let desc = &self.build.desc;

        if self.build.num_rows == 0 {
            let num_rows = data.num_rows();
            let types: Vec<_> = desc
                .build_schema
                .fields
                .iter()
                .map(|x| x.data_type().clone())
                .collect();
            let build_block = null_block(&types, num_rows)
                .map(|b| b.project(&desc.build_projection));
            let probe_block = Some(data.project(&desc.probe_projection));
            let result = final_result_block(desc, probe_block, build_block, num_rows);
            return Ok(Box::new(OneBlockJoinStream(Some(result))));
        }

        let (matched_probe, matched_build, unmatched) = self.build.probe(&data)?;
        let num_rows = data.num_rows();
        let probe_projected = data.project(&desc.probe_projection);

        // Build matched result
        let mut result_blocks = Vec::new();

        if !matched_probe.is_empty() {
            let probe_block = match probe_projected.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(&probe_projected, matched_probe.as_slice())?),
            };

            let build_block = self.build.gather_build_block(&matched_build);
            let build_block = build_block.map(|b| {
                let true_validity = Bitmap::new_constant(true, matched_build.len());
                let entries = b
                    .columns()
                    .iter()
                    .map(|c| wrap_true_validity(c, matched_build.len(), &true_validity));
                DataBlock::from_iter(entries, matched_build.len())
            });

            let mut matched_result = final_result_block(
                desc,
                probe_block,
                build_block,
                matched_build.len(),
            );

            if let Some(filter) = self.filter_executor.as_mut() {
                let count = filter.select(&matched_result)?;
                if count > 0 {
                    // Track which probe rows passed the filter
                    let true_sel = filter.true_selection();
                    let mut passed = vec![false; num_rows];
                    for idx in true_sel.iter().take(count) {
                        passed[matched_probe[*idx as usize] as usize] = true;
                    }

                    let origin_rows = matched_result.num_rows();
                    matched_result = filter.take(matched_result, origin_rows, count)?;
                    result_blocks.push(matched_result);

                    // Unmatched = original unmatched + matched rows that failed ALL filter checks
                    let mut all_unmatched: Vec<u64> = unmatched;
                    // Rows that were matched but never passed filter
                    let mut matched_set = vec![false; num_rows];
                    for idx in &matched_probe {
                        matched_set[*idx as usize] = true;
                    }
                    for i in 0..num_rows {
                        if matched_set[i] && !passed[i] {
                            all_unmatched.push(i as u64);
                        }
                    }

                    if !all_unmatched.is_empty() {
                        let unmatched_probe = match probe_projected.num_columns() {
                            0 => None,
                            _ => Some(DataBlock::take(&probe_projected, all_unmatched.as_slice())?),
                        };
                        let types = &self.build.column_types;
                        let unmatched_build = null_block(types, all_unmatched.len());
                        result_blocks.push(final_result_block(
                            desc,
                            unmatched_probe,
                            unmatched_build,
                            all_unmatched.len(),
                        ));
                    }
                } else {
                    // All matched rows failed filter, treat all as unmatched
                    let all_indices: Vec<u64> = (0..num_rows as u64).collect();
                    let unmatched_probe = match probe_projected.num_columns() {
                        0 => None,
                        _ => Some(DataBlock::take(&probe_projected, all_indices.as_slice())?),
                    };
                    let types = &self.build.column_types;
                    let unmatched_build = null_block(types, all_indices.len());
                    result_blocks.push(final_result_block(
                        desc,
                        unmatched_probe,
                        unmatched_build,
                        all_indices.len(),
                    ));
                }
            } else {
                result_blocks.push(matched_result);

                // Append unmatched rows with NULL build side
                if !unmatched.is_empty() {
                    let unmatched_probe = match probe_projected.num_columns() {
                        0 => None,
                        _ => Some(DataBlock::take(&probe_projected, unmatched.as_slice())?),
                    };
                    let types = &self.build.column_types;
                    let unmatched_build = null_block(types, unmatched.len());
                    result_blocks.push(final_result_block(
                        desc,
                        unmatched_probe,
                        unmatched_build,
                        unmatched.len(),
                    ));
                }
            }
        } else {
            // All rows unmatched
            let unmatched_probe = match probe_projected.num_columns() {
                0 => None,
                _ => Some(DataBlock::take(&probe_projected, unmatched.as_slice())?),
            };
            let types = &self.build.column_types;
            let unmatched_build = null_block(types, unmatched.len());
            result_blocks.push(final_result_block(
                desc,
                unmatched_probe,
                unmatched_build,
                unmatched.len(),
            ));
        }

        let result = DataBlock::concat(&result_blocks)?;
        Ok(Box::new(OneBlockJoinStream(Some(result))))
    }
}

impl GraceMemoryJoin for PartitionedLeftJoin {
    fn reset_memory(&mut self) {
        self.build.reset();
    }
}

pub fn final_result_block(
    desc: &HashJoinDesc,
    probe_block: Option<DataBlock>,
    build_block: Option<DataBlock>,
    num_rows: usize,
) -> DataBlock {
    let mut result_block = match (probe_block, build_block) {
        (Some(mut probe_block), Some(build_block)) => {
            probe_block.merge_block(build_block);
            probe_block
        }
        (Some(probe_block), None) => probe_block,
        (None, Some(build_block)) => build_block,
        (None, None) => DataBlock::new(vec![], num_rows),
    };

    if !desc.probe_to_build.is_empty() {
        for (index, (is_probe_nullable, is_build_nullable)) in desc.probe_to_build.iter() {
            let entry = match (is_probe_nullable, is_build_nullable) {
                (true, true) | (false, false) => result_block.get_by_offset(*index).clone(),
                (true, false) => result_block.get_by_offset(*index).clone().remove_nullable(),
                (false, true) => {
                    let entry = result_block.get_by_offset(*index);
                    let col = entry.to_column();
                    match col.is_null() || col.is_nullable() {
                        true => entry.clone(),
                        false => BlockEntry::from(NullableColumn::new_column(
                            col,
                            Bitmap::new_constant(true, result_block.num_rows()),
                        )),
                    }
                }
            };
            result_block.add_entry(entry);
        }
    }
    result_block
}

pub fn null_block(types: &[DataType], num_rows: usize) -> Option<DataBlock> {
    if types.is_empty() {
        return None;
    }
    let columns = types
        .iter()
        .map(|column_type| {
            BlockEntry::new_const_column(column_type.wrap_nullable(), Scalar::Null, num_rows)
        })
        .collect::<Vec<_>>();
    Some(DataBlock::new(columns, num_rows))
}
