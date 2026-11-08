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

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionID;
use databend_common_expression::RemoteExpr;
use databend_common_expression::group_hash_entry;
use databend_common_expression::group_hash_route_index;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::servers::flight::v1::scatter::flight_scatter::FlightScatter;

#[derive(Clone)]
pub struct HashFlightScatter {
    func_ctx: FunctionContext,
    hash_key: Vec<Expr>,
    scatter_size: usize,
}

impl HashFlightScatter {
    pub fn try_create(
        func_ctx: FunctionContext,
        hash_keys: Vec<RemoteExpr>,
        scatter_size: usize,
        local_pos: usize,
    ) -> Result<Box<dyn FlightScatter>> {
        if hash_keys.len() == 1 {
            return OneHashKeyFlightScatter::try_create(
                func_ctx,
                &hash_keys[0],
                scatter_size,
                local_pos,
            );
        }
        let hash_key = hash_keys
            .iter()
            .map(|key| key.as_expr(&BUILTIN_FUNCTIONS))
            .collect();

        Ok(Box::new(Self {
            func_ctx,
            scatter_size,
            hash_key,
        }))
    }
}

#[derive(Clone)]
struct OneHashKeyFlightScatter {
    scatter_size: usize,
    func_ctx: FunctionContext,
    hash_key: Expr,
    default_scatter_index: u64,
}

impl OneHashKeyFlightScatter {
    pub fn try_create(
        func_ctx: FunctionContext,
        hash_key: &RemoteExpr,
        scatter_size: usize,
        local_pos: usize,
    ) -> Result<Box<dyn FlightScatter>> {
        let default_scatter_index = if shuffle_by_block_id_in_merge_into(hash_key) {
            local_pos as u64
        } else {
            0
        };

        Ok(Box::new(OneHashKeyFlightScatter {
            scatter_size,
            func_ctx,
            hash_key: hash_key.as_expr(&BUILTIN_FUNCTIONS),
            default_scatter_index,
        }))
    }

    fn build_scatter_indices(&self, data_block: &DataBlock) -> Result<Vec<u64>> {
        let num_rows = data_block.num_rows();
        let evaluator = Evaluator::new(data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let entry = BlockEntry::new(evaluator.run(&self.hash_key)?, || {
            (self.hash_key.data_type().clone(), num_rows)
        });

        let validity = match &entry {
            BlockEntry::Const(scalar, _, _) if scalar.is_null() => {
                return Ok(vec![self.default_scatter_index; num_rows]);
            }
            BlockEntry::Const(..) => None,
            BlockEntry::Column(column) => match column.validity() {
                (true, _) => return Ok(vec![self.default_scatter_index; num_rows]),
                (false, validity) => validity.filter(|v| v.null_count() > 0).cloned(),
            },
        };

        let mut indices = vec![0; num_rows];
        group_hash_entry(&entry, &mut indices, true);

        let scatter_size = self.scatter_size as u64;
        match validity {
            None => {
                for index in &mut indices {
                    *index = group_hash_route_index(*index, scatter_size);
                }
            }
            Some(validity) => {
                for (index, valid) in indices.iter_mut().zip(validity.iter()) {
                    *index = if valid {
                        group_hash_route_index(*index, scatter_size)
                    } else {
                        self.default_scatter_index
                    };
                }
            }
        }
        Ok(indices)
    }
}

impl FlightScatter for OneHashKeyFlightScatter {
    fn name(&self) -> &'static str {
        "OneHashKey"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let indices = self.build_scatter_indices(&data_block)?;
        let data_blocks = DataBlock::scatter(&data_block, &indices, self.scatter_size)?;

        let block_meta = data_block.get_meta();
        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.cloned())?);
        }

        Ok(res)
    }

    fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
        Ok(Some(self.build_scatter_indices(data_block)?))
    }
}

impl FlightScatter for HashFlightScatter {
    fn name(&self) -> &'static str {
        "Hash"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let indices = self.build_scatter_indices(&data_block)?;

        let block_meta = data_block.get_meta();
        let data_blocks = DataBlock::scatter(&data_block, &indices, self.scatter_size)?;

        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.cloned())?);
        }

        Ok(res)
    }

    fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
        Ok(Some(self.build_scatter_indices(data_block)?))
    }
}

impl HashFlightScatter {
    fn build_scatter_indices(&self, data_block: &DataBlock) -> Result<Vec<u64>> {
        let num_rows = data_block.num_rows();
        if self.hash_key.is_empty() {
            return Ok(vec![0; num_rows]);
        }

        let evaluator = Evaluator::new(data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let mut hashes = vec![0; num_rows];
        for (index, expr) in self.hash_key.iter().enumerate() {
            let entry = BlockEntry::new(evaluator.run(expr)?, || {
                (expr.data_type().clone(), num_rows)
            });
            group_hash_entry(&entry, &mut hashes, index == 0);
        }

        let scatter_size = self.scatter_size as u64;
        for hash in &mut hashes {
            *hash = group_hash_route_index(*hash, scatter_size);
        }
        Ok(hashes)
    }
}

fn shuffle_by_block_id_in_merge_into(expr: &RemoteExpr) -> bool {
    if let RemoteExpr::FunctionCall {
        id: deref!(FunctionID::Builtin { name, .. }),
        args,
        ..
    } = expr
    {
        if name == "bit_and" {
            if let RemoteExpr::FunctionCall {
                id: deref!(FunctionID::Builtin { name, .. }),
                ..
            } = &args[0]
            {
                if name == "bit_shift_right" {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use databend_common_catalog::plan::NUM_ROW_ID_PREFIX_BITS;
    use databend_common_expression::ColumnRef;
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::type_check::check_function;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_expression::types::number::NumberScalar;
    use databend_storages_common_table_meta::meta::NUM_BLOCK_ID_BITS;

    use super::*;

    fn column_expr(id: usize, data_type: DataType) -> Expr {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id,
            data_type,
            display_name: format!("#{id}"),
        })
    }

    fn scatter(hash_key: Vec<Expr>, scatter_size: usize) -> Result<Box<dyn FlightScatter>> {
        let hash_key = hash_key.iter().map(Expr::as_remote_expr).collect();
        HashFlightScatter::try_create(FunctionContext::default(), hash_key, scatter_size, 0)
    }

    fn scatter_indices(
        hash_key: Vec<Expr>,
        scatter_size: usize,
        block: &DataBlock,
    ) -> Result<Vec<u64>> {
        Ok(scatter(hash_key, scatter_size)?
            .scatter_indices(block)?
            .unwrap())
    }

    fn block_hash_keys(block: &DataBlock) -> Vec<Expr> {
        block
            .columns()
            .iter()
            .enumerate()
            .map(|(index, entry)| column_expr(index, entry.data_type()))
            .collect()
    }

    fn assert_balanced(indices: &[u64], partitions: usize) {
        let mut counts = vec![0_usize; partitions];
        for &index in indices {
            counts[index as usize] += 1;
        }

        let max_partition = counts.iter().copied().max().unwrap_or_default();
        assert!(
            max_partition * partitions * 20 <= indices.len() * 21,
            "partition counts are imbalanced: {counts:?}"
        );
    }

    #[test]
    fn hashes_evaluated_and_materialized_keys_consistently() -> Result<()> {
        let text_keys = StringType::from_data(vec!["AbC", "DEF", "ghI", "Jkl"]);
        let normalized_text_keys = StringType::from_data(vec!["abc", "def", "ghi", "jkl"]);
        let numeric_keys = UInt64Type::from_data(vec![1, 2, 3, 4]);

        let raw_block = DataBlock::new_from_columns(vec![text_keys, numeric_keys.clone()]);
        let lower_block = DataBlock::new_from_columns(vec![normalized_text_keys, numeric_keys]);

        let lower_expr = check_function(
            None,
            "lower",
            &[],
            &[column_expr(0, DataType::String)],
            &BUILTIN_FUNCTIONS,
        )?;
        let evaluated_keys = vec![
            lower_expr,
            column_expr(1, DataType::Number(NumberDataType::UInt64)),
        ];
        let materialized_keys = block_hash_keys(&lower_block);

        for partitions in [3, 4, 8] {
            let evaluated = scatter_indices(evaluated_keys.clone(), partitions, &raw_block)?;
            let materialized =
                scatter_indices(materialized_keys.clone(), partitions, &lower_block)?;
            assert_eq!(evaluated, materialized);
        }
        Ok(())
    }

    #[test]
    fn hashes_scalar_and_materialized_keys_consistently() -> Result<()> {
        let ids = UInt64Type::from_data(vec![1, 2, 3, 4]);
        let scalar_block = DataBlock::new_from_columns(vec![ids.clone()]);
        let materialized_block =
            DataBlock::new_from_columns(vec![ids, UInt64Type::from_data(vec![7, 7, 7, 7])]);
        let scalar_keys = vec![
            column_expr(0, DataType::Number(NumberDataType::UInt64)),
            Expr::constant(
                Scalar::Number(NumberScalar::UInt64(7)),
                Some(DataType::Number(NumberDataType::UInt64)),
            ),
        ];

        for partitions in [3, 4, 8] {
            let scalar = scatter_indices(scalar_keys.clone(), partitions, &scalar_block)?;
            let materialized = scatter_indices(
                block_hash_keys(&materialized_block),
                partitions,
                &materialized_block,
            )?;
            assert_eq!(scalar, materialized);
        }
        Ok(())
    }

    #[test]
    fn hashes_nullable_keys_by_value_and_validity() -> Result<()> {
        let validity = vec![true, false, true, false];
        let left = DataBlock::new_from_columns(vec![
            StringType::from_data_with_validity(
                vec!["alpha", "ignored-left", "gamma", "hidden-left"],
                validity.clone(),
            ),
            UInt64Type::from_data_with_validity(vec![11, 1001, 13, 1003], validity.clone()),
        ]);
        let right = DataBlock::new_from_columns(vec![
            StringType::from_data_with_validity(
                vec!["alpha", "ignored-right", "gamma", "hidden-right"],
                validity.clone(),
            ),
            UInt64Type::from_data_with_validity(vec![11, 2001, 13, 2003], validity),
        ]);

        for partitions in [3, 4, 8] {
            let left_indices = scatter_indices(block_hash_keys(&left), partitions, &left)?;
            let right_indices = scatter_indices(block_hash_keys(&right), partitions, &right)?;
            assert_eq!(left_indices, right_indices);
        }
        Ok(())
    }

    #[test]
    fn hashes_valid_nullable_and_non_nullable_keys_consistently() -> Result<()> {
        let validity = vec![true, true, true, true];
        let nullable_block = DataBlock::new_from_columns(vec![
            StringType::from_data_with_validity(
                vec!["alpha", "beta", "gamma", "delta"],
                validity.clone(),
            ),
            UInt64Type::from_data_with_validity(vec![11, 12, 13, 14], validity),
        ]);
        let non_nullable_block = DataBlock::new_from_columns(vec![
            StringType::from_data(vec!["alpha", "beta", "gamma", "delta"]),
            UInt64Type::from_data(vec![11, 12, 13, 14]),
        ]);

        for partitions in [3, 4, 8] {
            let nullable = scatter_indices(
                block_hash_keys(&nullable_block),
                partitions,
                &nullable_block,
            )?;
            let non_nullable = scatter_indices(
                block_hash_keys(&non_nullable_block),
                partitions,
                &non_nullable_block,
            )?;
            assert_eq!(nullable, non_nullable);
        }
        Ok(())
    }

    #[test]
    fn handles_single_key_empty_keys_and_empty_blocks() -> Result<()> {
        let one_row = DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![42])]);
        let single_key = block_hash_keys(&one_row);
        let single_key_scatter = scatter(single_key, 3)?;
        assert_eq!(single_key_scatter.name(), "OneHashKey");
        assert_eq!(
            single_key_scatter.scatter_indices(&one_row)?.unwrap().len(),
            1
        );

        let no_columns = DataBlock::new(vec![], 3);
        assert_eq!(scatter_indices(vec![], 4, &no_columns)?, vec![0, 0, 0]);

        let empty = DataBlock::new_from_columns(vec![
            StringType::from_data(Vec::<String>::new()),
            UInt64Type::from_data(Vec::<u64>::new()),
        ]);
        assert!(scatter_indices(block_hash_keys(&empty), 8, &empty)?.is_empty());
        Ok(())
    }

    #[test]
    fn distributes_five_mixed_keys_across_partitions() -> Result<()> {
        const ROWS: usize = 1 << 16;

        let text_keys = (0..ROWS)
            .map(|row| format!("{row:042x}"))
            .collect::<Vec<_>>();
        let tag_keys = (0..ROWS)
            .map(|row| format!("{:042x}", row.wrapping_mul(17)))
            .collect::<Vec<_>>();
        let key_num_1 = (0..ROWS).map(|row| row as u64).collect::<Vec<_>>();
        let key_num_2 = (0..ROWS).map(|row| (row % 2048) as u64).collect::<Vec<_>>();
        let key_num_3 = (0..ROWS)
            .map(|row| row.wrapping_mul(31) as u64)
            .collect::<Vec<_>>();
        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(text_keys),
            StringType::from_data(tag_keys),
            UInt64Type::from_data(key_num_1),
            UInt64Type::from_data(key_num_2),
            UInt64Type::from_data(key_num_3),
        ]);

        for partitions in [3, 4, 8] {
            let indices = scatter_indices(block_hash_keys(&block), partitions, &block)?;
            assert_balanced(&indices, partitions);
        }
        Ok(())
    }

    fn single_key_scatter_indices(
        hash_key: Expr,
        scatter_size: usize,
        local_pos: usize,
        block: &DataBlock,
    ) -> Result<Vec<u64>> {
        let scatter = HashFlightScatter::try_create(
            FunctionContext::default(),
            vec![hash_key.as_remote_expr()],
            scatter_size,
            local_pos,
        )?;
        assert_eq!(scatter.name(), "OneHashKey");
        Ok(scatter.scatter_indices(block)?.unwrap())
    }

    fn u64_constant(value: u64) -> Expr {
        Expr::constant(
            Scalar::Number(NumberScalar::UInt64(value)),
            Some(DataType::Number(NumberDataType::UInt64)),
        )
    }

    // Same shape as the key built by `build_block_id_shuffle_exchange`.
    fn block_id_shuffle_key(row_id: Expr) -> Result<Expr> {
        check_function(
            None,
            "bit_and",
            &[],
            &[
                check_function(
                    None,
                    "bit_shift_right",
                    &[],
                    &[row_id, u64_constant((64 - NUM_ROW_ID_PREFIX_BITS) as u64)],
                    &BUILTIN_FUNCTIONS,
                )?,
                u64_constant(((1 << NUM_BLOCK_ID_BITS) - 1) as u64),
            ],
            &BUILTIN_FUNCTIONS,
        )
    }

    fn row_id(block_id: u64, row: u64) -> u64 {
        (block_id << (64 - NUM_ROW_ID_PREFIX_BITS)) | row
    }

    #[test]
    fn single_key_routes_equal_keys_to_same_partition() -> Result<()> {
        const ROWS: u64 = 4096;
        let nullable_type = DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)));

        // Both sides of a shuffle join: the same values in a different order, one side
        // nullable and the other not.
        let left = DataBlock::new_from_columns(vec![UInt64Type::from_data(
            (0..ROWS).map(|row| row * 7).collect::<Vec<_>>(),
        )]);
        let right = DataBlock::new_from_columns(vec![UInt64Type::from_data_with_validity(
            (0..ROWS).rev().map(|row| row * 7).collect::<Vec<_>>(),
            vec![true; ROWS as usize],
        )]);
        let left_text = DataBlock::new_from_columns(vec![StringType::from_data(
            (0..ROWS)
                .map(|row| format!("key-{row}"))
                .collect::<Vec<_>>(),
        )]);
        let right_text = DataBlock::new_from_columns(vec![StringType::from_data(
            (0..ROWS)
                .rev()
                .map(|row| format!("key-{row}"))
                .collect::<Vec<_>>(),
        )]);

        for partitions in [2, 3, 4, 8] {
            let numeric_type = DataType::Number(NumberDataType::UInt64);
            let mut left_indices =
                single_key_scatter_indices(column_expr(0, numeric_type), partitions, 0, &left)?;
            let right_indices = single_key_scatter_indices(
                column_expr(0, nullable_type.clone()),
                partitions,
                0,
                &right,
            )?;
            left_indices.reverse();
            assert_eq!(left_indices, right_indices);

            let mut left_indices = single_key_scatter_indices(
                column_expr(0, DataType::String),
                partitions,
                0,
                &left_text,
            )?;
            let right_indices = single_key_scatter_indices(
                column_expr(0, DataType::String),
                partitions,
                0,
                &right_text,
            )?;
            left_indices.reverse();
            assert_eq!(left_indices, right_indices);

            // A constant key is hashed like the materialized column.
            let constant = single_key_scatter_indices(u64_constant(42), partitions, 0, &left)?;
            let materialized = single_key_scatter_indices(
                column_expr(0, DataType::Number(NumberDataType::UInt64)),
                partitions,
                0,
                &DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![42; ROWS as usize])]),
            )?;
            assert_eq!(constant, materialized);
        }
        Ok(())
    }

    #[test]
    fn single_key_routes_null_keys_to_default_index() -> Result<()> {
        let nullable_type = DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)));
        let block = DataBlock::new_from_columns(vec![UInt64Type::from_data_with_validity(
            vec![1, 2, 3, 4, 5, 6],
            vec![true, false, true, false, false, true],
        )]);
        let all_null = DataBlock::new_from_columns(vec![UInt64Type::from_data_with_validity(
            vec![1, 2, 3],
            vec![false, false, false],
        )]);
        let non_null = DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![1, 3, 6])]);

        for partitions in [3, 4, 8] {
            // Ordinary shuffle keys send NULL to partition 0 regardless of the local position.
            let indices = single_key_scatter_indices(
                column_expr(0, nullable_type.clone()),
                partitions,
                2,
                &block,
            )?;
            let expected = single_key_scatter_indices(
                column_expr(0, DataType::Number(NumberDataType::UInt64)),
                partitions,
                2,
                &non_null,
            )?;
            assert_eq!([indices[0], indices[2], indices[5]], [
                expected[0],
                expected[1],
                expected[2]
            ]);
            assert_eq!([indices[1], indices[3], indices[4]], [0, 0, 0]);

            let indices = single_key_scatter_indices(
                column_expr(0, nullable_type.clone()),
                partitions,
                2,
                &all_null,
            )?;
            assert_eq!(indices, vec![0, 0, 0]);

            let null_constant = Expr::constant(Scalar::Null, Some(nullable_type.clone()));
            let indices = single_key_scatter_indices(null_constant, partitions, 2, &all_null)?;
            assert_eq!(indices, vec![0, 0, 0]);
        }
        Ok(())
    }

    #[test]
    fn single_key_routes_merge_into_unmatched_rows_to_local_pos() -> Result<()> {
        let nullable_type = DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64)));
        let hash_key = block_id_shuffle_key(column_expr(0, nullable_type))?;

        // Rows 0/2/4 are matched rows in blocks 5 and 9; NULL row ids are unmatched rows.
        let block = DataBlock::new_from_columns(vec![UInt64Type::from_data_with_validity(
            vec![row_id(5, 1), 0, row_id(5, 100), 0, row_id(9, 7)],
            vec![true, false, true, false, true],
        )]);
        let all_unmatched = DataBlock::new_from_columns(vec![UInt64Type::from_data_with_validity(
            vec![0, 0],
            vec![false, false],
        )]);

        for (partitions, local_pos) in [(2, 1), (3, 2), (4, 3), (8, 5)] {
            let indices =
                single_key_scatter_indices(hash_key.clone(), partitions, local_pos, &block)?;
            assert_eq!(indices[1], local_pos as u64);
            assert_eq!(indices[3], local_pos as u64);
            // Rows of the same block are sent to the same node.
            assert_eq!(indices[0], indices[2]);
            assert!(indices.iter().all(|index| *index < partitions as u64));

            let indices = single_key_scatter_indices(
                hash_key.clone(),
                partitions,
                local_pos,
                &all_unmatched,
            )?;
            assert_eq!(indices, vec![local_pos as u64; 2]);
        }
        Ok(())
    }

    #[test]
    fn single_key_distributes_across_partitions() -> Result<()> {
        const ROWS: usize = 1 << 16;
        let numeric_type = DataType::Number(NumberDataType::UInt64);

        // Dense, even-only and power-of-two strided integer keys, plus strings.
        let blocks = [
            (
                DataBlock::new_from_columns(vec![UInt64Type::from_data(
                    (0..ROWS as u64).collect::<Vec<_>>(),
                )]),
                numeric_type.clone(),
            ),
            (
                DataBlock::new_from_columns(vec![UInt64Type::from_data(
                    (0..ROWS as u64).map(|row| row * 2).collect::<Vec<_>>(),
                )]),
                numeric_type.clone(),
            ),
            (
                DataBlock::new_from_columns(vec![UInt64Type::from_data(
                    (0..ROWS as u64).map(|row| row << 20).collect::<Vec<_>>(),
                )]),
                numeric_type.clone(),
            ),
            (
                DataBlock::new_from_columns(vec![StringType::from_data(
                    (0..ROWS)
                        .map(|row| format!("{row:042x}"))
                        .collect::<Vec<_>>(),
                )]),
                DataType::String,
            ),
        ];

        for (block, data_type) in &blocks {
            for partitions in [2, 3, 4, 8, 16] {
                let indices = single_key_scatter_indices(
                    column_expr(0, data_type.clone()),
                    partitions,
                    0,
                    block,
                )?;
                assert_balanced(&indices, partitions);
            }
        }
        Ok(())
    }

    // Window partitioning and the aggregate hash index read raw bits of the same group
    // hash, so every scatter target must still cover all of their values.
    #[test]
    fn scatter_is_independent_of_local_hash_bits() -> Result<()> {
        const ROWS: u64 = 1 << 16;
        const WINDOW_PARTITIONS: u64 = 256;

        let block = DataBlock::new_from_columns(vec![
            UInt64Type::from_data((0..ROWS).collect::<Vec<_>>()),
            UInt64Type::from_data((0..ROWS).map(|row| row * 31).collect::<Vec<_>>()),
        ]);

        for key_count in [1, 2] {
            let mut hashes = vec![0; ROWS as usize];
            for (index, entry) in block.columns()[..key_count].iter().enumerate() {
                group_hash_entry(entry, &mut hashes, index == 0);
            }
            let hash_key = block_hash_keys(&block)[..key_count].to_vec();

            for partitions in [32, 48] {
                let indices = scatter_indices(hash_key.clone(), partitions, &block)?;
                let mut window_partitions = vec![HashSet::new(); partitions];
                let mut slots = vec![HashSet::new(); partitions];
                let mut tags = vec![HashSet::new(); partitions];
                for (index, hash) in indices.iter().zip(hashes.iter()) {
                    let target = *index as usize;
                    window_partitions[target].insert(hash % WINDOW_PARTITIONS);
                    slots[target].insert(hash & 127);
                    tags[target].insert(hash >> 57);
                }

                let coverage = window_partitions.iter().zip(&slots).zip(&tags);
                for (target, ((window_partitions, slots), tags)) in coverage.enumerate() {
                    assert!(
                        window_partitions.len() >= 240,
                        "target {target} covers {} window partitions",
                        window_partitions.len()
                    );
                    assert!(
                        slots.len() >= 120,
                        "target {target} covers {} low-bit slots",
                        slots.len()
                    );
                    assert!(
                        tags.len() >= 120,
                        "target {target} covers {} tags",
                        tags.len()
                    );
                }
            }
        }
        Ok(())
    }
}
