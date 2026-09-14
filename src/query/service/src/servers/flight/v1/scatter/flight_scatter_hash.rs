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
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::group_hash_entry;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::number::NumberScalar;
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
    indices_scalar: Expr,
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
        let indices_scalar = check_function(
            None,
            "modulo",
            &[],
            &[
                check_function(
                    None,
                    "siphash",
                    &[],
                    &[hash_key.as_expr(&BUILTIN_FUNCTIONS)],
                    &BUILTIN_FUNCTIONS,
                )?,
                Expr::constant(
                    Scalar::Number(NumberScalar::UInt64(scatter_size as u64)),
                    Some(DataType::Number(NumberDataType::UInt64)),
                ),
            ],
            &BUILTIN_FUNCTIONS,
        )?;

        Ok(Box::new(OneHashKeyFlightScatter {
            scatter_size,
            func_ctx,
            indices_scalar,
            default_scatter_index,
        }))
    }
}

impl FlightScatter for OneHashKeyFlightScatter {
    fn name(&self) -> &'static str {
        "OneHashKey"
    }

    fn execute(&self, data_block: DataBlock) -> Result<Vec<DataBlock>> {
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num = data_block.num_rows();

        let indices = evaluator.run(&self.indices_scalar).unwrap();
        let indices = get_hash_values(indices, num, self.default_scatter_index)?;
        let data_blocks = DataBlock::scatter(&data_block, &indices, self.scatter_size)?;

        let block_meta = data_block.get_meta();
        let mut res = Vec::with_capacity(data_blocks.len());
        for data_block in data_blocks {
            res.push(data_block.add_meta(block_meta.cloned())?);
        }

        Ok(res)
    }

    fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
        let evaluator = Evaluator::new(data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let num = data_block.num_rows();
        let indices = evaluator.run(&self.indices_scalar).unwrap();
        let indices = get_hash_values(indices, num, self.default_scatter_index)?;
        Ok(Some(indices.to_vec()))
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
            *hash %= scatter_size;
        }
        Ok(hashes)
    }
}

fn shuffle_by_block_id_in_merge_into(expr: &RemoteExpr) -> bool {
    if let RemoteExpr::FunctionCall {
        id: box FunctionID::Builtin { name, .. },
        args,
        ..
    } = expr
    {
        if name == "bit_and" {
            if let RemoteExpr::FunctionCall {
                id: box FunctionID::Builtin { name, .. },
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

fn get_hash_values(
    column: Value<AnyType>,
    rows: usize,
    default_scatter_index: u64,
) -> Result<Buffer<u64>> {
    match column {
        Value::Scalar(c) => match c {
            databend_common_expression::Scalar::Null => {
                Ok(vec![default_scatter_index; rows].into())
            }
            databend_common_expression::Scalar::Number(NumberScalar::UInt64(x)) => {
                Ok(vec![x; rows].into())
            }
            _ => unreachable!(),
        },
        Value::Column(c) => {
            if let Ok(column) = NumberType::<u64>::try_downcast_column(&c) {
                return Ok(column);
            }

            let mut column = NullableType::<NumberType<u64>>::try_downcast_column(&c).unwrap();
            let null_map = column.validity;
            if null_map.null_count() == 0 {
                return Ok(column.column);
            }
            if null_map.null_count() == null_map.len() {
                return Ok(vec![default_scatter_index; rows].into());
            }

            let mut need_new_vec = true;
            if let Some(column) = unsafe { column.column.get_mut() } {
                column
                    .iter_mut()
                    .zip(null_map.iter())
                    .for_each(|(x, valid)| {
                        if valid {
                            *x *= valid as u64;
                        } else {
                            *x = default_scatter_index;
                        }
                    });
                need_new_vec = false;
            }

            if !need_new_vec {
                Ok(column.column)
            } else {
                Ok(column
                    .column
                    .iter()
                    .zip(null_map.iter())
                    .map(|(x, b)| if b { *x } else { default_scatter_index })
                    .collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::FromData;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;

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
}
