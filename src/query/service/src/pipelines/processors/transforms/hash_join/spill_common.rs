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

// Define some methods that are used by both the build and probe spilling of the hash join.

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;

pub fn get_hashes(
    func_ctx: &FunctionContext,
    block: &DataBlock,
    keys: &[Expr],
    method: &HashMethodKind,
    hashes: &mut Vec<u64>,
) -> Result<()> {
    let evaluator = Evaluator::new(block, func_ctx, &BUILTIN_FUNCTIONS);
    let columns: Vec<(Column, DataType)> = keys
        .iter()
        .map(|expr| {
            let return_type = expr.data_type();
            Ok((
                evaluator
                    .run(expr)?
                    .convert_to_full_column(return_type, block.num_rows()),
                return_type.clone(),
            ))
        })
        .collect::<Result<_>>()?;
    match method {
        HashMethodKind::Serializer(method) => {
            let rows_state = method.build_keys_state(&columns, block.num_rows())?;
            for row in method.build_keys_iter(&rows_state)? {
                hashes.push(method.get_hash(row));
            }
        }
        HashMethodKind::DictionarySerializer(method) => {
            let rows_state = method.build_keys_state(&columns, block.num_rows())?;
            for row in method.build_keys_iter(&rows_state)? {
                hashes.push(method.get_hash(row));
            }
        }
        HashMethodKind::SingleString(method) => {
            let rows_state = method.build_keys_state(&columns, block.num_rows())?;
            for row in method.build_keys_iter(&rows_state)? {
                hashes.push(method.get_hash(row));
            }
        }
        HashMethodKind::KeysU8(method) => {
            let rows_state = method.build_keys_state(&columns, block.num_rows())?;
            for row in method.build_keys_iter(&rows_state)? {
                hashes.push(method.get_hash(row));
            }
        }
        HashMethodKind::KeysU16(method) => {
            let rows_state = method.build_keys_state(&columns, block.num_rows())?;
            for row in method.build_keys_iter(&rows_state)? {
                hashes.push(method.get_hash(row));
            }
        }
        HashMethodKind::KeysU32(method) => {
            let rows_state = method.build_keys_state(&columns, block.num_rows())?;
            for row in method.build_keys_iter(&rows_state)? {
                hashes.push(method.get_hash(row));
            }
        }
        HashMethodKind::KeysU64(method) => {
            let rows_state = method.build_keys_state(&columns, block.num_rows())?;
            for row in method.build_keys_iter(&rows_state)? {
                hashes.push(method.get_hash(row));
            }
        }
        HashMethodKind::KeysU128(method) => {
            let rows_state = method.build_keys_state(&columns, block.num_rows())?;
            for row in method.build_keys_iter(&rows_state)? {
                hashes.push(method.get_hash(row));
            }
        }
        HashMethodKind::KeysU256(method) => {
            let rows_state = method.build_keys_state(&columns, block.num_rows())?;
            for row in method.build_keys_iter(&rows_state)? {
                hashes.push(method.get_hash(row));
            }
        }
    }
    Ok(())
}
