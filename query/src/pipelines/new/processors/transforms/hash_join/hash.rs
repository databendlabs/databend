// Copyright 2022 Datafuse Labs.
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

use std::hash::Hasher;

use common_datavalues::ColumnRef;
use common_datavalues::ColumnWithField;
use common_datavalues::DataField;
use common_datavalues::PrimitiveColumn;
use common_datavalues::Series;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_functions::scalars::FunctionFactory;
use twox_hash::XxHash64;

pub type HashVector = Vec<u64>;

pub struct HashUtil;

impl HashUtil {
    pub fn compute_hash(column: &ColumnRef) -> Result<HashVector> {
        let hash_function = FunctionFactory::instance().get("xxhash64", &[&column.data_type()])?;
        let field = DataField::new("", column.data_type());
        let result = hash_function.eval(
            FunctionContext::default(),
            &[ColumnWithField::new(column.clone(), field)],
            column.len(),
        )?;

        let result = Series::remove_nullable(&result);
        let result = Series::check_get::<PrimitiveColumn<u64>>(&result)?;
        Ok(result.values().to_vec())
    }

    pub fn combine_hashes(inputs: &[HashVector], size: usize) -> HashVector {
        static XXHASH_SEED: u64 = 0;

        let mut result = Vec::with_capacity(size);
        result.resize(size, XxHash64::with_seed(XXHASH_SEED));
        for input in inputs.iter() {
            assert_eq!(input.len(), size);
            for i in 0..size {
                result[i].write_u64(input[i]);
            }
        }
        result.into_iter().map(|h| h.finish()).collect()
    }
}
