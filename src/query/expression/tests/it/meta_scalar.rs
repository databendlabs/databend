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

use databend_common_expression::converts::meta::IndexScalar;
use databend_common_expression::converts::meta::LegacyColumn;
use databend_common_expression::converts::meta::LegacyScalar;
use databend_common_expression::Column;
use databend_common_expression::Scalar;
use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_io::prelude::bincode_serialize_into_buf;

use crate::rand_block_for_all_types;
use crate::rand_block_for_simple_types;

#[test]
pub fn test_legacy_converts() -> databend_common_exception::Result<()> {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let test_times = rng.gen_range(5..30);

    for _ in 0..test_times {
        let rows = rng.gen_range(100..1024);
        let random_block = rand_block_for_all_types(rows);
        for entry in random_block
            .columns()
            .iter()
            .filter(|c| !c.data_type.remove_nullable().is_binary())
        {
            let column = entry.value.as_column().unwrap().clone();

            let legacy_column: LegacyColumn = column.clone().into();
            let convert_back_column: Column = legacy_column.into();
            assert_eq!(column, convert_back_column);

            let mut v3_scalars = vec![];

            for row in 0..rows {
                let scalar = entry.value.index(row).unwrap().to_owned();
                let legacy_scalar: LegacyScalar = scalar.clone().into();
                v3_scalars.push(legacy_scalar.clone());

                let convert_back_scalar: Scalar = legacy_scalar.into();
                assert_eq!(scalar, convert_back_scalar);
            }

            let mut data = vec![];
            bincode_serialize_into_buf(&mut data, &v3_scalars).unwrap();
            let new_scalars: Vec<LegacyScalar> = bincode_deserialize_from_slice(&data).unwrap();

            for (a, b) in v3_scalars.into_iter().zip(new_scalars.into_iter()) {
                let a: Scalar = a.into();
                let b: Scalar = b.into();

                assert_eq!(a, b);
            }
        }
    }

    Ok(())
}

#[test]
pub fn test_simple_converts() -> databend_common_exception::Result<()> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let test_times = rng.gen_range(5..30);

    for _ in 0..test_times {
        let rows = rng.gen_range(100..1024);
        let random_block = rand_block_for_simple_types(rows);
        for entry in random_block
            .columns()
            .iter()
            .filter(|c| !c.data_type.remove_nullable().is_binary())
        {
            let mut scalars = vec![];
            let mut simple_scalars = vec![];

            for row in 0..rows {
                let scalar = entry.value.index(row).unwrap().to_owned();
                let simple_scalar: IndexScalar = scalar.clone().try_into().unwrap();
                simple_scalars.push(simple_scalar.clone());
                scalars.push(scalar.clone());

                let convert_back_scalar: Scalar = simple_scalar.try_into().unwrap();
                assert_eq!(scalar, convert_back_scalar);
            }

            // TODO: comment these when we swith string in scalar
            // let data = rmp_serde::to_vec(&scalars).unwrap();
            // let new_scalars: Vec<IndexScalar> = rmp_serde::from_slice(&data).unwrap();
            // assert_eq!(simple_scalars, new_scalars);
        }
    }

    Ok(())
}
