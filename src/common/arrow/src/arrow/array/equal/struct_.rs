// Copyright 2020-2022 Jorge C. Leitão
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

use crate::arrow::array::Array;
use crate::arrow::array::StructArray;

pub(super) fn equal(lhs: &StructArray, rhs: &StructArray) -> bool {
    lhs.data_type() == rhs.data_type()
        && lhs.len() == rhs.len()
        && match (lhs.validity(), rhs.validity()) {
            (None, None) => lhs.values().iter().eq(rhs.values().iter()),
            (Some(l_validity), Some(r_validity)) => lhs
                .values()
                .iter()
                .zip(rhs.values().iter())
                .all(|(lhs, rhs)| {
                    l_validity.iter().zip(r_validity.iter()).enumerate().all(
                        |(i, (lhs_is_valid, rhs_is_valid))| {
                            if lhs_is_valid && rhs_is_valid {
                                lhs.sliced(i, 1) == rhs.sliced(i, 1)
                            } else {
                                lhs_is_valid == rhs_is_valid
                            }
                        },
                    )
                }),
            (Some(l_validity), None) => {
                lhs.values()
                    .iter()
                    .zip(rhs.values().iter())
                    .all(|(lhs, rhs)| {
                        l_validity.iter().enumerate().all(|(i, lhs_is_valid)| {
                            if lhs_is_valid {
                                lhs.sliced(i, 1) == rhs.sliced(i, 1)
                            } else {
                                // rhs is always valid => different
                                false
                            }
                        })
                    })
            }
            (None, Some(r_validity)) => {
                lhs.values()
                    .iter()
                    .zip(rhs.values().iter())
                    .all(|(lhs, rhs)| {
                        r_validity.iter().enumerate().all(|(i, rhs_is_valid)| {
                            if rhs_is_valid {
                                lhs.sliced(i, 1) == rhs.sliced(i, 1)
                            } else {
                                // lhs is always valid => different
                                false
                            }
                        })
                    })
            }
        }
}
