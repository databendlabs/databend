// Copyright 2023 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use ndarray::Array;

pub fn cosine_distance(from: &[f32], to: &[f32]) -> Result<f32> {
    if from.len() != to.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            from.len(),
            to.len(),
        )));
    }

    // TODO: avoid memory copy
    let a = Array::from_vec(from.to_vec());
    let b = Array::from_vec(to.to_vec());
    let aa_sum = (&a * &a).sum();
    let bb_sum = (&b * &b).sum();

    Ok((a * b).sum() / ((aa_sum).sqrt() * (bb_sum).sqrt()))
}
