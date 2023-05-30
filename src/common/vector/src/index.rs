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

use ndarray::ArrayViewMut;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VectorIndex {
    IvfFlat(IvfFlatIndex),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IvfFlatIndex {
    pub nlists: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MetricType {
    Cosine,
}

pub fn normalize(vec: &mut [f32]) {
    const DIFF: f32 = 1e-6;
    let mut vec = ArrayViewMut::from(vec);
    let norm = vec.dot(&vec).sqrt();
    if (norm - 1.0).abs() < DIFF || (norm - 0.0).abs() < DIFF {
        return;
    }
    vec.mapv_inplace(|x| x / norm);
}

pub fn normalize_vectors(vecs: &mut [f32], dim: usize) {
    let _: Vec<_> = vecs.chunks_mut(dim).map(normalize).collect();
}
