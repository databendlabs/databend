// Copyright Qdrant
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

use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceType {
    Dot,
    L1,
    L2,
}

impl Display for DistanceType {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            DistanceType::Dot => write!(f, "dot"),
            DistanceType::L1 => write!(f, "l1"),
            DistanceType::L2 => write!(f, "l2"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct VectorParameters {
    pub dim: usize,
    pub count: usize,
    pub distance_type: DistanceType,
    pub invert: bool,
}

pub trait EncodedVectors<TEncodedQuery: Sized>: Sized {
    fn build_data(&self) -> Result<Vec<u8>>;

    fn build_meta(&self) -> Result<Vec<u8>>;

    fn load(data: &[u8], meta: &[u8], vector_parameters: &VectorParameters) -> Result<Self>;

    fn encode_query(&self, query: &[f32]) -> TEncodedQuery;

    fn score_point(&self, query: &TEncodedQuery, i: u32) -> f32;

    fn score_internal(&self, i: u32, j: u32) -> f32;
}

impl DistanceType {
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            DistanceType::Dot => a.iter().zip(b).map(|(a, b)| a * b).sum(),
            DistanceType::L1 => a.iter().zip(b).map(|(a, b)| (a - b).abs()).sum(),
            DistanceType::L2 => a.iter().zip(b).map(|(a, b)| (a - b) * (a - b)).sum(),
        }
    }
}

pub(crate) fn validate_vector_parameters<'a>(
    data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    vector_parameters: &VectorParameters,
) -> Result<()> {
    let mut count = 0;
    for vector in data {
        let vector = vector.as_ref();
        if vector.len() != vector_parameters.dim {
            return Err(ErrorCode::BadArguments(format!(
                "Vector length {} does not match vector parameters dim {}",
                vector.len(),
                vector_parameters.dim
            )));
        }
        count += 1;
    }
    if count != vector_parameters.count {
        return Err(ErrorCode::BadArguments(format!(
            "Vector count {} does not match vector parameters count {}",
            count, vector_parameters.count
        )));
    }
    Ok(())
}
