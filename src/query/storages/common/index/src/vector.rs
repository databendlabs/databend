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
use databend_common_expression::types::F32;
use databend_common_vector::cosine_distance;
use databend_common_vector::l1_distance;
use databend_common_vector::l2_distance;
use databend_common_vector::vector_norm;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use databend_storages_common_table_meta::meta::VectorDistanceType;

use crate::DistanceType;

pub fn vector_distance(
    left: &[f32],
    right: &[f32],
    distance_type: VectorDistanceType,
) -> Result<f32> {
    match distance_type {
        VectorDistanceType::L1 => l1_distance(left, right),
        VectorDistanceType::L2 => l2_distance(left, right),
        VectorDistanceType::Dot => {
            let distance = cosine_distance(left, right)?;
            if distance.is_finite() {
                Ok(distance)
            } else {
                Ok(1.0)
            }
        }
    }
}

pub fn vector_spheres_overlap(
    left: &VectorColumnStatistics,
    right: &VectorColumnStatistics,
    distance_type: VectorDistanceType,
) -> Result<bool> {
    let left_centroid = vector_centroid_values(left);
    let right_centroid = vector_centroid_values(right);
    let distance = vector_stat_distance(&left_centroid, &right_centroid, distance_type)?;

    Ok(distance <= left.radius.0 + right.radius.0)
}

pub fn vector_distance_lower_bound(
    query: &[f32],
    stats: &VectorColumnStatistics,
    distance_type: VectorDistanceType,
) -> Result<f32> {
    let centroid = vector_centroid_values(stats);
    let distance = vector_stat_distance(query, &centroid, distance_type)?;
    let lower_bound = (distance - stats.radius.0).max(0.0);
    if matches!(distance_type, VectorDistanceType::Dot) {
        return Ok(1.0 - lower_bound.cos());
    }

    Ok(lower_bound)
}

pub fn vector_stat_distance(
    left: &[f32],
    right: &[f32],
    distance_type: VectorDistanceType,
) -> Result<f32> {
    if matches!(distance_type, VectorDistanceType::Dot) {
        return vector_angular_distance(left, right);
    }

    vector_distance(left, right, distance_type)
}

pub fn vector_angular_distance(left: &[f32], right: &[f32]) -> Result<f32> {
    // Cosine distance (1 - cos(theta)) is not a metric, so it cannot be used
    // directly with the triangle inequality for safe block pruning.  Convert it
    // to angular distance theta, which is a metric on normalized vectors.
    let distance = cosine_distance(left, right)?;
    if !distance.is_finite() {
        return Ok(std::f32::consts::FRAC_PI_2);
    }

    Ok((1.0 - distance).clamp(-1.0, 1.0).acos())
}

pub fn preprocess_vector(values: &[F32], distance_type: DistanceType) -> Vec<f32> {
    let mut vector = values.iter().map(|value| value.0).collect::<Vec<_>>();
    if matches!(distance_type, DistanceType::Dot) {
        normalize_vector(&mut vector);
    }
    vector
}

pub fn normalize_vector(vector: &mut [f32]) {
    let norm = vector_norm(vector);
    if norm <= f32::EPSILON {
        return;
    }
    for value in vector {
        *value /= norm;
    }
}

pub fn vector_statistics_distance_type(distance_type: DistanceType) -> VectorDistanceType {
    match distance_type {
        DistanceType::L1 => VectorDistanceType::L1,
        DistanceType::L2 => VectorDistanceType::L2,
        DistanceType::Dot => VectorDistanceType::Dot,
    }
}

pub fn vector_distance_type_from_index_option(distance_type: &str) -> Option<VectorDistanceType> {
    match distance_type.trim() {
        "cosine" | "dot" => Some(VectorDistanceType::Dot),
        "l1" => Some(VectorDistanceType::L1),
        "l2" => Some(VectorDistanceType::L2),
        _ => None,
    }
}

pub fn vector_distance_type_name(distance_type: VectorDistanceType) -> &'static str {
    match distance_type {
        VectorDistanceType::L1 => "l1",
        VectorDistanceType::L2 => "l2",
        VectorDistanceType::Dot => "dot",
    }
}

fn vector_centroid_values(stats: &VectorColumnStatistics) -> Vec<f32> {
    stats.centroid.iter().map(|value| value.0).collect()
}
