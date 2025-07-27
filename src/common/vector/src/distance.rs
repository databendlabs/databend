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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use ndarray::ArrayView;

pub fn cosine_distance(lhs: &[f32], rhs: &[f32]) -> Result<f32> {
    if lhs.len() != rhs.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            lhs.len(),
            rhs.len(),
        )));
    }

    let a = ArrayView::from(lhs);
    let b = ArrayView::from(rhs);
    let aa_sum = (&a * &a).sum();
    let bb_sum = (&b * &b).sum();

    Ok(1.0 - (&a * &b).sum() / ((aa_sum).sqrt() * (bb_sum).sqrt()))
}

pub fn l1_distance(lhs: &[f32], rhs: &[f32]) -> Result<f32> {
    if lhs.len() != rhs.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            lhs.len(),
            rhs.len(),
        )));
    }

    Ok(lhs
        .iter()
        .zip(rhs.iter())
        .map(|(a, b)| (a - b).abs())
        .sum::<f32>())
}

pub fn l2_distance(lhs: &[f32], rhs: &[f32]) -> Result<f32> {
    if lhs.len() != rhs.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            lhs.len(),
            rhs.len(),
        )));
    }

    Ok(lhs
        .iter()
        .zip(rhs.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f32>()
        .sqrt())
}

pub fn inner_product(lhs: &[f32], rhs: &[f32]) -> Result<f32> {
    if lhs.len() != rhs.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            lhs.len(),
            rhs.len(),
        )));
    }

    let a = ArrayView::from(lhs);
    let b = ArrayView::from(rhs);

    Ok((&a * &b).sum())
}

pub fn cosine_distance_64(lhs: &[f64], rhs: &[f64]) -> Result<f64> {
    if lhs.len() != rhs.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            lhs.len(),
            rhs.len(),
        )));
    }

    let a = ArrayView::from(lhs);
    let b = ArrayView::from(rhs);
    let aa_sum = (&a * &a).sum();
    let bb_sum = (&b * &b).sum();

    Ok(1.0 - (&a * &b).sum() / ((aa_sum).sqrt() * (bb_sum).sqrt()))
}

pub fn l1_distance_64(lhs: &[f64], rhs: &[f64]) -> Result<f64> {
    if lhs.len() != rhs.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            lhs.len(),
            rhs.len(),
        )));
    }

    Ok(lhs
        .iter()
        .zip(rhs.iter())
        .map(|(a, b)| (a - b).abs())
        .sum::<f64>())
}

pub fn l2_distance_64(lhs: &[f64], rhs: &[f64]) -> Result<f64> {
    if lhs.len() != rhs.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            lhs.len(),
            rhs.len(),
        )));
    }

    Ok(lhs
        .iter()
        .zip(rhs.iter())
        .map(|(a, b)| (a - b).powi(2))
        .sum::<f64>()
        .sqrt())
}

pub fn inner_product_64(lhs: &[f64], rhs: &[f64]) -> Result<f64> {
    if lhs.len() != rhs.len() {
        return Err(ErrorCode::InvalidArgument(format!(
            "Vector length not equal: {:} != {:}",
            lhs.len(),
            rhs.len(),
        )));
    }

    let a = ArrayView::from(lhs);
    let b = ArrayView::from(rhs);

    Ok((&a * &b).sum())
}

pub fn vector_norm(vector: &[f32]) -> f32 {
    let a = ArrayView::from(vector);
    (&a * &a).sum().sqrt()
}
