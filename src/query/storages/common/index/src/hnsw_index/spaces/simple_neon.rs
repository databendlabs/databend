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

// Copied from qdrant `lib/segment/src/spaces/simple_neon.rs` (v1.20.0), without
// the cosine preprocessing.

#[cfg(target_feature = "neon")]
use std::arch::aarch64::*;

#[cfg(target_feature = "neon")]
use crate::hnsw_index::common::types::ScoreType;

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn euclid_similarity_neon(v1: &[f32], v2: &[f32]) -> ScoreType {
    unsafe {
        let n = v1.len();
        let m = n - (n % 16);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum1 = vdupq_n_f32(0.);
        let mut sum2 = vdupq_n_f32(0.);
        let mut sum3 = vdupq_n_f32(0.);
        let mut sum4 = vdupq_n_f32(0.);

        let mut i: usize = 0;
        while i < m {
            let sub1 = vsubq_f32(vld1q_f32(ptr1), vld1q_f32(ptr2));
            sum1 = vfmaq_f32(sum1, sub1, sub1);

            let sub2 = vsubq_f32(vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
            sum2 = vfmaq_f32(sum2, sub2, sub2);

            let sub3 = vsubq_f32(vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
            sum3 = vfmaq_f32(sum3, sub3, sub3);

            let sub4 = vsubq_f32(vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
            sum4 = vfmaq_f32(sum4, sub4, sub4);

            ptr1 = ptr1.add(16);
            ptr2 = ptr2.add(16);
            i += 16;
        }
        let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
        for i in 0..n - m {
            result += (*ptr1.add(i) - *ptr2.add(i)).powi(2);
        }
        -result
    }
}

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn manhattan_similarity_neon(v1: &[f32], v2: &[f32]) -> ScoreType {
    unsafe {
        let n = v1.len();
        let m = n - (n % 16);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum1 = vdupq_n_f32(0.);
        let mut sum2 = vdupq_n_f32(0.);
        let mut sum3 = vdupq_n_f32(0.);
        let mut sum4 = vdupq_n_f32(0.);

        let mut i: usize = 0;
        while i < m {
            let sub1 = vsubq_f32(vld1q_f32(ptr1), vld1q_f32(ptr2));
            sum1 = vaddq_f32(sum1, vabsq_f32(sub1));

            let sub2 = vsubq_f32(vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
            sum2 = vaddq_f32(sum2, vabsq_f32(sub2));

            let sub3 = vsubq_f32(vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
            sum3 = vaddq_f32(sum3, vabsq_f32(sub3));

            let sub4 = vsubq_f32(vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
            sum4 = vaddq_f32(sum4, vabsq_f32(sub4));

            ptr1 = ptr1.add(16);
            ptr2 = ptr2.add(16);
            i += 16;
        }
        let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
        for i in 0..n - m {
            result += (*ptr1.add(i) - *ptr2.add(i)).abs();
        }
        -result
    }
}

#[cfg(target_feature = "neon")]
pub(crate) unsafe fn dot_similarity_neon(v1: &[f32], v2: &[f32]) -> ScoreType {
    unsafe {
        let n = v1.len();
        let m = n - (n % 16);
        let mut ptr1: *const f32 = v1.as_ptr();
        let mut ptr2: *const f32 = v2.as_ptr();
        let mut sum1 = vdupq_n_f32(0.);
        let mut sum2 = vdupq_n_f32(0.);
        let mut sum3 = vdupq_n_f32(0.);
        let mut sum4 = vdupq_n_f32(0.);

        let mut i: usize = 0;
        while i < m {
            sum1 = vfmaq_f32(sum1, vld1q_f32(ptr1), vld1q_f32(ptr2));
            sum2 = vfmaq_f32(sum2, vld1q_f32(ptr1.add(4)), vld1q_f32(ptr2.add(4)));
            sum3 = vfmaq_f32(sum3, vld1q_f32(ptr1.add(8)), vld1q_f32(ptr2.add(8)));
            sum4 = vfmaq_f32(sum4, vld1q_f32(ptr1.add(12)), vld1q_f32(ptr2.add(12)));
            ptr1 = ptr1.add(16);
            ptr2 = ptr2.add(16);
            i += 16;
        }
        let mut result = vaddvq_f32(sum1) + vaddvq_f32(sum2) + vaddvq_f32(sum3) + vaddvq_f32(sum4);
        for i in 0..n - m {
            result += (*ptr1.add(i)) * (*ptr2.add(i));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_feature = "neon")]
    #[test]
    fn test_spaces_neon() {
        use super::*;
        use crate::hnsw_index::spaces::simple::*;

        if std::arch::is_aarch64_feature_detected!("neon") {
            let v1: Vec<f32> = vec![
                10., 11., 12., 13., 14., 15., 16., 17., 18., 19., 20., 21., 22., 23., 24., 25.,
                26., 27., 28., 29., 30., 31.,
            ];
            let v2: Vec<f32> = vec![
                40., 41., 42., 43., 44., 45., 46., 47., 48., 49., 50., 51., 52., 53., 54., 55.,
                56., 57., 58., 59., 60., 61.,
            ];

            let euclid_simd = unsafe { euclid_similarity_neon(&v1, &v2) };
            let euclid = euclid_similarity(&v1, &v2);
            assert_eq!(euclid_simd, euclid);

            let manhattan_simd = unsafe { manhattan_similarity_neon(&v1, &v2) };
            let manhattan = manhattan_similarity(&v1, &v2);
            assert_eq!(manhattan_simd, manhattan);

            let dot_simd = unsafe { dot_similarity_neon(&v1, &v2) };
            let dot = dot_similarity(&v1, &v2);
            assert_eq!(dot_simd, dot);
        } else {
            println!("neon test skipped");
        }
    }
}
