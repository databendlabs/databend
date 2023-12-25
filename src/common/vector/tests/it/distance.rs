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

use databend_common_vector::cosine_distance;

#[test]
fn test_cosine() {
    {
        let x: Vec<f32> = (1..9).map(|v| v as f32).collect();
        let y: Vec<f32> = (100..108).map(|v| v as f32).collect();
        let d = cosine_distance(&x, &y).unwrap();
        // from scipy.spatial.distance.cosine
        approx::assert_relative_eq!(d, 1.0 - 0.900_957);
    }

    {
        let x = vec![3.0, 45.0, 7.0, 2.0, 5.0, 20.0, 13.0, 12.0];
        let y = vec![2.0, 54.0, 13.0, 15.0, 22.0, 34.0, 50.0, 1.0];
        let d = cosine_distance(&x, &y).unwrap();
        // from sklearn.metrics.pairwise import cosine_similarity
        approx::assert_relative_eq!(d, 1.0 - 0.873_580_6);
    }

    {
        let x = vec![3.0, 45.0, 7.0, 2.0, 5.0, 20.0, 13.0, 12.0];
        let y = vec![2.0, 54.0];
        let d = cosine_distance(&x, &y);
        assert!(d.is_err());
    }
}
