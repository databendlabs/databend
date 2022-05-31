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

#[test]
fn test_position_neon() {
    #[cfg(all(any(target_arch = "aarch64"), target_feature = "neon"))]
    {
        use common_io::prelude::position_neon;

        assert_eq!(
            3,
            position_neon::<true, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(&[
                1, 2, 3, 4, 5, 6, 7
            ])
        );
        assert_eq!(
            7,
            position_neon::<true, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(&[
                1, 2, 3, 4, 5, 6, 7
            ])
        );
        assert_eq!(
            1,
            position_neon::<false, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(&[
                1, 2, 3, 4, 5, 6, 7
            ])
        );
        assert_eq!(
            2,
            position_neon::<false, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(&[
                1, 2, 3, 4, 5, 6, 7
            ])
        );

        assert_eq!(
            5,
            position_neon::<true, 18, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(&[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
            ])
        );
        assert_eq!(
            17,
            position_neon::<true, 18, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(&[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
            ])
        );

        assert_eq!(
            1,
            position_neon::<false, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>(&[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
            ])
        );
        assert_eq!(
            16,
            position_neon::<false, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>(&[
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
            ])
        );
    }
}
