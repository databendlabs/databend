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

pub enum MergeIntoJoinType {
    Left,
    Right,
    Inner,
    LeftAnti,
    RightAnti,
    // it means this join is not a merge into join
    NormalJoin,
}

pub struct MergeIntoJoin {
    merge_into_join_type: MergeIntoJoinType,
    is_distributed: bool,
}

impl Default for MergeIntoJoin {
    fn default() -> Self {
        Self {
            merge_into_join_type: MergeIntoJoinType::NormalJoin,
            is_distributed: Default::default(),
        }
    }
}
