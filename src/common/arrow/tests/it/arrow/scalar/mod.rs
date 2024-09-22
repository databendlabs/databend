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

mod binary;
mod boolean;
mod fixed_size_binary;
mod fixed_size_list;
mod list;
mod map;
mod null;
mod primitive;
mod struct_;
mod utf8;

use databend_common_arrow::arrow::scalar::Scalar;

// check that `PartialEq` can be derived
#[allow(dead_code)]
#[derive(PartialEq)]
struct A {
    array: Box<dyn Scalar>,
}
