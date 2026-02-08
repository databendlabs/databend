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

#[derive(Debug, Clone, Copy)]
pub enum Ndv {
    // safe for selectivity
    Stat(f64),
    Max(f64),
}

impl Ndv {
    pub fn reduce(self, ndv: f64) -> Self {
        match self {
            Ndv::Stat(v) => Ndv::Stat(v.min(ndv)),
            Ndv::Max(v) => Ndv::Max(v.min(ndv)),
        }
    }

    pub fn reduce_by_selectivity(self, selectivity: f64) -> Self {
        match self {
            Ndv::Stat(v) => Ndv::Stat((v * selectivity).ceil()),
            Ndv::Max(v) => Ndv::Max((v * selectivity).ceil()),
        }
    }

    pub fn value(self) -> f64 {
        match self {
            Ndv::Stat(v) => v,
            Ndv::Max(v) => v,
        }
    }
}
